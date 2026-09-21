/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package asb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/connectors"
)

// ASB has no log compaction, so each instance caches the sync topic and the
// leader writes that cache to a snapshot queue
const (
	compactionSnapshotInterval       = 30 * time.Second
	compactionLockRenewBefore        = 20 * time.Second
	compactionSnapshotDebounce       = time.Second
	sessionRenewTimeout              = 5 * time.Second
	sessionRenewMaxAttempts          = 2
	sessionRenewRetryDelay           = time.Second
	compactionElectionAttemptTimeout = 10 * time.Second
	compactionElectionRetryBase      = 3 * time.Second
	compactionElectionRetryJitterMax = 3 * time.Second

	snapshotPeekCount    = 10
	snapshotDrainTimeout = 2 * time.Second
	shutdownFlushTimeout = 10 * time.Second

	leaderSessionID     = "leader"
	leaderSeedMessageID = "leader-election-seed"
)

// leaderTimings lets tests drive runLeaderLoop on millisecond cadences.
type leaderTimings struct {
	snapshotInterval time.Duration
	renewInterval    time.Duration
	debounce         time.Duration
	renewMaxAttempts int
	renewRetryDelay  time.Duration
}

func defaultLeaderTimings() leaderTimings {
	return leaderTimings{
		snapshotInterval: compactionSnapshotInterval,
		renewInterval:    compactionLockRenewBefore,
		debounce:         compactionSnapshotDebounce,
		renewMaxAttempts: sessionRenewMaxAttempts,
		renewRetryDelay:  sessionRenewRetryDelay,
	}
}

// compactedTopicState owns the leader-election goroutine and live cache for one
// compacted topic. Started by EnsureCompactedTopic.
type compactedTopicState struct {
	topic         string
	snapshotQueue string
	lockQueue     string

	driver *ASBBrokerDriver

	mu               sync.RWMutex
	cache            map[string][]byte // key -> latest value; tombstoned keys are absent
	version          uint64
	publishedVersion uint64

	dirtySince time.Time
	dirty      chan struct{}
	seeded     bool

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// EnsureCompactedTopic provisions the topic and its two queues, seeds the cache
// from any existing snapshot, and starts the live-tail and election goroutines.
func (e *ASBBrokerDriver) EnsureCompactedTopic(ctx context.Context, topic string) error {
	if _, err := e.admin.CreateTopic(ctx, topic, nil); err != nil && !isConflictErr(err) {
		return fmt.Errorf("failed to create compacted topic %s: %w", topic, err)
	}

	snapshotQueue := snapshotQueueName(topic)
	lockQueue := leaderLockQueueName(topic)

	if _, err := e.admin.CreateQueue(ctx, snapshotQueue, nil); err != nil && !isConflictErr(err) {
		return fmt.Errorf("failed to create snapshot queue %s: %w", snapshotQueue, err)
	}

	// LockDuration left unset: ASB's 1-minute default already clears
	// compactionLockRenewBefore, and the field wants an ATOM-XML duration.
	if _, err := e.admin.CreateQueue(ctx, lockQueue, &admin.CreateQueueOptions{
		Properties: &admin.QueueProperties{
			RequiresSession:            to.Ptr(true),
			RequiresDuplicateDetection: to.Ptr(true),
		},
	}); err != nil && !isConflictErr(err) {
		return fmt.Errorf("failed to create leader-lock queue %s: %w", lockQueue, err)
	}

	e.compactedMu.Lock()
	defer e.compactedMu.Unlock()
	if _, running := e.compacted[topic]; running {
		return nil // already provisioned and running for this topic
	}

	state := &compactedTopicState{
		topic:         topic,
		snapshotQueue: snapshotQueue,
		lockQueue:     lockQueue,
		driver:        e,
		cache:         make(map[string][]byte),
		dirty:         make(chan struct{}, 1),
	}

	// Seed before the live tail starts, so the cache covers state predating it.
	if err := e.replaySnapshotInto(ctx, snapshotQueue, func(key string, value []byte) {
		state.cache[key] = value
	}); err != nil {
		slog.Error("Failed to seed compacted-topic cache from the existing snapshot; "+
			"this instance will not take leadership, so the snapshot is left intact. "+
			"Restart once the namespace is reachable, or purge the snapshot queue if its contents are corrupt",
			"topic", topic, "snapshot_queue", snapshotQueue, "error", err)
	} else {
		state.seeded = true
	}

	runCtx, cancel := context.WithCancel(context.Background())
	state.cancel = cancel

	state.wg.Add(2)
	go func() {
		defer state.wg.Done()
		state.runLiveTail(runCtx)
	}()
	go func() {
		defer state.wg.Done()
		state.runLeaderElection(runCtx)
	}()

	e.compacted[topic] = state
	return nil
}

// stopCompactedTopic stops the goroutines for topic, if running.
func (e *ASBBrokerDriver) stopCompactedTopic(topic string) {
	e.compactedMu.Lock()
	defer e.compactedMu.Unlock()
	e.stopCompactedTopicLocked(topic)
}

func (e *ASBBrokerDriver) stopCompactedTopicLocked(topic string) {
	state, ok := e.compacted[topic]
	if !ok {
		return
	}
	state.cancel()
	state.wg.Wait()
	delete(e.compacted, topic)
}

// Replay calls handler once per key in the current snapshot with that key's
// latest value.
func (e *ASBBrokerDriver) Replay(ctx context.Context, topic string, handler connectors.MessageHandler) error {
	var handlerErr error
	replayed := 0
	err := e.replaySnapshotInto(ctx, snapshotQueueName(topic), func(key string, value []byte) {
		if handlerErr != nil {
			return
		}
		replayed++
		if err := handler(ctx, &connectors.Message{Topic: topic, Key: []byte(key), Value: value}); err != nil {
			handlerErr = fmt.Errorf("replay handler failed for key %q: %w", key, err)
		}
	})
	if err != nil {
		return fmt.Errorf("failed to replay snapshot for topic %s: %w", topic, err)
	}
	if handlerErr != nil {
		return handlerErr
	}
	slog.Info("ASB snapshot replay complete", "topic", topic, "keys", replayed)
	return nil
}

// replaySnapshotInto peeks the snapshot queue and invokes visit per key. Peek,
// not receive: a consuming read would delete the state every instance needs.
func (e *ASBBrokerDriver) replaySnapshotInto(ctx context.Context, snapshotQueue string, visit func(key string, value []byte)) error {
	receiver, err := e.client.NewReceiverForQueue(snapshotQueue, nil)
	if err != nil {
		return fmt.Errorf("failed to open snapshot receiver: %w", err)
	}
	defer receiver.Close(ctx)

	// A failed cleanup can leave an older message behind, so take the newest.
	messages, err := receiver.PeekMessages(ctx, snapshotPeekCount, nil)
	if err != nil {
		return fmt.Errorf("failed to peek snapshot: %w", err)
	}
	if len(messages) == 0 {
		return nil
	}

	newest := messages[0]
	for _, msg := range messages[1:] {
		if msg.SequenceNumber != nil && (newest.SequenceNumber == nil || *msg.SequenceNumber > *newest.SequenceNumber) {
			newest = msg
		}
	}

	var snapshot map[string][]byte
	if err := json.Unmarshal(newest.Body, &snapshot); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot payload: %w", err)
	}
	for key, value := range snapshot {
		visit(key, value)
	}
	return nil
}

// runLiveTail keeps the cache current from a per-instance subscription.
func (s *compactedTopicState) runLiveTail(ctx context.Context) {
	subscriptionName := cacheSubscriptionName(s.driver.instanceID)

	receiver, err := s.openCacheReceiver(ctx, subscriptionName)
	if err != nil {
		slog.Error("Failed to open compacted-topic cache receiver; state will not stay current", "topic", s.topic, "error", err)
		return
	}
	defer func() { _ = receiver.Close(context.Background()) }()

	attempt := 0
	for {
		if ctx.Err() != nil {
			return
		}
		messages, err := receiver.ReceiveMessages(ctx, 10, nil)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isNotFoundErr(err) {
				if rebuilt, rebuildErr := s.rebuildCacheTail(ctx, receiver, subscriptionName); rebuildErr == nil {
					slog.Info("Rebuilt reclaimed compacted-topic cache subscription", "topic", s.topic)
					receiver = rebuilt
					attempt = 0
					continue
				}
			}
			slog.Error("Compacted-topic cache receive error", "topic", s.topic, "error", err)
			if !sleepBackoff(ctx, attempt) {
				return
			}
			attempt++
			continue
		}
		attempt = 0

		for _, rm := range messages {
			s.applyToCache(receivedMessageToMessage(rm, s.topic))
			if err := receiver.CompleteMessage(ctx, rm, nil); err != nil {
				slog.Error("Failed to complete compacted-topic cache message", "topic", s.topic, "error", err)
			}
		}
	}
}

func (s *compactedTopicState) openCacheReceiver(ctx context.Context, subscriptionName string) (*azservicebus.Receiver, error) {
	if err := s.driver.ensureDisposableSubscription(ctx, s.topic, subscriptionName); err != nil {
		return nil, fmt.Errorf("failed to ensure compacted-topic cache subscription: %w", err)
	}
	receiver, err := s.driver.client.NewReceiverForSubscription(s.topic, subscriptionName, &azservicebus.ReceiverOptions{
		ReceiveMode: azservicebus.ReceiveModePeekLock,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create compacted-topic cache receiver: %w", err)
	}
	return receiver, nil
}

// rebuildCacheTail recreates a reclaimed subscription and refills the cache from
// the snapshot, the only record of what was published while it was gone.
func (s *compactedTopicState) rebuildCacheTail(ctx context.Context, stale *azservicebus.Receiver, subscriptionName string) (*azservicebus.Receiver, error) {
	receiver, err := s.openCacheReceiver(ctx, subscriptionName)
	if err != nil {
		return nil, err
	}

	rebuilt := make(map[string][]byte)
	if err := s.driver.replaySnapshotInto(ctx, snapshotQueueName(s.topic), func(key string, value []byte) {
		rebuilt[key] = value
	}); err != nil {
		_ = receiver.Close(context.Background())
		return nil, fmt.Errorf("failed to refill cache from snapshot: %w", err)
	}

	s.mu.Lock()
	s.cache = rebuilt
	s.version++
	s.mu.Unlock()

	_ = stale.Close(context.Background())
	return receiver, nil
}

func (s *compactedTopicState) applyToCache(msg *connectors.Message) {
	s.mu.Lock()
	key := string(msg.Key)
	if msg.Value == nil {
		delete(s.cache, key)
	} else {
		s.cache[key] = msg.Value
	}
	s.version++
	// Only the first change after a write starts the clock
	if s.dirtySince.IsZero() {
		s.dirtySince = time.Now()
	}
	s.mu.Unlock()

	s.markDirty()
}

// cacheVersion reads the current mutation count. Captured before a publish, so
// a change landing during that publish is never mistaken for one it covered.
func (s *compactedTopicState) cacheVersion() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.version
}

// markPublished records the version a write covered, reporting how long its
// oldest change waited and how many subscribers the snapshot holds.
func (s *compactedTopicState) markPublished(version uint64) (exposure time.Duration, subscribers int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.publishedVersion = version
	subscribers = len(s.cache)
	if !s.dirtySince.IsZero() {
		exposure = time.Since(s.dirtySince)
		s.dirtySince = time.Time{}
	}
	return exposure, subscribers
}

// hasUnpublishedChanges reports whether the cache has moved on since the last
// successful publish, including when that publish failed and left it behind.
func (s *compactedTopicState) hasUnpublishedChanges() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.version != s.publishedVersion
}

// markDirty flags the cache as diverged from the last snapshot. Non-blocking:
// a pending signal already covers this change, since snapshots are whole-cache.
func (s *compactedTopicState) markDirty() {
	if s.dirty == nil {
		return
	}
	select {
	case s.dirty <- struct{}{}:
	default:
	}
}

// drainDirty clears any pending signal. Call before reading the cache: draining
// after could discard the signal for a change the snapshot missed.
func (s *compactedTopicState) drainDirty() {
	if s.dirty == nil {
		return
	}
	for {
		select {
		case <-s.dirty:
		default:
			return
		}
	}
}

func (s *compactedTopicState) snapshotCache() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return maps.Clone(s.cache)
}

// runLeaderElection contends for the leader session until ctx is cancelled,
// acting as leader while it holds the lock.
func (s *compactedTopicState) runLeaderElection(ctx context.Context) {
	if !s.seeded {
		slog.Warn("Not contending for compacted-topic leadership: cache is unseeded", "topic", s.topic)
		return
	}

	for {
		if ctx.Err() != nil {
			return
		}

		session, err := s.tryAcquireLeadership(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if !isSBTimeoutErr(err) {
				slog.Warn("Leader-election attempt failed", "topic", s.topic, "error", err)
			}
			s.seedLeaderSessionBestEffort(ctx)
			if !sleepFor(ctx, compactionElectionRetryBase+rand.N(compactionElectionRetryJitterMax)) {
				return
			}
			continue
		}

		slog.Info("Acquired compacted-topic leadership", "topic", s.topic)
		s.runAsLeader(ctx, session)
		slog.Info("Stepped down from compacted-topic leadership", "topic", s.topic)
	}
}

func (s *compactedTopicState) tryAcquireLeadership(ctx context.Context) (*azservicebus.SessionReceiver, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, compactionElectionAttemptTimeout)
	defer cancel()
	return s.driver.client.AcceptSessionForQueue(attemptCtx, s.lockQueue, leaderSessionID, &azservicebus.SessionReceiverOptions{
		ReceiveMode: azservicebus.ReceiveModePeekLock,
	})
}

// seedLeaderSessionBestEffort creates the "leader" session by sending to it;
// duplicate detection collapses repeats.
func (s *compactedTopicState) seedLeaderSessionBestEffort(ctx context.Context) {
	defer s.driver.publisher.closeSender(s.lockQueue)

	sender, err := s.driver.publisher.senderFor(s.lockQueue)
	if err != nil {
		slog.Warn("Failed to open leader-lock seed sender", "topic", s.topic, "error", err)
		return
	}
	sessionID := leaderSessionID
	messageID := leaderSeedMessageID
	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := sender.SendMessage(sendCtx, &azservicebus.Message{
		Body:      []byte("leader-election-anchor"),
		SessionID: &sessionID,
		MessageID: &messageID,
	}, nil); err != nil {
		slog.Debug("Leader-lock seed send did not complete (likely already seeded)", "topic", s.topic, "error", err)
	}
}

// runAsLeader binds Azure resources for a leadership term and delegates
// scheduling to runLeaderLoop, which is testable without a namespace.
func (s *compactedTopicState) runAsLeader(ctx context.Context, session *azservicebus.SessionReceiver) {
	defer session.Close(context.Background())

	snapshotReceiver, err := s.driver.client.NewReceiverForQueue(s.snapshotQueue, &azservicebus.ReceiverOptions{
		ReceiveMode: azservicebus.ReceiveModePeekLock,
	})
	if err != nil {
		slog.Error("Failed to open snapshot receiver, stepping down", "topic", s.topic, "error", err)
		return
	}
	defer snapshotReceiver.Close(context.Background())

	s.runLeaderLoop(ctx, defaultLeaderTimings(),
		func(ctx context.Context) error {
			renewCtx, cancel := context.WithTimeout(ctx, sessionRenewTimeout)
			defer cancel()
			return session.RenewSessionLock(renewCtx, nil)
		},
		func(ctx context.Context) error {
			return s.publishSnapshot(ctx, snapshotReceiver)
		},
	)
}

// renewWithRetry makes one extra attempt before conceding.
func (s *compactedTopicState) renewWithRetry(ctx context.Context, timings leaderTimings, renewLock func(context.Context) error) error {
	attempts := timings.renewMaxAttempts
	if attempts < 1 {
		attempts = 1
	}

	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err = renewLock(ctx); err == nil {
			if attempt > 1 {
				slog.Info("Leader session lock renewed after retry", "topic", s.topic, "attempts", attempt)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if isLockLostErr(err) {
			return fmt.Errorf("leader session lock lost: %w", err)
		}
		if attempt == attempts {
			break
		}

		slog.Debug("Leader session lock renewal failed, retrying",
			"topic", s.topic, "attempt", attempt, "error", err)

		if !sleepFor(ctx, timings.renewRetryDelay) {
			return ctx.Err()
		}
	}
	return fmt.Errorf("leader session lock renewal failed after %d attempts: %w", attempts, err)
}

// flushPendingSnapshot writes one final snapshot at shutdown, on a fresh context
// because shutdown has already cancelled the loop's own.
func (s *compactedTopicState) flushPendingSnapshot(publish func(context.Context) error) {
	if !s.hasUnpublishedChanges() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), shutdownFlushTimeout)
	defer cancel()

	if err := s.publishTracked(ctx, publish); err != nil {
		slog.Error("Failed to flush pending compacted-topic snapshot on shutdown", "topic", s.topic, "error", err)
		return
	}
	slog.Info("Flushed pending compacted-topic snapshot on shutdown", "topic", s.topic)
}

// publishTracked publishes and records the version it covered.
func (s *compactedTopicState) publishTracked(ctx context.Context, publish func(context.Context) error) error {
	version := s.cacheVersion()
	if err := publish(ctx); err != nil {
		return err
	}

	// The only place the durability window is observable: until here the change
	// existed solely in memory.
	exposure, subscribers := s.markPublished(version)
	slog.Info("Compacted-topic snapshot written",
		"topic", s.topic,
		"subscribers", subscribers,
		"unpublished_for", exposure.Round(time.Millisecond))
	return nil
}

// publishAndReport writes a snapshot and reports a failure at the level it deserves.
func (s *compactedTopicState) publishAndReport(ctx context.Context, publish func(context.Context) error, failureMsg string) {
	err := s.publishTracked(ctx, publish)
	if err == nil {
		return
	}
	if ctx.Err() != nil {
		slog.Debug("Compacted-topic snapshot write cancelled during teardown",
			"topic", s.topic, "error", err)
		return
	}
	slog.Error(failureMsg, "topic", s.topic, "error", err)
}

func (s *compactedTopicState) runLeaderLoop(
	ctx context.Context,
	timings leaderTimings,
	renewLock func(context.Context) error,
	publish func(context.Context) error,
) {
	snapshotTicker := time.NewTicker(timings.snapshotInterval)
	defer snapshotTicker.Stop()
	renewTicker := time.NewTicker(timings.renewInterval)
	defer renewTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.flushPendingSnapshot(publish)
			return
		case <-renewTicker.C:
			if err := s.renewWithRetry(ctx, timings, renewLock); err != nil {
				if ctx.Err() != nil {
					s.flushPendingSnapshot(publish)
					return
				}
				slog.Warn("Failed to renew leader session lock, stepping down", "topic", s.topic, "error", err)
				return
			}
		case <-s.dirty:
			if !sleepFor(ctx, timings.debounce) {
				s.flushPendingSnapshot(publish)
				return
			}
			s.drainDirty()
			s.publishAndReport(ctx, publish, "Failed to publish compacted-topic snapshot after change")
		case <-snapshotTicker.C:
			// Only covers a change the dirty path failed to write; a quiet topic
			// would otherwise pay a full write every tick.
			if !s.hasUnpublishedChanges() {
				continue
			}
			s.drainDirty()
			s.publishAndReport(ctx, publish, "Failed to publish compacted-topic snapshot")
		}
	}
}

// publishSnapshot replaces the queue's contents with the current cache.
func (s *compactedTopicState) publishSnapshot(ctx context.Context, receiver *azservicebus.Receiver) error {
	payload, err := json.Marshal(s.snapshotCache())
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	drainCtx, cancel := context.WithTimeout(ctx, snapshotDrainTimeout)
	stale, err := receiver.ReceiveMessages(drainCtx, snapshotPeekCount, nil)
	cancel()
	if err != nil && ctx.Err() == nil {
		// Expected on the first cycle: the queue is empty and ReceiveMessages
		// waits out the timeout to confirm it.
		slog.Debug("No stale snapshot message found to clear before republish", "topic", s.topic, "error", err)
	}

	sender, err := s.driver.publisher.senderFor(s.snapshotQueue)
	if err != nil {
		return fmt.Errorf("failed to open snapshot sender: %w", err)
	}
	if err := sender.SendMessage(ctx, &azservicebus.Message{Body: payload}, nil); err != nil {
		return fmt.Errorf("failed to send snapshot: %w", err)
	}

	// Self-healing on failure: readers take the newest until the next cycle.
	for _, msg := range stale {
		if err := receiver.CompleteMessage(ctx, msg, nil); err != nil {
			slog.Warn("Failed to settle stale snapshot message", "topic", s.topic, "error", err)
		}
	}
	return nil
}

func snapshotQueueName(topic string) string   { return topic + "-snapshot" }
func leaderLockQueueName(topic string) string { return topic + "-leader-lock" }

func cacheSubscriptionName(runtimeID string) string {
	return "cache-" + shortHash(runtimeID)
}

// syncSubscriptionName names the Watch subscription.
func syncSubscriptionName(consumerID string) string {
	return "sync-" + shortHash(consumerID)
}

func shortHash(value string) string {
	h := sha256.Sum256([]byte(value))
	return hex.EncodeToString(h[:])[:16]
}
