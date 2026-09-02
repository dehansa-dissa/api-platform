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
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/connectors"
)

// ASB has no equivalent of Kafka's log compaction: a consumed message is gone,
// and a new Subscription never receives messages published before it existed.
// Instead each instance folds the sync topic into an in-memory cache, and
// whichever instance holds the leader-lock session writes that cache to a
// snapshot queue. Replay reads only the snapshot. Leadership uses a
// session-enabled queue: ASB allows one holder per session ID and expires the
// lock on its own if that holder dies.
const (
	compactionSnapshotInterval       = 30 * time.Second
	compactionLockRenewBefore        = 20 * time.Second
	compactionSnapshotDebounce       = time.Second
	sessionRenewTimeout              = 5 * time.Second
	compactionElectionAttemptTimeout = 10 * time.Second
	compactionElectionRetryBase      = 3 * time.Second
	compactionElectionRetryJitterMax = 3 * time.Second

	snapshotPeekCount    = 10
	snapshotDrainTimeout = 2 * time.Second

	leaderSessionID = "leader"
	// Fixed MessageID, deduplicated server-side by the lock queue's
	// RequiresDuplicateDetection so repeated seeding never accumulates.
	leaderSeedMessageID = "leader-election-seed"
)

// leaderTimings lets tests drive runLeaderLoop on millisecond cadences.
type leaderTimings struct {
	snapshotInterval time.Duration
	renewInterval    time.Duration
	debounce         time.Duration
}

func defaultLeaderTimings() leaderTimings {
	return leaderTimings{
		snapshotInterval: compactionSnapshotInterval,
		renewInterval:    compactionLockRenewBefore,
		debounce:         compactionSnapshotDebounce,
	}
}

// compactedTopicState owns the leader-election goroutine and live cache for
// one compacted topic. Started by EnsureCompactedTopic, stopped by
// ASBBrokerDriver.Close or stopCompactedTopic.
type compactedTopicState struct {
	topic         string
	snapshotQueue string
	lockQueue     string

	driver *ASBBrokerDriver

	mu    sync.RWMutex
	cache map[string][]byte // key -> latest value; tombstoned keys are absent

	// dirty flags "cache changed since the last snapshot". Buffered to 1 and
	// written non-blockingly, so many changes collapse into one signal.
	dirty chan struct{}

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// EnsureCompactedTopic idempotently provisions the topic plus its snapshot and
// leader-lock queues, seeds the cache from any existing snapshot, and starts
// the live-tail and leader-election goroutines.
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
		slog.Warn("Failed to seed compacted-topic cache from existing snapshot; starting empty", "topic", topic, "error", err)
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
// latest value. Raw topic history is never consulted; ASB cannot provide it.
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

// replaySnapshotInto peeks the snapshot queue and invokes visit per key. Peek
// is non-destructive, so the snapshot survives any number of reads. An empty
// queue means no state has been recorded yet, which is not an error.
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

// runLiveTail keeps state.cache current from a per-instance subscription on the
// compacted topic. Separate from subscription.SyncWatcher's: every instance
// needs its own full copy of the stream, not a competing one.
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
			// The cache subscription carries AutoDeleteOnIdle, so a quiet topic
			// can have it reclaimed underneath us. Recreating alone would leave
			// the cache missing anything published while it was gone, so the
			// snapshot is replayed over it before tailing resumes.
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

// rebuildCacheTail recreates a reclaimed cache subscription and refills the
// cache from the snapshot, which is the only remaining record of anything
// published while the subscription was gone.
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

	s.replaceCache(rebuilt)

	_ = stale.Close(context.Background())
	return receiver, nil
}

// replaceCache swaps the whole cache rather than merging into it, so keys the
// snapshot no longer carries are dropped.
func (s *compactedTopicState) replaceCache(next map[string][]byte) {
	s.mu.Lock()
	s.cache = next
	s.mu.Unlock()
}

func (s *compactedTopicState) applyToCache(msg *connectors.Message) {
	s.mu.Lock()
	key := string(msg.Key)
	if msg.Value == nil {
		delete(s.cache, key)
	} else {
		s.cache[key] = msg.Value
	}
	s.mu.Unlock()

	s.markDirty()
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

// drainDirty clears any pending signal. Call before reading the cache, never
// after: draining afterwards could discard the signal for a change the
// snapshot missed.
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
	out := make(map[string][]byte, len(s.cache))
	for k, v := range s.cache {
		out[k] = v
	}
	return out
}

// runLeaderElection contends for the "leader" session and runs as leader while
// it holds the lock.
func (s *compactedTopicState) runLeaderElection(ctx context.Context) {
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
			delay := compactionElectionRetryBase + rand.N(compactionElectionRetryJitterMax)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
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

// seedLeaderSessionBestEffort creates the "leader" session by sending to it.
// Duplicate detection on the queue collapses repeated seeding server-side.
func (s *compactedTopicState) seedLeaderSessionBestEffort(ctx context.Context) {
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

	// Held for the whole term: a fresh AMQP link each cycle could itself consume
	// publishSnapshot's drain timeout.
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

// runLeaderLoop keeps the session lock alive and decides when to snapshot,
// returning when ctx is cancelled or the lock cannot be renewed. The dirty
// signal is the primary trigger; the interval is a safety net. A failed write
// is not fatal
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
			return
		case <-renewTicker.C:
			if err := renewLock(ctx); err != nil {
				slog.Warn("Failed to renew leader session lock, stepping down", "topic", s.topic, "error", err)
				return
			}
		case <-s.dirty:
			select {
			case <-ctx.Done():
				return
			case <-time.After(timings.debounce):
			}
			s.drainDirty()
			if err := publish(ctx); err != nil {
				slog.Error("Failed to publish compacted-topic snapshot after change", "topic", s.topic, "error", err)
			}
		case <-snapshotTicker.C:
			s.drainDirty()
			if err := publish(ctx); err != nil {
				slog.Error("Failed to publish compacted-topic snapshot", "topic", s.topic, "error", err)
			}
		}
	}
}

// publishSnapshot replaces the queue's contents with the current cache.
// Ordering matters: the stale message is locked but not settled until the
// replacement is sent. Locked messages stay visible to PeekMessages, so a
// concurrent Replay always sees one snapshot or the other, never an empty queue.
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
		// Leave stale messages unsettled: their locks lapse and the previous
		// snapshot stays readable.
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

// cacheSubscriptionName names the per-instance live-tail subscription, hashed
// to stay inside ASB's 50-character limit.
func cacheSubscriptionName(runtimeID string) string {
	return "cache-" + shortHash(runtimeID)
}

// syncSubscriptionName names the Watch subscription. Hashed: runtimeID plus an
// API identifier exceeds ASB's 50-character limit outright.
func syncSubscriptionName(consumerID string) string {
	return "sync-" + shortHash(consumerID)
}

func shortHash(value string) string {
	h := sha256.Sum256([]byte(value))
	return hex.EncodeToString(h[:])[:16]
}
