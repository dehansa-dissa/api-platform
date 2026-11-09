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
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/connectors"
)

// ASB has no log compaction, so each instance caches the sync topic and the leader
// writes the cache to a snapshot queue, which a restarting instance reads.

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
	// Slower because every non-leader polls every topic; a crash takeover already waits ~60 s.
	leaderContendRetryBase      = 15 * time.Second
	leaderContendRetryJitterMax = 15 * time.Second

	snapshotPeekPage     = 50
	snapshotCleanupBatch = 20
	snapshotIOTimeout    = 20 * time.Second
	shutdownFlushTimeout = 10 * time.Second

	leaderSessionID     = "leader"
	leaderSeedMessageID = "leader-election-seed"
	leaderSeedBody      = "leader-election-anchor"
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

	subscriptionCreatedAt time.Time
	snapshotReadAt        time.Time
	electionHoldBack      time.Duration
	observers             []*cacheObserver
	cleanupPending        atomic.Bool
	leaderAnchored        bool // election goroutine only

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// EnsureCompactedTopic provisions the topic and its two queues, seeds the cache
// from any existing snapshot, and starts the live-tail and election goroutines.
func (e *ASBBrokerDriver) EnsureCompactedTopic(ctx context.Context, topic string) error {
	home, err := e.homeConn(ctx, topic, true)
	if err != nil {
		return fmt.Errorf("failed to place compacted topic %s in a namespace: %w", topic, err)
	}
	if _, err := createTopic(ctx, home.admin, topic); err != nil {
		return fmt.Errorf("failed to create compacted topic %s: %w", topic, err)
	}

	snapshotQueue := snapshotQueueName(topic)
	lockQueue := leaderLockQueueName(topic)

	if _, err := home.admin.CreateQueue(ctx, snapshotQueue, nil); err != nil && !isConflictErr(err) {
		return fmt.Errorf("failed to create snapshot queue %s: %w", snapshotQueue, err)
	}

	// LockDuration left unset: ASB's 1-minute default already clears
	// compactionLockRenewBefore, and the field wants an ATOM-XML duration.
	if _, err := home.admin.CreateQueue(ctx, lockQueue, &admin.CreateQueueOptions{
		Properties: &admin.QueueProperties{
			RequiresSession:            to.Ptr(true),
			RequiresDuplicateDetection: to.Ptr(true),
			// The window defaults to ten minutes, which lets a restarting
			// instance's anchor through as a new message every time it rolls.
			DuplicateDetectionHistoryTimeWindow: to.Ptr("P1D"),
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
		topic:            topic,
		snapshotQueue:    snapshotQueue,
		lockQueue:        lockQueue,
		driver:           e,
		cache:            make(map[string][]byte),
		dirty:            make(chan struct{}, 1),
		electionHoldBack: compactionSnapshotInterval,
	}

	// Open the subscription before reading the snapshot.
	receiver, err := state.openCacheReceiver(ctx, cacheSubscriptionName(e.instanceID))
	if err != nil {
		slog.Error("Failed to open the compacted-topic cache receiver up front; "+
			"the live tail will retry, leaving a wider startup window",
			"topic", topic, "error", err)
	} else {
		state.subscriptionCreatedAt = time.Now()
	}

	// Seed from the snapshot, which covers state predating the subscription.
	written, err := e.replaySnapshotInto(ctx, snapshotQueue, func(key string, value []byte) {
		state.cache[key] = value
	})
	if err != nil {
		slog.Error("Failed to seed compacted-topic cache from the existing snapshot; "+
			"this instance will not take leadership, so the snapshot is left intact. "+
			"Restart once the namespace is reachable, or purge the snapshot queue if its contents are corrupt",
			"topic", topic, "snapshot_queue", snapshotQueue, "error", err)
	} else {
		state.seeded = true
		state.snapshotReadAt = written
	}

	runCtx, cancel := context.WithCancel(context.Background())
	state.cancel = cancel

	state.wg.Add(2)
	go func() {
		defer state.wg.Done()
		state.runLiveTail(runCtx, receiver)
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
	if _, err := e.homeConn(ctx, topic, false); err != nil {
		if errors.Is(err, errNotPlaced) {
			return nil
		}
		return fmt.Errorf("failed to replay snapshot for topic %s: %w", topic, err)
	}
	var handlerErr error
	replayed := 0
	_, err := e.replaySnapshotInto(ctx, snapshotQueueName(topic), func(key string, value []byte) {
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

// snapshotReceiver is the subset of *azservicebus.Receiver the snapshot paths use,
// so paging and cleanup can be tested without a namespace.
type snapshotReceiver interface {
	PeekMessages(ctx context.Context, maxMessageCount int, options *azservicebus.PeekMessagesOptions) (
		[]*azservicebus.ReceivedMessage, error)
	ReceiveMessages(ctx context.Context, maxMessages int, options *azservicebus.ReceiveMessagesOptions) (
		[]*azservicebus.ReceivedMessage, error)
	CompleteMessage(ctx context.Context, message *azservicebus.ReceivedMessage,
		options *azservicebus.CompleteMessageOptions) error
	AbandonMessage(ctx context.Context, message *azservicebus.ReceivedMessage,
		options *azservicebus.AbandonMessageOptions) error
}

var _ snapshotReceiver = (*azservicebus.Receiver)(nil)

// peekSnapshotQueue opens a receiver and surveys the whole queue.
func (e *ASBBrokerDriver) peekSnapshotQueue(ctx context.Context, snapshotQueue string) (
	*azservicebus.ReceivedMessage, []int64, error) {
	receiver, err := e.home.client.NewReceiverForQueue(snapshotQueue, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open snapshot receiver: %w", err)
	}
	defer receiver.Close(context.Background())
	return peekAllSnapshots(ctx, receiver)
}

// peekAllSnapshots pages the whole queue and reports the newest message plus
// every sequence number found.
func peekAllSnapshots(ctx context.Context, peeker snapshotReceiver) (
	*azservicebus.ReceivedMessage, []int64, error) {
	var (
		newest *azservicebus.ReceivedMessage
		seqs   []int64
		from   int64
		paging bool
	)
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}

		opts := &azservicebus.PeekMessagesOptions{}
		if paging {
			opts.FromSequenceNumber = &from
		}
		batch, err := peeker.PeekMessages(ctx, snapshotPeekPage, opts)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to peek snapshot: %w", err)
		}
		if len(batch) == 0 {
			return newest, seqs, nil
		}

		// Paging advances off the sequence numbers the batch carries, so a batch
		// that yields none would re-read itself forever.
		advanced := false
		for _, msg := range batch {
			if msg.SequenceNumber == nil {
				continue
			}
			seqs = append(seqs, *msg.SequenceNumber)
			if newest == nil || *msg.SequenceNumber > *newest.SequenceNumber {
				newest = msg
			}
			from, paging, advanced = *msg.SequenceNumber+1, true, true
		}
		if !advanced {
			return nil, nil, fmt.Errorf("peeked %d snapshot messages with no sequence number; cannot page", len(batch))
		}
	}
}

// replaySnapshotInto peeks the snapshot queue and invokes visit per key.
func (e *ASBBrokerDriver) replaySnapshotInto(ctx context.Context, snapshotQueue string,
	visit func(key string, value []byte)) (time.Time, error) {
	newest, _, err := e.peekSnapshotQueue(ctx, snapshotQueue)
	if err != nil {
		return time.Time{}, err
	}
	if newest == nil {
		return time.Time{}, nil
	}

	if err := decodeSnapshot(newest.Body, visit); err != nil {
		return time.Time{}, err
	}

	var written time.Time
	if newest.EnqueuedTime != nil {
		written = *newest.EnqueuedTime
	}
	return written, nil
}

// runLiveTail keeps the cache current from a per-instance subscription. receiver
// is nil when EnsureCompactedTopic could not open one, and this opens its own.
func (s *compactedTopicState) runLiveTail(ctx context.Context, receiver *azservicebus.Receiver) {
	subscriptionName := cacheSubscriptionName(s.driver.instanceID)

	if receiver == nil {
		var err error
		receiver, err = s.openCacheReceiver(ctx, subscriptionName)
		if err != nil {
			slog.Error("Failed to open compacted-topic cache receiver; state will not stay current", "topic", s.topic, "error", err)
			return
		}
		s.recordSubscriptionCreated()
	}
	defer func() { _ = receiver.Close(context.Background()) }()

	s.seedFromSnapshotWithRetry(ctx)

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
				if rebuilt, rebuildErr := s.rebuildCacheTail(ctx, subscriptionName); rebuildErr == nil {
					slog.Info("Rebuilt reclaimed compacted-topic cache subscription", "topic", s.topic)
					_ = receiver.Close(context.Background())
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
			msg := receivedMessageToMessage(rm, s.topic)
			if len(msg.Key) == 0 {
				slog.Warn("Skipping a compacted-topic message that carries no key",
					"topic", s.topic, "sequence_number", rm.SequenceNumber)
				if err := receiver.CompleteMessage(ctx, rm, nil); err != nil {
					slog.Error("Failed to settle a keyless compacted-topic message", "topic", s.topic, "error", err)
				}
				continue
			}
			// Cache first, observers second: the snapshot must never lag what an
			// observer has already acted on.
			s.applyToCache(msg)
			s.fanOutToObservers(msg)
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
	receiver, err := s.driver.home.client.NewReceiverForSubscription(s.topic, subscriptionName, &azservicebus.ReceiverOptions{
		ReceiveMode: azservicebus.ReceiveModePeekLock,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create compacted-topic cache receiver: %w", err)
	}
	return receiver, nil
}

// rebuildCacheTail recreates a reclaimed subscription and refills the cache from
// the snapshot. The caller still owns, and closes, the receiver being replaced.
func (s *compactedTopicState) rebuildCacheTail(ctx context.Context, subscriptionName string) (*azservicebus.Receiver, error) {
	receiver, err := s.openCacheReceiver(ctx, subscriptionName)
	if err != nil {
		return nil, err
	}

	rebuilt := make(map[string][]byte)
	written, err := s.driver.replaySnapshotInto(ctx, snapshotQueueName(s.topic), func(key string, value []byte) {
		rebuilt[key] = value
	})
	if err != nil {
		_ = receiver.Close(context.Background())
		return nil, fmt.Errorf("failed to refill cache from snapshot: %w", err)
	}

	s.mu.Lock()
	s.cache = rebuilt
	s.version++
	// Read a snapshot in full, which is what being seeded means, so an instance
	// that started unseeded becomes eligible for leadership here.
	s.seeded = true
	// The new subscription and the snapshot behind it are both current as of now,
	// so the startup window this instance carried is closed.
	s.subscriptionCreatedAt = time.Now()
	s.snapshotReadAt = written
	s.mu.Unlock()

	// The cache was replaced, not advanced, so observers cannot catch up from the
	// message stream alone.
	s.resyncObservers()

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
	// Hold back before contending. so the snapshot is written from a complete cache.
	if !sleepFor(ctx, s.electionHoldBack) {
		return
	}

	// Checked after the hold-back, by which point the live tail's own retry has
	// normally already seeded, so this returns at once.
	if !s.waitUntilSeeded(ctx) {
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
			// A held session proves the anchor exists, so only seed when the failure
			// was something else.
			if !isSessionHeldErr(err) {
				if !isSBTimeoutErr(err) {
					slog.Warn("Leader-election attempt failed", "topic", s.topic, "error", err)
				}
				s.seedLeaderSessionBestEffort(ctx)
			}
			if !sleepFor(ctx, leaderContendRetryBase+rand.N(leaderContendRetryJitterMax)) {
				return
			}
			continue
		}

		slog.Info("Acquired compacted-topic leadership", "topic", s.topic)
		s.warnIfSeedPredatesSubscription()
		s.runAsLeader(ctx, session)
		slog.Info("Stepped down from compacted-topic leadership", "topic", s.topic)
	}
}

// registerObserver attaches o and returns a copy of the cache, both under one
// lock, so no message falls between the copy and the first live delivery.
func (s *compactedTopicState) registerObserver(o *cacheObserver) map[string][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.observers = append(s.observers, o)
	return maps.Clone(s.cache)
}

func (s *compactedTopicState) removeObserver(o *cacheObserver) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, existing := range s.observers {
		if existing == o {
			s.observers = append(s.observers[:i], s.observers[i+1:]...)
			return
		}
	}
}

// fanOutToObservers offers msg to each observer without blocking the tail. A full
// queue costs that observer a re-prime, not the snapshot its freshness.
func (s *compactedTopicState) fanOutToObservers(msg *connectors.Message) {
	s.mu.RLock()
	observers := slices.Clone(s.observers)
	s.mu.RUnlock()

	for _, o := range observers {
		select {
		case o.queue <- msg:
		default:
			o.markStale()
			slog.Warn("Compacted-topic observer fell behind; scheduling a re-prime from cache",
				"topic", s.topic, "queue_depth", observerQueueDepth)
		}
	}
}

// resyncObservers re-primes every observer, for when the cache was replaced
// wholesale rather than advanced one message at a time.
func (s *compactedTopicState) resyncObservers() {
	s.mu.RLock()
	observers := slices.Clone(s.observers)
	s.mu.RUnlock()

	for _, o := range observers {
		o.markStale()
	}
}

// recordSubscriptionCreated marks when this instance's cache subscription began
// receiving, for the provenance check below.
func (s *compactedTopicState) recordSubscriptionCreated() {
	s.mu.Lock()
	s.subscriptionCreatedAt = time.Now()
	s.mu.Unlock()
}

var (
	// errCacheAlreadyLive stops a seed that would undo live changes. It is not
	// retried: the cache only moves forward, so a later attempt cannot become valid.
	errCacheAlreadyLive = errors.New("cache already holds live changes")
	// errAlreadySeeded reports that another path got there first, which is a
	// success for the caller rather than a failure.
	errAlreadySeeded = errors.New("cache is already seeded")
)

func (s *compactedTopicState) isSeeded() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.seeded
}

// seedable reports whether a seed may still be applied. seedableLocked is the
// same check for a caller that already holds the lock.
func (s *compactedTopicState) seedable() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.seedableLocked()
}

func (s *compactedTopicState) seedableLocked() error {
	if s.seeded {
		return errAlreadySeeded
	}
	if s.version != 0 {
		return errCacheAlreadyLive
	}
	return nil
}

// seedFromSnapshot replaces the cache with the snapshot and marks the state
// seeded.
func (s *compactedTopicState) seedFromSnapshot(ctx context.Context) error {
	// Checked before the read as well as after, so a cache that is already live
	// costs no round trip.
	if err := s.seedable(); err != nil {
		if errors.Is(err, errAlreadySeeded) {
			return nil
		}
		return err
	}

	// Read outside the lock: a snapshot peek is a network round trip, and holding
	// the cache against the live tail for its duration would stall delivery.
	seeded := make(map[string][]byte)
	written, err := s.driver.replaySnapshotInto(ctx, s.snapshotQueue, func(key string, value []byte) {
		seeded[key] = value
	})
	if err != nil {
		return err
	}

	s.mu.Lock()
	if err := s.seedableLocked(); err != nil {
		s.mu.Unlock()
		if errors.Is(err, errAlreadySeeded) {
			return nil
		}
		return err
	}
	s.cache = seeded
	s.seeded = true
	s.snapshotReadAt = written
	s.mu.Unlock()

	// The cache was replaced, not advanced, so an observer that registered while
	// it was unseeded holds values this did not keep.
	s.resyncObservers()
	return nil
}

// seedFromSnapshotWithRetry seeds the cache until it succeeds or ctx ends.
func (s *compactedTopicState) seedFromSnapshotWithRetry(ctx context.Context) {
	warned := false
	for !s.isSeeded() {
		err := s.seedFromSnapshot(ctx)
		if err == nil {
			if warned {
				slog.Info("Seeded the compacted-topic cache on a retry", "topic", s.topic)
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errCacheAlreadyLive) {
			slog.Error("Not seeding the compacted-topic cache over live changes; this instance "+
				"will not contend for leadership", "topic", s.topic)
			return
		}

		if !warned {
			slog.Warn("Could not seed the compacted-topic cache from the snapshot; retrying, "+
				"and leadership waits on it", "topic", s.topic, "error", err)
			warned = true
		}
		if !sleepFor(ctx, compactionElectionRetryBase+rand.N(compactionElectionRetryJitterMax)) {
			return
		}
	}
}

// waitUntilSeeded blocks until the cache is seeded, reporting false only when ctx
// ends first.
func (s *compactedTopicState) waitUntilSeeded(ctx context.Context) bool {
	warned := false
	for !s.isSeeded() {
		if !warned {
			slog.Warn("Deferring compacted-topic leadership until the cache is seeded", "topic", s.topic)
			warned = true
		}
		if !sleepFor(ctx, compactionElectionRetryBase+rand.N(compactionElectionRetryJitterMax)) {
			return false
		}
	}
	return true
}

// seedPredatesSubscription reports a cache seeded from a snapshot older than this
// instance's subscription, and the window of changes it may be missing.
func (s *compactedTopicState) seedPredatesSubscription() (bool, time.Duration) {
	s.mu.RLock()
	readAt, createdAt := s.snapshotReadAt, s.subscriptionCreatedAt
	s.mu.RUnlock()

	if readAt.IsZero() || createdAt.IsZero() || !readAt.Before(createdAt) {
		return false, 0
	}
	return true, createdAt.Sub(readAt)
}

func (s *compactedTopicState) warnIfSeedPredatesSubscription() {
	behind, window := s.seedPredatesSubscription()
	if !behind {
		return
	}
	slog.Warn("Taking compacted-topic leadership with a cache seeded before this instance "+
		"could observe the topic; changes published in the window below are absent from the "+
		"snapshot this instance will write",
		"topic", s.topic, "window", window)
}

func (s *compactedTopicState) tryAcquireLeadership(ctx context.Context) (*azservicebus.SessionReceiver, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, compactionElectionAttemptTimeout)
	defer cancel()
	return s.driver.home.client.AcceptSessionForQueue(attemptCtx, s.lockQueue, leaderSessionID, &azservicebus.SessionReceiverOptions{
		ReceiveMode: azservicebus.ReceiveModePeekLock,
	})
}

// shouldSeedLeaderSession reports whether an anchor is needed. The session exists
// only while the lock queue holds a message, so the count answers directly.
func shouldSeedLeaderSession(activeCount int32, probeErr error) bool {
	return probeErr != nil || activeCount == 0
}

// seedLeaderSessionBestEffort creates the "leader" session by sending an anchor,
// only when the probe finds the queue empty.
func (s *compactedTopicState) seedLeaderSessionBestEffort(ctx context.Context) {
	// An anchor is never consumed, so once one is known the probe is wasted load.
	if s.leaderAnchored {
		return
	}
	probeCtx, cancelProbe := context.WithTimeout(ctx, snapshotIOTimeout)
	props, probeErr := s.driver.home.admin.GetQueueRuntimeProperties(probeCtx, s.lockQueue, nil)
	cancelProbe()

	var active int32
	if probeErr == nil {
		active = props.ActiveMessageCount
	}
	if !shouldSeedLeaderSession(active, probeErr) {
		s.leaderAnchored = true
		slog.Debug("Leader session already anchored; not seeding",
			"topic", s.topic, "anchors", active)
		return
	}

	defer s.driver.home.publisher.closeSender(s.lockQueue)

	sender, err := s.driver.home.publisher.senderFor(s.lockQueue)
	if err != nil {
		slog.Warn("Failed to open leader-lock seed sender", "topic", s.topic, "error", err)
		return
	}
	sendCtx, cancel := context.WithTimeout(ctx, snapshotIOTimeout)
	defer cancel()
	if err := sender.SendMessage(sendCtx, &azservicebus.Message{
		Body:      []byte(leaderSeedBody),
		SessionID: to.Ptr(leaderSessionID),
		MessageID: to.Ptr(leaderSeedMessageID),
	}, nil); err != nil {
		slog.Debug("Leader-lock seed send did not complete (likely already seeded)", "topic", s.topic, "error", err)
		return
	}
	s.leaderAnchored = true

	// Logged because an anchor is never consumed, so every extra one stays forever.
	slog.Info("Seeded the leader-lock session", "topic", s.topic, "probe_error", probeErr)
}

// runAsLeader binds Azure resources for a leadership term and delegates
// scheduling to runLeaderLoop, which is testable without a namespace.
func (s *compactedTopicState) runAsLeader(ctx context.Context, session *azservicebus.SessionReceiver) {
	defer session.Close(context.Background())

	s.runLeaderLoop(ctx, defaultLeaderTimings(),
		func(ctx context.Context) error {
			renewCtx, cancel := context.WithTimeout(ctx, sessionRenewTimeout)
			defer cancel()
			return session.RenewSessionLock(renewCtx, nil)
		},
		s.publishSnapshot,
		s.retryCleanup,
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
	reconcile func(context.Context),
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
			if !s.hasUnpublishedChanges() {
				reconcile(ctx)
				continue
			}
			s.drainDirty()
			s.publishAndReport(ctx, publish, "Failed to publish compacted-topic snapshot")
		}
	}
}

// retryCleanup finishes a cleanup an earlier write could not complete.
func (s *compactedTopicState) retryCleanup(ctx context.Context) {
	if s.cleanupPending.Load() {
		s.cleanupSnapshots(ctx)
	}
}

// cleanupSnapshots deletes every snapshot older than the newest. Keeping the
// newest is the only guard needed, so a retry is always safe.
func (s *compactedTopicState) cleanupSnapshots(ctx context.Context) {
	if err := s.deleteOlderSnapshots(ctx); err != nil {
		s.cleanupPending.Store(true)
		if ctx.Err() == nil {
			slog.Warn("Snapshot cleanup did not finish; it will be retried", "topic", s.topic, "error", err)
		}
		return
	}
	s.cleanupPending.Store(false)
}

func (s *compactedTopicState) deleteOlderSnapshots(ctx context.Context) error {
	peekCtx, cancel := context.WithTimeout(ctx, snapshotIOTimeout)
	newest, seqs, err := s.driver.peekSnapshotQueue(peekCtx, s.snapshotQueue)
	cancel()
	if err != nil {
		return err
	}
	if newest == nil || len(seqs) <= 1 {
		return nil
	}

	receiver, err := s.driver.home.client.NewReceiverForQueue(s.snapshotQueue, &azservicebus.ReceiverOptions{
		ReceiveMode: azservicebus.ReceiveModePeekLock,
	})
	if err != nil {
		return fmt.Errorf("failed to open the snapshot cleanup receiver: %w", err)
	}
	defer receiver.Close(context.Background())

	deleted, err := settleStaleSnapshots(ctx, receiver, *newest.SequenceNumber-1, len(seqs)-1)
	if err != nil {
		return err
	}
	slog.Debug("Collected stale snapshots", "topic", s.topic, "deleted", deleted)
	return nil
}

// encodeSnapshot marshals the cache, writing JSON-object values as themselves
// rather than as base64, which keeps the snapshot a third smaller.
func encodeSnapshot(cache map[string][]byte) ([]byte, error) {
	raw := make(map[string]json.RawMessage, len(cache))
	for key, value := range cache {
		if len(value) > 0 && value[0] == '{' && json.Valid(value) {
			raw[key] = value
			continue
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal snapshot value for key %q: %w", key, err)
		}
		raw[key] = encoded
	}
	return json.Marshal(raw)
}

// decodeSnapshot reverses encodeSnapshot, invoking visit per key.
func decodeSnapshot(body []byte, visit func(key string, value []byte)) error {
	var snapshot map[string]json.RawMessage
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot payload: %w", err)
	}
	for key, value := range snapshot {
		// A JSON object is the value itself. Anything else went through []byte —
		// a base64 string.
		if len(value) > 0 && value[0] == '{' {
			visit(key, value)
			continue
		}
		var boxed []byte
		if err := json.Unmarshal(value, &boxed); err != nil {
			return fmt.Errorf("failed to decode snapshot value for key %q: %w", key, err)
		}
		visit(key, boxed)
	}
	return nil
}

// publishSnapshot writes the cache as the newest snapshot, then deletes the
// copies that preceded it.
func (s *compactedTopicState) publishSnapshot(ctx context.Context) error {
	payload, err := encodeSnapshot(s.snapshotCache())
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	sender, err := s.driver.home.publisher.senderFor(s.snapshotQueue)
	if err != nil {
		return fmt.Errorf("failed to open snapshot sender: %w", err)
	}
	sendCtx, cancelSend := context.WithTimeout(ctx, snapshotIOTimeout)
	defer cancelSend()
	if err := sender.SendMessage(sendCtx, &azservicebus.Message{Body: payload}, nil); err != nil {
		return fmt.Errorf("failed to send snapshot: %w", err)
	}

	s.cleanupSnapshots(ctx)
	return nil
}

// settleStaleSnapshots deletes up to count messages at or below mark, reporting
// how many it removed.
func settleStaleSnapshots(ctx context.Context, receiver snapshotReceiver, mark int64, count int) (int, error) {
	deleted := 0
	for deleted < count {
		recvCtx, cancel := context.WithTimeout(ctx, snapshotIOTimeout)
		messages, err := receiver.ReceiveMessages(recvCtx, min(snapshotCleanupBatch, count-deleted), nil)
		cancel()
		if err != nil {
			return deleted, fmt.Errorf("failed to receive stale snapshots: %w", err)
		}
		if len(messages) == 0 {
			return deleted, nil
		}

		for _, msg := range messages {
			if msg.SequenceNumber != nil && *msg.SequenceNumber > mark {
				// Released rather than held to lock expiry, so the next pass sees it.
				if err := receiver.AbandonMessage(ctx, msg, nil); err != nil {
					slog.Debug("Failed to abandon a snapshot above the cleanup mark", "error", err)
				}
				return deleted, nil
			}
			if err := receiver.CompleteMessage(ctx, msg, nil); err != nil {
				return deleted, fmt.Errorf("failed to delete a stale snapshot: %w", err)
			}
			deleted++
		}
	}
	return deleted, nil
}

func snapshotQueueName(topic string) string   { return topic + "-snapshot" }
func leaderLockQueueName(topic string) string { return topic + "-leader-lock" }

func cacheSubscriptionName(runtimeID string) string {
	return "cache-" + shortHash(runtimeID)
}

func shortHash(value string) string {
	h := sha256.Sum256([]byte(value))
	return hex.EncodeToString(h[:])[:16]
}
