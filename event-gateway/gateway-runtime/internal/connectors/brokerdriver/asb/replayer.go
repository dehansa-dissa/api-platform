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

// Kafka answers "what's the current subscription state after a restart?" via
// log compaction — replaying a compacted topic from offset 0 yields exactly
// the latest value per key, because Kafka retains full topic history and
// compacts it in the background. ASB has no equivalent: once a message is
// consumed it's gone, and a brand-new Subscription created "now" never
// receives messages published to the topic before it existed, so there is no
// way to ask ASB itself to replay a topic's current state.
//
// Instead, every runtime instance keeps its own in-memory fold of the
// compacted topic (compactedTopicState.cache), fed by a per-instance,
// continuously-running internal subscription (not the higher-level
// subscription.SyncWatcher's own subscription — a separate one, private to
// this file, that exists purely to keep this cache current). Periodically,
// whichever instance holds the leader-lock session serializes its cache to
// JSON and replaces the single message sitting in a dedicated snapshot queue.
// EnsureCompactedTopic seeds a new instance's cache from that snapshot before
// starting its own live tail, and Replay reads only from the snapshot — never
// from raw topic history, which ASB cannot provide.
//
// Leadership uses a session-enabled queue with a single well-known session ID
// ("leader"). ASB sessions are exclusively lockable: only one receiver can
// hold a given session ID at a time, and the lock is server-managed — it
// auto-expires if the holder disappears without an explicit release, which is
// exactly the "auto-releases on crash" behaviour needed here. Every instance's
// cache is already current (each maintains its own independent live tail), so
// whichever instance next acquires the session lock can resume publishing
// accurate snapshots immediately, with no replay/rebuild gap.
const (
	compactionSnapshotInterval       = 30 * time.Second
	compactionLockRenewBefore        = 20 * time.Second // renew comfortably inside the lock's duration
	compactionElectionAttemptTimeout = 10 * time.Second
	compactionElectionRetryBase      = 3 * time.Second
	compactionElectionRetryJitterMax = 3 * time.Second
)

// snapshotPeekCount bounds how many snapshot-queue messages are examined at
// once. The queue holds one message in the steady state; the headroom absorbs
// an in-flight republish or a cleanup that previously failed.
const snapshotPeekCount = 10

// snapshotDrainTimeout bounds how long publishSnapshot waits to find a stale
// message to clear before republishing. Short, because the receiver is
// long-lived (held for the whole leadership term, see runAsLeader) rather
// than freshly opened each cycle — the only time this is expected to time out
// is the very first cycle, before any snapshot has ever been written.
const snapshotDrainTimeout = 2 * time.Second

// Azure Service Bus caps a queue or topic name at 260 characters.
const maxEntityNameLength = 260

// leaderSessionID is the fixed, well-known session ID that every runtime
// instance competes to lock on a topic's leader-lock queue.
const leaderSessionID = "leader"

// leaderSeedMessageID is a fixed MessageID used together with the leader-lock
// queue's RequiresDuplicateDetection setting so that repeated seed attempts
// (every instance calling EnsureCompactedTopic at startup) are deduplicated
// server-side instead of accumulating unconsumed messages under the session.
const leaderSeedMessageID = "leader-election-seed"

// compactedTopicState owns the leader-election goroutine and live cache for
// one compacted topic. Started by EnsureCompactedTopic, stopped by
// ASBBrokerDriver.Close or stopCompactedTopic (on DeleteTopics).
type compactedTopicState struct {
	topic         string
	snapshotQueue string
	lockQueue     string

	driver *ASBBrokerDriver

	mu    sync.RWMutex
	cache map[string][]byte // compaction key -> latest value; tombstoned keys are simply absent

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// EnsureCompactedTopic provisions a topic plus its snapshot and leader-lock
// queues (idempotent), seeds this instance's cache from any existing snapshot,
// and starts the live-tail and leader-election goroutines. Safe to call more
// than once for the same topic (for example on every startup) — subsequent
// calls only re-verify the entities exist.
func (e *ASBBrokerDriver) EnsureCompactedTopic(ctx context.Context, topic string) error {
	if _, err := e.admin.CreateTopic(ctx, topic, nil); err != nil && !isConflictErr(err) {
		return fmt.Errorf("failed to create compacted topic %s: %w", topic, err)
	}

	snapshotQueue, err := snapshotQueueName(topic)
	if err != nil {
		return err
	}
	lockQueue, err := leaderLockQueueName(topic)
	if err != nil {
		return err
	}

	if _, err := e.admin.CreateQueue(ctx, snapshotQueue, nil); err != nil && !isConflictErr(err) {
		return fmt.Errorf("failed to create snapshot queue %s: %w", snapshotQueue, err)
	}

	// LockDuration is left unset: ASB's default (1 minute) already leaves ample
	// headroom over compactionLockRenewBefore, and the property is a string in
	// ASB's ATOM-XML duration format rather than a plain time.Duration, so
	// relying on the documented default avoids a format-mismatch risk here.
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
	}

	// Seed from any existing snapshot before starting the live tail, so this
	// instance's cache reflects state that predates it.
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

// stopCompactedTopic stops and removes the leader-election/live-tail
// goroutines for topic, if running.
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

// Replay reads the current compacted-topic snapshot and calls handler once per
// key with that key's latest value. It never touches raw topic history (ASB
// cannot provide it) — only the snapshot maintained by the leader-election
// goroutine below.
func (e *ASBBrokerDriver) Replay(ctx context.Context, topic string, handler connectors.MessageHandler) error {
	snapshotQueue, err := snapshotQueueName(topic)
	if err != nil {
		return err
	}

	var handlerErr error
	replayed := 0
	err = e.replaySnapshotInto(ctx, snapshotQueue, func(key string, value []byte) {
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

// replaySnapshotInto peeks (never locks or consumes) the snapshot queue's
// single message and invokes visit for each key in it. A missing snapshot
// (queue empty — no leader has published one yet) is not an error: it means
// the compacted topic has no recorded state yet, which is the correct starting
// condition for a brand-new topic.
func (e *ASBBrokerDriver) replaySnapshotInto(ctx context.Context, snapshotQueue string, visit func(key string, value []byte)) error {
	receiver, err := e.client.NewReceiverForQueue(snapshotQueue, nil)
	if err != nil {
		return fmt.Errorf("failed to open snapshot receiver: %w", err)
	}
	defer receiver.Close(ctx)

	// Peek several: publishSnapshot converges the queue back to a single
	// message, but a republish whose cleanup failed can transiently leave an
	// older one alongside the newest. Peek sees locked messages too, so the
	// in-flight republish window is covered as well. Highest SequenceNumber
	// is always the most recent snapshot.
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

// runLiveTail maintains state.cache from a per-instance internal subscription
// on the raw compacted topic, applying each publish (upsert) or tombstone
// (delete) as it arrives. This is intentionally separate from the runtime's
// own subscription.SyncWatcher subscription — every runtime instance needs its
// own independent, full copy of the stream to keep its own cache current, not
// a shared/competing one.
func (s *compactedTopicState) runLiveTail(ctx context.Context) {
	subscriptionName := cacheSubscriptionName(s.driver.instanceID)
	if err := s.driver.ensureSubscription(ctx, s.topic, subscriptionName); err != nil {
		slog.Error("Failed to ensure compacted-topic cache subscription; state will not stay current", "topic", s.topic, "error", err)
		return
	}

	receiver, err := s.driver.client.NewReceiverForSubscription(s.topic, subscriptionName, &azservicebus.ReceiverOptions{
		ReceiveMode: azservicebus.ReceiveModePeekLock,
	})
	if err != nil {
		slog.Error("Failed to create compacted-topic cache receiver", "topic", s.topic, "error", err)
		return
	}
	defer receiver.Close(context.Background())

	for {
		if ctx.Err() != nil {
			return
		}
		messages, err := receiver.ReceiveMessages(ctx, 10, nil)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("Compacted-topic cache receive error", "topic", s.topic, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		for _, rm := range messages {
			s.applyToCache(receivedMessageToMessage(rm, s.topic))
			if err := receiver.CompleteMessage(ctx, rm, nil); err != nil {
				slog.Error("Failed to complete compacted-topic cache message", "topic", s.topic, "error", err)
			}
		}
	}
}

func (s *compactedTopicState) applyToCache(msg *connectors.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(msg.Key)
	if msg.Value == nil {
		delete(s.cache, key)
		return
	}
	s.cache[key] = msg.Value
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

// runLeaderElection repeatedly attempts to lock the well-known "leader"
// session on the lock queue; once held, it periodically publishes the current
// cache as the topic's snapshot until the lock is lost or ctx is cancelled.
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

// seedLeaderSessionBestEffort ensures the "leader" session exists by sending a
// fixed-MessageID message to it. The lock queue's RequiresDuplicateDetection
// setting (see EnsureCompactedTopic) means repeated calls across many
// instances and restarts collapse into a single message server-side, so this
// never accumulates backlog under the session.
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

func (s *compactedTopicState) runAsLeader(ctx context.Context, session *azservicebus.SessionReceiver) {
	defer session.Close(context.Background())

	// One receiver, held for the whole leadership term, rather than opened
	// and closed on every snapshotTicker tick. Re-establishing an AMQP link
	// to Azure on every cycle just to check for a stale message was slow
	// enough on its own to occasionally eat the entire drain timeout below,
	// which read as "no message found" even when one was actually sitting in
	// the queue.
	snapshotReceiver, err := s.driver.client.NewReceiverForQueue(s.snapshotQueue, &azservicebus.ReceiverOptions{
		ReceiveMode: azservicebus.ReceiveModePeekLock,
	})
	if err != nil {
		slog.Error("Failed to open snapshot receiver, stepping down", "topic", s.topic, "error", err)
		return
	}
	defer snapshotReceiver.Close(context.Background())

	snapshotTicker := time.NewTicker(compactionSnapshotInterval)
	defer snapshotTicker.Stop()
	renewTicker := time.NewTicker(compactionLockRenewBefore)
	defer renewTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-renewTicker.C:
			renewCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := session.RenewSessionLock(renewCtx, nil)
			cancel()
			if err != nil {
				slog.Warn("Failed to renew leader session lock, stepping down", "topic", s.topic, "error", err)
				return
			}
		case <-snapshotTicker.C:
			if err := s.publishSnapshot(ctx, snapshotReceiver); err != nil {
				slog.Error("Failed to publish compacted-topic snapshot", "topic", s.topic, "error", err)
			}
		}
	}
}

// publishSnapshot replaces the snapshot queue's contents with the current
// cache, converging the queue back to exactly one message. receiver is held
// by the caller for the whole leadership term (see runAsLeader) rather than
// opened fresh here — an empty ASB queue makes ReceiveMessages block for the
// full drain timeout waiting for a message that will never come, and doing
// that on a brand-new AMQP link every cycle made the link-setup time count
// against that same budget.
//
// Ordering matters here. The stale message is locked first but *not* settled
// until the replacement has been sent successfully — a locked message is
// still visible to PeekMessages, so a concurrent Replay always sees either the
// old snapshot or the new one, never an empty queue. Settling first would open
// a window where a restarting instance reads zero subscriptions, seeds an
// empty cache, and — on becoming leader — publishes that emptiness back over
// real state.
func (s *compactedTopicState) publishSnapshot(ctx context.Context, receiver *azservicebus.Receiver) error {
	payload, err := json.Marshal(s.snapshotCache())
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	drainCtx, cancel := context.WithTimeout(ctx, snapshotDrainTimeout)
	stale, err := receiver.ReceiveMessages(drainCtx, snapshotPeekCount, nil)
	cancel()
	if err != nil && ctx.Err() == nil {
		// Expected and harmless the very first cycle after EnsureCompactedTopic,
		// before any snapshot has ever been written — the queue really is empty
		// and ReceiveMessages waits out the full timeout to confirm that.
		slog.Debug("No stale snapshot message found to clear before republish", "topic", s.topic, "error", err)
	}

	sender, err := s.driver.publisher.senderFor(s.snapshotQueue)
	if err != nil {
		return fmt.Errorf("failed to open snapshot sender: %w", err)
	}
	if err := sender.SendMessage(ctx, &azservicebus.Message{Body: payload}, nil); err != nil {
		// Leave the stale messages locked but unsettled: their locks lapse and
		// the previous snapshot stays readable until the next attempt.
		return fmt.Errorf("failed to send snapshot: %w", err)
	}

	// The replacement is durable now, so the old copies can go. A failure here
	// is self-healing: the extra message lingers, and readers pick the highest
	// SequenceNumber until the next cycle clears it.
	for _, msg := range stale {
		if err := receiver.CompleteMessage(ctx, msg, nil); err != nil {
			slog.Warn("Failed to settle stale snapshot message", "topic", s.topic, "error", err)
		}
	}
	return nil
}

// snapshotQueueName derives the per-compacted-topic snapshot queue name.
// event-gateway topic names are always a 64-character SHA-256 hex digest
// (binding.JoinNormalizedTopic), so the suffixed name never approaches the
// 260-character ASB entity limit in practice — the check below is a defensive
// fail-closed guard, not a workaround for an expected case.
func snapshotQueueName(topic string) (string, error) {
	name := topic + "-snapshot"
	if len(name) > maxEntityNameLength {
		return "", fmt.Errorf("derived snapshot queue name for topic %q exceeds the %d-character ASB entity name limit", topic, maxEntityNameLength)
	}
	return name, nil
}

// leaderLockQueueName derives the per-compacted-topic leader-election queue name.
func leaderLockQueueName(topic string) (string, error) {
	name := topic + "-leader-lock"
	if len(name) > maxEntityNameLength {
		return "", fmt.Errorf("derived leader-lock queue name for topic %q exceeds the %d-character ASB entity name limit", topic, maxEntityNameLength)
	}
	return name, nil
}

// cacheSubscriptionName derives the per-instance subscription name used to keep
// a compacted topic's live cache current. Hashed and truncated to stay within
// the 50-character ASB subscription-name limit regardless of runtimeID's length.
func cacheSubscriptionName(runtimeID string) string {
	return "cache-" + shortHash(runtimeID)
}

// syncSubscriptionName derives the Watch subscription name for a consumerID.
// Hashed for the same reason as cacheSubscriptionName: the caller composes
// consumerID from a runtimeID plus an API identifier, which together exceed
// ASB's 50-character subscription-name limit outright.
func syncSubscriptionName(consumerID string) string {
	return "sync-" + shortHash(consumerID)
}

func shortHash(value string) string {
	h := sha256.Sum256([]byte(value))
	return hex.EncodeToString(h[:])[:16]
}
