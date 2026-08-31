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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/connectors"
)

// Azure Service Bus caps a subscription name at 50 characters.
const maxSubscriptionNameLength = 50

// settleMode controls how a Consumer settles a message after the handler runs,
// matching the two Kafka consumer variants:
//   - settleAutoComplete: always Complete, regardless of handler outcome. Used
//     for Subscribe/Watch, mirroring Kafka's auto-commit Consumer, which
//     advances past a record whether or not the handler succeeded.
//   - settleOnHandlerResult: Complete on success, DeadLetter on failure. Used
//     for SubscribeManual.
//
// Neither mode uses AbandonMessage on handler failure. AbandonMessage would
// make ASB immediately redeliver the message and re-drive the handler, which
// for the WebSub delivery path (see consumer_manager.go) would duplicate the
// Deliverer's own retry-with-backoff — by the time the handler returns an
// error there, the Deliverer has already exhausted its own retries, so a
// second, independent redelivery loop would only re-attempt a delivery that
// has already been given every retry it's going to get. DeadLettering instead
// keeps the failure visible (in the subscription's dead-letter subqueue)
// without double-retrying. A message lock that expires because the process
// crashed mid-handling (not because the handler returned an error) still
// redelivers normally — that's the intended crash-recovery path, not a retry
// mechanism.
type settleMode int

const (
	settleAutoComplete settleMode = iota
	settleOnHandlerResult
)

// Consumer consumes events from a single ASB topic through one subscription.
type Consumer struct {
	receiver *azservicebus.Receiver
	topic    string
	handler  connectors.MessageHandler
	mode     settleMode

	// ephemeral marks a subscription whose lifetime is exactly this
	// consumer's: Stop deletes it from the namespace instead of leaving it
	// behind. Set only by NewConsumer, whose callers (the per-connection
	// WebSocket paths) mint a fresh groupID per connection — without this
	// every connection would strand a durable ASB Subscription that nothing
	// ever removes, and ASB caps subscriptions at 2,000 per topic.
	//
	// Durable subscribers leave this false: NewManualCommitConsumer (WebSub
	// delivery, deleted explicitly on unsubscribe) and Watch (cross-instance
	// sync, whose read position must survive restarts).
	ephemeral bool
	driver    *ASBBrokerDriver
	groupID   string

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Start begins consuming events.
func (c *Consumer) Start(ctx context.Context) error {
	ctx, c.cancel = context.WithCancel(ctx)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.consumeLoop(ctx)
	}()
	return nil
}

// Stop stops the consumer, waits for the consume loop to exit, and closes the
// underlying ASB receiver link. An ephemeral consumer also deletes its
// subscription, since nothing else will.
func (c *Consumer) Stop(ctx context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	closeErr := c.receiver.Close(ctx)

	if c.ephemeral && c.driver != nil {
		if err := c.driver.DeleteSubscription(ctx, c.groupID, c.topic); err != nil {
			slog.Error("Failed to delete ephemeral asb subscription",
				"topic", c.topic, "subscription", c.groupID, "error", err)
		}
	}

	return closeErr
}

func (c *Consumer) consumeLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		messages, err := c.receiver.ReceiveMessages(ctx, 1, nil)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("ASB consumer receive error", "topic", c.topic, "error", err)
			// Avoid a tight error loop against a misbehaving/unreachable namespace.
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		for _, rm := range messages {
			c.handleMessage(ctx, rm)
		}
	}
}

func (c *Consumer) handleMessage(ctx context.Context, rm *azservicebus.ReceivedMessage) {
	slog.Debug("[7] Message read from ASB",
		"topic", c.topic,
		"sequence_number", rm.SequenceNumber,
		"delivery_count", rm.DeliveryCount,
		"size_bytes", len(rm.Body))

	msg := receivedMessageToMessage(rm, c.topic)
	handlerErr := c.handler(ctx, msg)
	if handlerErr != nil {
		slog.Error("Message handler error",
			"topic", c.topic,
			"sequence_number", rm.SequenceNumber,
			"error", handlerErr,
		)
	}

	if c.mode == settleOnHandlerResult && handlerErr != nil {
		reason := "handler-failed"
		desc := handlerErr.Error()
		if err := c.receiver.DeadLetterMessage(ctx, rm, &azservicebus.DeadLetterOptions{
			Reason:           &reason,
			ErrorDescription: &desc,
		}); err != nil {
			slog.Error("Failed to dead-letter ASB message", "topic", c.topic, "error", err)
		}
		return
	}

	if err := c.receiver.CompleteMessage(ctx, rm, nil); err != nil {
		slog.Error("Failed to complete ASB message", "topic", c.topic, "error", err)
	}
}

// MultiTopicConsumer fans Start/Stop out to one Consumer per topic, so a
// single subscriber reading several topics (see ConsumerManager) is still
// exposed as one connectors.Receiver. Kafka gets this for free — one client
// consumes many topics — while an ASB receiver is bound to a single entity.
type MultiTopicConsumer struct {
	consumers []*Consumer
}

// Start begins consuming on every topic, unwinding cleanly if any fails.
// Stop is safe on a consumer that never started, so the unwind covers the
// whole set rather than only the ones already running — otherwise the
// remainder would strand open receivers and, for ephemeral consumers, their
// subscriptions.
func (m *MultiTopicConsumer) Start(ctx context.Context) error {
	for _, c := range m.consumers {
		if err := c.Start(ctx); err != nil {
			for _, other := range m.consumers {
				_ = other.Stop(context.Background())
			}
			return err
		}
	}
	return nil
}

// Stop stops every per-topic consumer, returning the first error seen.
func (m *MultiTopicConsumer) Stop(ctx context.Context) error {
	var firstErr error
	for _, c := range m.consumers {
		if err := c.Stop(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// NewConsumer creates a consumer that auto-completes every message regardless
// of handler outcome, mirroring the Kafka driver's auto-commit Consumer.
//
// Subscriptions created here are ephemeral — Stop deletes them. Callers that
// need the subscription to outlive the consumer must use
// NewManualCommitConsumer or Watch instead.
func NewConsumer(e *ASBBrokerDriver, groupID string, topics []string, handler connectors.MessageHandler) (*MultiTopicConsumer, error) {
	return newMultiTopicConsumer(e, groupID, topics, handler, settleAutoComplete, true)
}

// NewManualCommitConsumer creates a consumer that completes on handler success
// and dead-letters on handler failure — the ASB analogue of Kafka's manual
// offset commit, where a failed record must not be silently advanced past.
// The subscription is durable and survives Stop.
func NewManualCommitConsumer(e *ASBBrokerDriver, groupID string, topics []string, handler connectors.MessageHandler) (*MultiTopicConsumer, error) {
	return newMultiTopicConsumer(e, groupID, topics, handler, settleOnHandlerResult, false)
}

func newMultiTopicConsumer(e *ASBBrokerDriver, groupID string, topics []string, handler connectors.MessageHandler, mode settleMode, ephemeral bool) (*MultiTopicConsumer, error) {
	subscriptionName, err := subscriptionNameFor(groupID)
	if err != nil {
		return nil, err
	}
	if len(topics) == 0 {
		return nil, fmt.Errorf("asb subscribe requires at least one topic")
	}

	ctx := context.Background()
	consumers := make([]*Consumer, 0, len(topics))
	created := make([]string, 0, len(topics))

	// unwind releases everything this call has built so far. Ephemeral
	// subscriptions are deleted too: nothing else knows about them, so a
	// half-built consumer would otherwise strand them in the namespace.
	unwind := func() {
		for _, c := range consumers {
			if err := c.receiver.Close(ctx); err != nil {
				slog.Warn("Failed to close asb receiver during unwind", "topic", c.topic, "error", err)
			}
		}
		if !ephemeral {
			return
		}
		for _, topic := range created {
			if err := e.DeleteSubscription(ctx, subscriptionName, topic); err != nil {
				slog.Warn("Failed to delete asb subscription during unwind",
					"topic", topic, "subscription", subscriptionName, "error", err)
			}
		}
	}

	for _, topic := range topics {
		if err := e.ensureSubscription(ctx, topic, subscriptionName); err != nil {
			unwind()
			return nil, fmt.Errorf("failed to ensure asb subscription %s on topic %s: %w", subscriptionName, topic, err)
		}
		created = append(created, topic)

		receiver, err := e.client.NewReceiverForSubscription(topic, subscriptionName, &azservicebus.ReceiverOptions{
			ReceiveMode: azservicebus.ReceiveModePeekLock,
		})
		if err != nil {
			unwind()
			return nil, fmt.Errorf("failed to create asb receiver for topic %s subscription %s: %w", topic, subscriptionName, err)
		}

		consumers = append(consumers, &Consumer{
			receiver:  receiver,
			topic:     topic,
			handler:   handler,
			mode:      mode,
			ephemeral: ephemeral,
			driver:    e,
			groupID:   subscriptionName,
		})
	}

	return &MultiTopicConsumer{consumers: consumers}, nil
}

// Subscribe creates a consumer for the given topics using a shared
// subscription name (groupID). The returned Receiver must be Start()ed by the
// caller.
func (e *ASBBrokerDriver) Subscribe(groupID string, topics []string, handler connectors.MessageHandler) (connectors.Receiver, error) {
	return NewConsumer(e, groupID, topics, handler)
}

// SubscribeManual creates a consumer that settles on handler outcome for the
// given topics.
func (e *ASBBrokerDriver) SubscribeManual(groupID string, topics []string, handler connectors.MessageHandler) (connectors.Receiver, error) {
	return NewManualCommitConsumer(e, groupID, topics, handler)
}

// Watch tails a topic from the current position and delivers all future
// messages to handler until ctx is cancelled. consumerID is used as a stable
// subscription-name suffix so the read position survives restarts, mirroring
// the Kafka driver's consumer-group-per-consumerID approach.
func (e *ASBBrokerDriver) Watch(ctx context.Context, consumerID string, topic string, handler connectors.MessageHandler) (connectors.Receiver, error) {
	subscriptionName := syncSubscriptionName(consumerID)

	if err := e.ensureSubscription(ctx, topic, subscriptionName); err != nil {
		return nil, fmt.Errorf("failed to ensure asb watch subscription %s on topic %s: %w", subscriptionName, topic, err)
	}

	receiver, err := e.client.NewReceiverForSubscription(topic, subscriptionName, &azservicebus.ReceiverOptions{
		ReceiveMode: azservicebus.ReceiveModePeekLock,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create asb watch receiver for topic %s: %w", topic, err)
	}

	return &Consumer{
		receiver: receiver,
		topic:    topic,
		handler:  handler,
		mode:     settleAutoComplete,
	}, nil
}

// ensureSubscription idempotently creates an ASB subscription on topic.
// HTTP 409 (already exists) is treated as success, matching EnsureTopics.
func (e *ASBBrokerDriver) ensureSubscription(ctx context.Context, topic, subscriptionName string) error {
	if _, err := e.admin.CreateSubscription(ctx, topic, subscriptionName, nil); err != nil && !isConflictErr(err) {
		return err
	}
	return nil
}

// DeleteSubscription removes a subscriber's durable ASB Subscription on topic.
//
// This is not part of connectors.BrokerDriver: Kafka has no equivalent entity
// to delete, since a consumer group is implicit and broker-managed. The WebSub
// ConsumerManager discovers this method through an optional-capability type
// assertion, so drivers with nothing to clean up need no changes. An ASB
// Subscription, by contrast, is a persistent entity that keeps accumulating
// messages until explicitly removed.
func (e *ASBBrokerDriver) DeleteSubscription(ctx context.Context, groupID string, topic string) error {
	if _, err := e.admin.DeleteSubscription(ctx, topic, groupID, nil); err != nil {
		if isNotFoundErr(err) {
			return nil
		}
		return fmt.Errorf("failed to delete asb subscription %s on topic %s: %w", groupID, topic, err)
	}
	return nil
}

// subscriptionNameFor maps a broker-agnostic groupID onto a valid ASB
// subscription name. Group IDs are sized for Kafka, which has no practical
// consumer-group length limit, so some callers exceed ASB's 50 characters —
// the per-connection WebSocket paths compose prefix + "-ws-" + a UUID, which
// lands at 53. Rather than reject those (they are legitimate subscribers),
// hash anything that doesn't fit.
//
// Short names pass through unchanged so the common case stays readable in the
// Azure portal, and the mapping is deterministic either way: the same groupID
// always resolves to the same subscription, which is what lets a durable
// subscriber find its subscription again after a restart.
func subscriptionNameFor(groupID string) (string, error) {
	if groupID == "" {
		return "", fmt.Errorf("asb subscription name must not be empty")
	}
	if len(groupID) <= maxSubscriptionNameLength {
		return groupID, nil
	}
	return "grp-" + shortHash(groupID), nil
}

// validateSubscriptionName reports whether a name derived internally by this
// package is usable as-is. It fails closed rather than silently truncating —
// a truncated name could collide with another subscriber's subscription on
// the same topic, which is a correctness bug, not a cosmetic one.
func validateSubscriptionName(name string) error {
	if name == "" {
		return fmt.Errorf("asb subscription name must not be empty")
	}
	if len(name) > maxSubscriptionNameLength {
		return fmt.Errorf("asb subscription name %q is %d characters, exceeding the %d-character ASB limit", name, len(name), maxSubscriptionNameLength)
	}
	return nil
}

// receivedMessageToMessage converts a received ASB message into a
// connectors.Message, reconstructing the tombstone/key/header encoding applied
// by toServiceBusMessage.
func receivedMessageToMessage(rm *azservicebus.ReceivedMessage, topic string) *connectors.Message {
	msg := &connectors.Message{
		Topic: topic,
		Metadata: map[string]interface{}{
			"sequence_number": rm.SequenceNumber,
			"delivery_count":  rm.DeliveryCount,
			"enqueued_time":   rm.EnqueuedTime,
		},
	}

	if rm.ApplicationProperties == nil {
		msg.Value = rm.Body
		return msg
	}

	if tombstone, ok := rm.ApplicationProperties[applyPropertyTombstone].(bool); ok && tombstone {
		msg.Value = nil
	} else {
		msg.Value = rm.Body
	}

	if encodedKey, ok := rm.ApplicationProperties[applyPropertyKey].(string); ok {
		if key, err := base64.StdEncoding.DecodeString(encodedKey); err == nil {
			msg.Key = key
		}
	}

	if encodedHeaders, ok := rm.ApplicationProperties[applyPropertyHeaders].(string); ok {
		var headers map[string][]string
		if err := json.Unmarshal([]byte(encodedHeaders), &headers); err == nil {
			msg.Headers = headers
		}
	}

	return msg
}
