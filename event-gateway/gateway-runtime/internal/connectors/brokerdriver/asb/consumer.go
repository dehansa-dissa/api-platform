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
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/connectors"
)

// Azure Service Bus caps a subscription name at 50 characters.
const maxSubscriptionNameLength = 50

const (
	receiveRetryBase = time.Second
	receiveRetryMax  = 30 * time.Second
)

// backoffDelay grows the retry interval exponentially to receiveRetryMax and
// adds jitter, so replicas that lost the namespace together do not retry in
// lockstep and stampede it on recovery.
func backoffDelay(attempt int) time.Duration {
	delay := receiveRetryMax
	if attempt < 5 {
		delay = receiveRetryBase << attempt
	}
	return delay + rand.N(delay/2)
}

// sleepBackoff waits out a failed receive, reporting false if ctx ended first.
func sleepBackoff(ctx context.Context, attempt int) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(backoffDelay(attempt)):
		return true
	}
}

// settleMode mirrors the two Kafka consumer variants. Handler failure never
// abandons the message — the Deliverer owns retry, and abandoning would compound
// two retry systems. A crash still redelivers via lock expiry.
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

	// ephemeral: Stop deletes the subscription. Set only by NewConsumer, whose
	// WebSocket callers mint a groupID per connection.
	ephemeral bool
	// disposable: the subscription carries AutoDeleteOnIdle, so reopen must
	// recreate it the same way rather than as a durable one.
	disposable bool
	driver     *ASBBrokerDriver
	groupID    string

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

// Stop halts the consume loop and closes the receiver. An ephemeral consumer
// also deletes its subscription, since nothing else will.
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
	attempt := 0
	for {
		if ctx.Err() != nil {
			return
		}

		messages, err := c.receiver.ReceiveMessages(ctx, 1, nil)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isNotFoundErr(err) && c.reopen(ctx) == nil {
				slog.Info("Recreated reclaimed asb subscription",
					"topic", c.topic, "subscription", c.groupID)
				attempt = 0
				continue
			}
			slog.Error("ASB consumer receive error", "topic", c.topic, "error", err)
			if !sleepBackoff(ctx, attempt) {
				return
			}
			attempt++
			continue
		}
		attempt = 0

		for _, rm := range messages {
			c.handleMessage(ctx, rm)
		}
	}
}

// reopen recreates a subscription that vanished underneath the receiver and
// rebuilds the receiver on it. Reachable via AutoDeleteOnIdle reclaiming a
// disposable subscription, or an operator deleting any of them by hand.
func (c *Consumer) reopen(ctx context.Context) error {
	if c.driver == nil || c.groupID == "" {
		return fmt.Errorf("consumer is not bound to a driver")
	}

	ensure := c.driver.ensureSubscription
	if c.disposable {
		ensure = c.driver.ensureDisposableSubscription
	}
	if err := ensure(ctx, c.topic, c.groupID); err != nil {
		return err
	}

	receiver, err := c.driver.client.NewReceiverForSubscription(c.topic, c.groupID, &azservicebus.ReceiverOptions{
		ReceiveMode: azservicebus.ReceiveModePeekLock,
	})
	if err != nil {
		return err
	}

	_ = c.receiver.Close(ctx)
	c.receiver = receiver
	return nil
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

// MultiTopicConsumer exposes one Consumer per topic as a single connectors.Receiver.
type MultiTopicConsumer struct {
	consumers []*Consumer
}

// Start begins consuming on every topic. On failure the whole set is stopped:
// Stop is safe on an unstarted consumer, and the rest would strand receivers.
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

// NewConsumer creates a consumer that auto-completes every message, mirroring
// the Kafka driver's auto-commit Consumer. Its subscriptions are ephemeral:
// Stop deletes them. Callers needing durability use NewManualCommitConsumer.
func NewConsumer(e *ASBBrokerDriver, groupID string, topics []string, handler connectors.MessageHandler) (*MultiTopicConsumer, error) {
	return newMultiTopicConsumer(e, groupID, topics, handler, settleAutoComplete, true)
}

// NewManualCommitConsumer completes on handler success and dead-letters on
// failure — the ASB analogue of Kafka's manual offset commit. The subscription
// is durable and survives Stop.
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

	// Ephemeral subscriptions are deleted too: nothing else knows about them.
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

	ensure := e.ensureSubscription
	if ephemeral {
		ensure = e.ensureDisposableSubscription
	}

	for _, topic := range topics {
		if err := ensure(ctx, topic, subscriptionName); err != nil {
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
			receiver:   receiver,
			topic:      topic,
			handler:    handler,
			mode:       mode,
			ephemeral:  ephemeral,
			disposable: ephemeral,
			driver:     e,
			groupID:    subscriptionName,
		})
	}

	return &MultiTopicConsumer{consumers: consumers}, nil
}

// Subscribe creates a consumer for the given topics. The returned Receiver
// must be Start()ed by the caller.
func (e *ASBBrokerDriver) Subscribe(groupID string, topics []string, handler connectors.MessageHandler) (connectors.Receiver, error) {
	return NewConsumer(e, groupID, topics, handler)
}

// SubscribeManual creates a consumer that settles on handler outcome.
func (e *ASBBrokerDriver) SubscribeManual(groupID string, topics []string, handler connectors.MessageHandler) (connectors.Receiver, error) {
	return NewManualCommitConsumer(e, groupID, topics, handler)
}

// Watch tails a topic from the current position until ctx is cancelled.
// consumerID gives a stable subscription name so the read position survives
// restarts, mirroring Kafka's consumer-group-per-consumerID approach.
func (e *ASBBrokerDriver) Watch(ctx context.Context, consumerID string, topic string, handler connectors.MessageHandler) (connectors.Receiver, error) {
	subscriptionName := syncSubscriptionName(consumerID)

	if err := e.ensureDisposableSubscription(ctx, topic, subscriptionName); err != nil {
		return nil, fmt.Errorf("failed to ensure asb watch subscription %s on topic %s: %w", subscriptionName, topic, err)
	}

	receiver, err := e.client.NewReceiverForSubscription(topic, subscriptionName, &azservicebus.ReceiverOptions{
		ReceiveMode: azservicebus.ReceiveModePeekLock,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create asb watch receiver for topic %s: %w", topic, err)
	}

	return &Consumer{
		receiver:   receiver,
		topic:      topic,
		handler:    handler,
		mode:       settleAutoComplete,
		disposable: true,
		driver:     e,
		groupID:    subscriptionName,
	}, nil
}

// ensureSubscription idempotently creates a durable ASB subscription; 409 is
// success. Durable subscriptions outlive idle periods and are removed only by
// DeleteSubscription, because a quiet subscriber must not lose its backlog.
func (e *ASBBrokerDriver) ensureSubscription(ctx context.Context, topic, subscriptionName string) error {
	return e.createSubscription(ctx, topic, subscriptionName, nil)
}

// ensureDisposableSubscription creates a subscription Azure reclaims once idle.
func (e *ASBBrokerDriver) ensureDisposableSubscription(ctx context.Context, topic, subscriptionName string) error {
	if e.cfg.SubscriptionIdleTimeout <= 0 {
		return e.createSubscription(ctx, topic, subscriptionName, nil)
	}
	return e.createSubscription(ctx, topic, subscriptionName, &admin.SubscriptionProperties{
		AutoDeleteOnIdle: to.Ptr(iso8601Seconds(e.cfg.SubscriptionIdleTimeout)),
	})
}

func (e *ASBBrokerDriver) createSubscription(ctx context.Context, topic, subscriptionName string, props *admin.SubscriptionProperties) error {
	var opts *admin.CreateSubscriptionOptions
	if props != nil {
		opts = &admin.CreateSubscriptionOptions{Properties: props}
	}
	_, err := e.admin.CreateSubscription(ctx, topic, subscriptionName, opts)
	if err == nil {
		return nil
	}
	if !isConflictErr(err) {
		return err
	}
	if props == nil {
		return nil
	}
	if err := e.reconcileIdleTimeout(ctx, topic, subscriptionName, props); err != nil {
		// Not fatal: the subscription exists and is usable, only its reclaim
		// window is stale, and an early reclaim is already self-healed on 404.
		slog.Warn("Failed to update asb subscription idle timeout",
			"topic", topic, "subscription", subscriptionName, "error", err)
	}
	return nil
}

// reconcileIdleTimeout applies a changed AutoDeleteOnIdle to a subscription that
// already exists. Create answers 409 without touching the entity, so without
// this a subscription keeps the timeout it was born with and a raised
// subscription_idle_timeout_seconds never reaches the subscriptions already out
// there — only ones created afterwards.
func (e *ASBBrokerDriver) reconcileIdleTimeout(ctx context.Context, topic, subscriptionName string, desired *admin.SubscriptionProperties) error {
	if desired.AutoDeleteOnIdle == nil {
		return nil
	}
	existing, err := e.admin.GetSubscription(ctx, topic, subscriptionName, nil)
	if err != nil {
		return err
	}

	if existing == nil {
		return nil
	}
	if existing.AutoDeleteOnIdle != nil && *existing.AutoDeleteOnIdle == *desired.AutoDeleteOnIdle {
		return nil
	}

	// Update replaces the whole entity, so send back what Get returned with only
	// the idle timeout swapped — passing desired alone would reset every other
	// property to an SDK default.
	updated := existing.SubscriptionProperties
	updated.AutoDeleteOnIdle = desired.AutoDeleteOnIdle
	if _, err := e.admin.UpdateSubscription(ctx, topic, subscriptionName, updated, nil); err != nil {
		return err
	}
	slog.Info("Updated asb subscription idle timeout",
		"topic", topic, "subscription", subscriptionName, "idle_timeout", *desired.AutoDeleteOnIdle)
	return nil
}

// DeleteSubscription removes a subscriber's durable ASB Subscription. Kept off
// connectors.BrokerDriver — Kafka consumer groups need no cleanup — and found by
// ConsumerManager through an optional-capability type assertion.
func (e *ASBBrokerDriver) DeleteSubscription(ctx context.Context, groupID string, topic string) error {
	if _, err := e.admin.DeleteSubscription(ctx, topic, groupID, nil); err != nil {
		if isNotFoundErr(err) {
			return nil
		}
		return fmt.Errorf("failed to delete asb subscription %s on topic %s: %w", groupID, topic, err)
	}
	return nil
}

// subscriptionNameFor maps a groupID onto a valid ASB subscription name. Group
// IDs are sized for Kafka's unlimited names; the WebSocket paths compose one at
// 53 characters, over ASB's 50. Short names pass through, longer ones hash —
// deterministically, so a durable subscriber finds its subscription again.
func subscriptionNameFor(groupID string) (string, error) {
	if groupID == "" {
		return "", fmt.Errorf("asb subscription name must not be empty")
	}
	if len(groupID) <= maxSubscriptionNameLength {
		return groupID, nil
	}
	return "grp-" + shortHash(groupID), nil
}

// receivedMessageToMessage reverses toServiceBusMessage's encoding.
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
