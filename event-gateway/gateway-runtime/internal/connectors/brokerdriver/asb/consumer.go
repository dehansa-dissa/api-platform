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

// messageReceiver is the subset of *azservicebus.Receiver the consume path
// uses. The SDK's type is concrete and its methods open AMQP links, which left
// handleMessage unreachable from a test — and handleMessage is where a message
// is settled, the difference between delivering an event once and twice. The
// settle bug that shipped in this file got through precisely because no test
// could call it.
type messageReceiver interface {
	ReceiveMessages(ctx context.Context, maxMessages int, options *azservicebus.ReceiveMessagesOptions) (
		[]*azservicebus.ReceivedMessage, error)
	CompleteMessage(ctx context.Context, message *azservicebus.ReceivedMessage,
		options *azservicebus.CompleteMessageOptions) error
	DeadLetterMessage(ctx context.Context, message *azservicebus.ReceivedMessage,
		options *azservicebus.DeadLetterOptions) error
	RenewMessageLock(ctx context.Context, message *azservicebus.ReceivedMessage,
		options *azservicebus.RenewMessageLockOptions) error
	Close(ctx context.Context) error
}

// Compile-time proof the SDK type still satisfies it. An SDK upgrade that
// changes a signature fails here rather than at the assignment in NewConsumer.
var _ messageReceiver = (*azservicebus.Receiver)(nil)

const maxSubscriptionNameLength = 50

const (
	receiveRetryBase = time.Second
	receiveRetryMax  = 30 * time.Second
)

const (
	messageLockRenewInterval = 20 * time.Second
	messageLockRenewTimeout  = 10 * time.Second
	maxMessageProcessingTime = 5 * time.Minute
)

// backoffDelay grows the retry interval to receiveRetryMax, with jitter so
// replicas do not retry in lockstep.
func backoffDelay(attempt int) time.Duration {
	delay := receiveRetryMax
	if attempt < 5 {
		delay = receiveRetryBase << attempt
	}
	return delay + rand.N(delay/2)
}

// sleepFor waits d, reporting false if ctx ended first. Every timed wait in this
// package routes through it, so cancellation behaves the same way everywhere and
// no waiter holds a timer past its own return.
func sleepFor(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// sleepBackoff waits out a failed receive, reporting false if ctx ended first.
func sleepBackoff(ctx context.Context, attempt int) bool {
	return sleepFor(ctx, backoffDelay(attempt))
}

// settleMode mirrors the two Kafka consumer variants. Handler failure never
// abandons: the Deliverer owns retry, and a crash still redelivers via lock expiry.
type settleMode int

const (
	settleAutoComplete settleMode = iota
	settleOnHandlerResult
)

// Consumer consumes events from a single ASB topic through one subscription.
type Consumer struct {
	receiver messageReceiver
	topic    string
	handler  connectors.MessageHandler
	mode     settleMode

	ephemeral bool

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

// reopen recreates a subscription that vanished underneath the receiver, whether
// reclaimed by AutoDeleteOnIdle or deleted by hand.
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
	c.handleMessageWithBudget(ctx, rm, maxMessageProcessingTime, messageLockRenewInterval)
}

// handleMessageWithBudget takes the handler budget and renewal cadence as
// parameters so a test can drive the lapsed-lock path in milliseconds instead
// of the five real minutes production allows, the same shape as runHandler.
func (c *Consumer) handleMessageWithBudget(ctx context.Context, rm *azservicebus.ReceivedMessage,
	budget, renewInterval time.Duration) {

	slog.Debug("[7] Message read from ASB",
		"topic", c.topic,
		"sequence_number", rm.SequenceNumber,
		"delivery_count", rm.DeliveryCount,
		"size_bytes", len(rm.Body))

	if rm.DeliveryCount > 1 {
		slog.Warn("ASB message redelivered after an unsettled attempt",
			"topic", c.topic,
			"subscription", c.groupID,
			"sequence_number", rm.SequenceNumber,
			"delivery_count", rm.DeliveryCount,
		)
	}

	msg := receivedMessageToMessage(rm, c.topic)

	handlerErr, lockHeld := c.runHandler(ctx, msg, budget, renewInterval,
		func(renewCtx context.Context) error {
			return c.receiver.RenewMessageLock(renewCtx, rm, nil)
		})

	if !lockHeld && ctx.Err() == nil {
		slog.Warn("Left ASB message unsettled: lock no longer held",
			"topic", c.topic,
			"subscription", c.groupID,
			"sequence_number", rm.SequenceNumber,
			"delivery_count", rm.DeliveryCount,
		)
		return
	}

	if c.mode == settleOnHandlerResult && handlerErr != nil {
		reason := "handler-failed"
		desc := handlerErr.Error()
		if err := c.receiver.DeadLetterMessage(ctx, rm, &azservicebus.DeadLetterOptions{
			Reason:           &reason,
			ErrorDescription: &desc,
		}); err != nil {
			c.reportSettleFailure(ctx, "Failed to dead-letter ASB message", err)
			return
		}

		slog.Warn("Dead-lettered ASB message: delivery failed permanently",
			"topic", c.topic,
			"subscription", c.groupID,
			"sequence_number", rm.SequenceNumber,
			"delivery_count", rm.DeliveryCount,
			"reason", reason,
			"error", handlerErr,
		)
		return
	}

	if handlerErr != nil {
		slog.Error("Message handler error",
			"topic", c.topic,
			"sequence_number", rm.SequenceNumber,
			"error", handlerErr,
		)
	}

	if err := c.receiver.CompleteMessage(ctx, rm, nil); err != nil {
		c.reportSettleFailure(ctx, "Failed to complete ASB message", err)
	}
}

// runHandler runs the handler with the message lock held open, reporting whether
// it finished while the lock was still ours.
func (c *Consumer) runHandler(ctx context.Context, msg *connectors.Message, budget, renewInterval time.Duration,
	renew func(context.Context) error) (handlerErr error, lockHeld bool) {

	// Derived once and never reassigned: the lockHeld read below depends on this
	// being the timeout's own context, and an earlier version got that wrong.
	handlerCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		c.renewLock(handlerCtx, cancel, renewInterval, renew)
	}()

	handlerErr = c.handler(handlerCtx, msg)
	lockHeld = handlerCtx.Err() == nil

	cancel()
	<-renewed
	return handlerErr, lockHeld
}

// renewLock keeps one message's lock alive until ctx ends.
func (c *Consumer) renewLock(ctx context.Context, cancel context.CancelFunc, interval time.Duration, renew func(context.Context) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, renewCancel := context.WithTimeout(ctx, messageLockRenewTimeout)
			err := renew(renewCtx)
			renewCancel()

			switch {
			case err == nil:
			case ctx.Err() != nil:
				return
			case isLockLostErr(err):
				slog.Warn("ASB message lock lost while handling; stopping delivery",
					"topic", c.topic, "subscription", c.groupID, "error", err)
				cancel()
				return
			default:
				slog.Debug("ASB message lock renewal failed, retrying",
					"topic", c.topic, "error", err)
			}
		}
	}
}

// reportSettleFailure logs a failed settle at the level it deserves.
func (c *Consumer) reportSettleFailure(ctx context.Context, msg string, err error) {
	if ctx.Err() != nil {
		slog.Debug("ASB message settle cancelled during teardown", "topic", c.topic, "error", err)
		return
	}
	slog.Error(msg, "topic", c.topic, "error", err)
}

// MultiTopicConsumer exposes one Consumer per topic as a single connectors.Receiver.
type MultiTopicConsumer struct {
	consumers []*Consumer
}

// Start begins consuming on every topic.
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

// NewConsumer auto-completes every message. Its subscriptions are ephemeral: Stop deletes them.
func NewConsumer(e *ASBBrokerDriver, groupID string, topics []string, handler connectors.MessageHandler) (*MultiTopicConsumer, error) {
	return newMultiTopicConsumer(e, groupID, topics, handler, settleAutoComplete, true)
}

// NewManualCommitConsumer completes on success and dead-letters on failure.
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

// Watch tails a topic until ctx is cancelled. consumerID names the subscription,
// so the read position survives a restart.
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

// ensureSubscription idempotently creates a durable subscription; 409 is success.
// Durable because a quiet subscriber must not lose its backlog.
func (e *ASBBrokerDriver) ensureSubscription(ctx context.Context, topic, subscriptionName string) error {
	return e.createSubscription(ctx, topic, subscriptionName, e.subscriptionProps(false))
}

// ensureDisposableSubscription creates a subscription Azure reclaims once idle.
func (e *ASBBrokerDriver) ensureDisposableSubscription(ctx context.Context, topic, subscriptionName string) error {
	return e.createSubscription(ctx, topic, subscriptionName, e.subscriptionProps(true))
}

// subscriptionProps builds the properties this driver asserts, or nil when none
// are configured. nil keeps a conflicting create free of a read-back.
func (e *ASBBrokerDriver) subscriptionProps(disposable bool) *admin.SubscriptionProperties {
	var props *admin.SubscriptionProperties
	if disposable && e.cfg.SubscriptionIdleTimeout > 0 {
		props = &admin.SubscriptionProperties{
			AutoDeleteOnIdle: to.Ptr(iso8601Seconds(e.cfg.SubscriptionIdleTimeout)),
		}
	}
	if e.cfg.MaxDeliveryCount > 0 {
		if props == nil {
			props = &admin.SubscriptionProperties{}
		}
		props.MaxDeliveryCount = to.Ptr(e.cfg.MaxDeliveryCount)
	}
	return props
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
	if err := e.reconcileSubscription(ctx, topic, subscriptionName, props); err != nil {
		slog.Warn("Failed to update asb subscription properties",
			"topic", topic, "subscription", subscriptionName, "error", err)
	}
	return nil
}

// reconcileSubscription applies changed properties to an existing subscription,
// which a 409 create leaves untouched.
func (e *ASBBrokerDriver) reconcileSubscription(ctx context.Context, topic, subscriptionName string, desired *admin.SubscriptionProperties) error {
	if desired.AutoDeleteOnIdle == nil && desired.MaxDeliveryCount == nil {
		return nil
	}
	existing, err := e.admin.GetSubscription(ctx, topic, subscriptionName, nil)
	if err != nil {
		return err
	}

	if existing == nil {
		return nil
	}

	// Update replaces the whole entity, so start from what Get returned and swap
	// only what changed; desired alone would reset the rest to defaults.
	updated := existing.SubscriptionProperties
	changed := false
	if desired.AutoDeleteOnIdle != nil &&
		(existing.AutoDeleteOnIdle == nil || !sameIdleTimeout(*existing.AutoDeleteOnIdle, *desired.AutoDeleteOnIdle)) {
		updated.AutoDeleteOnIdle = desired.AutoDeleteOnIdle
		changed = true
	}
	if desired.MaxDeliveryCount != nil &&
		(existing.MaxDeliveryCount == nil || *existing.MaxDeliveryCount != *desired.MaxDeliveryCount) {
		updated.MaxDeliveryCount = desired.MaxDeliveryCount
		changed = true
	}
	if !changed {
		return nil
	}

	if _, err := e.admin.UpdateSubscription(ctx, topic, subscriptionName, updated, nil); err != nil {
		return err
	}
	slog.Info("Updated asb subscription properties",
		"topic", topic, "subscription", subscriptionName)
	return nil
}

// DeleteSubscription removes a subscriber's durable Subscription.
func (e *ASBBrokerDriver) DeleteSubscription(ctx context.Context, groupID string, topic string) error {
	if _, err := e.admin.DeleteSubscription(ctx, topic, groupID, nil); err != nil {
		if isNotFoundErr(err) {
			return nil
		}
		return fmt.Errorf("failed to delete asb subscription %s on topic %s: %w", groupID, topic, err)
	}
	return nil
}

// subscriptionNameFor maps a groupID onto a valid ASB subscription name. Short
// ones pass through; longer hash deterministically, so a restart finds them again.
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
