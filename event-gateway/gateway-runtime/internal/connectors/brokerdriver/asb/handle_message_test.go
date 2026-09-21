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
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/connectors"
)

type fakeReceiver struct {
	mu sync.Mutex

	completed    int
	deadLettered int
	renewals     int
	closed       int

	completeErr   error
	deadLetterErr error
	renewErr      error

	deadLetterReason string
}

func (f *fakeReceiver) ReceiveMessages(context.Context, int, *azservicebus.ReceiveMessagesOptions) (
	[]*azservicebus.ReceivedMessage, error) {
	return nil, errors.New("not used by these tests")
}

func (f *fakeReceiver) CompleteMessage(context.Context, *azservicebus.ReceivedMessage,
	*azservicebus.CompleteMessageOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.completeErr != nil {
		return f.completeErr
	}
	f.completed++
	return nil
}

func (f *fakeReceiver) DeadLetterMessage(_ context.Context, _ *azservicebus.ReceivedMessage,
	options *azservicebus.DeadLetterOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deadLetterErr != nil {
		return f.deadLetterErr
	}
	f.deadLettered++
	if options != nil && options.Reason != nil {
		f.deadLetterReason = *options.Reason
	}
	return nil
}

func (f *fakeReceiver) RenewMessageLock(context.Context, *azservicebus.ReceivedMessage,
	*azservicebus.RenewMessageLockOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewals++
	return f.renewErr
}

func (f *fakeReceiver) Close(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return nil
}

func (f *fakeReceiver) counts() (completed, deadLettered int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.completed, f.deadLettered
}

func receivedMessage(deliveryCount uint32) *azservicebus.ReceivedMessage {
	sequence := int64(1)
	return &azservicebus.ReceivedMessage{
		Body:           []byte(`{"id":1}`),
		DeliveryCount:  deliveryCount,
		SequenceNumber: &sequence,
	}
}

func TestHandleMessage_CompletesAHealthyMessage(t *testing.T) {
	receiver := &fakeReceiver{}
	c := &Consumer{
		topic:    "cache-settle01",
		groupID:  "grp-settle",
		receiver: receiver,
		mode:     settleAutoComplete,
		handler:  func(context.Context, *connectors.Message) error { return nil },
	}

	c.handleMessage(context.Background(), receivedMessage(1))

	completed, deadLettered := receiver.counts()
	if completed != 1 {
		t.Fatalf("message was completed %d times, want 1 — an unsettled message is redelivered", completed)
	}
	if deadLettered != 0 {
		t.Errorf("a healthy message was dead-lettered %d times, want 0", deadLettered)
	}
}

func TestHandleMessage_DeadLettersAFailedHandler(t *testing.T) {
	receiver := &fakeReceiver{}
	c := &Consumer{
		topic:    "cache-settle02",
		groupID:  "grp-settle",
		receiver: receiver,
		mode:     settleOnHandlerResult,
		handler:  func(context.Context, *connectors.Message) error { return errors.New("subscriber refused") },
	}

	c.handleMessage(context.Background(), receivedMessage(1))

	completed, deadLettered := receiver.counts()
	if deadLettered != 1 {
		t.Fatalf("message was dead-lettered %d times, want 1", deadLettered)
	}
	if completed != 0 {
		t.Errorf("a failed message was also completed %d times, want 0", completed)
	}
	if receiver.deadLetterReason == "" {
		t.Error("dead-letter carried no reason; an operator cannot tell why it is in the queue")
	}
}

func TestHandleMessage_AutoCompleteSettlesDespiteAHandlerError(t *testing.T) {
	receiver := &fakeReceiver{}
	c := &Consumer{
		topic:    "cache-settle03",
		groupID:  "grp-settle",
		receiver: receiver,
		mode:     settleAutoComplete,
		handler:  func(context.Context, *connectors.Message) error { return errors.New("delivery failed") },
	}

	c.handleMessage(context.Background(), receivedMessage(1))

	completed, deadLettered := receiver.counts()
	if completed != 1 {
		t.Errorf("auto-complete mode completed %d messages, want 1", completed)
	}
	if deadLettered != 0 {
		t.Errorf("auto-complete mode dead-lettered %d messages, want 0", deadLettered)
	}
}

// A redelivery is an ordinary message: Azure raises DeliveryCount, and nothing
// about settlement changes. Only the WARN it emits is different.
func TestHandleMessage_RedeliveryIsSettledLikeAnyOtherMessage(t *testing.T) {
	receiver := &fakeReceiver{}
	c := &Consumer{
		topic:    "cache-settle04",
		groupID:  "grp-settle",
		receiver: receiver,
		mode:     settleAutoComplete,
		handler:  func(context.Context, *connectors.Message) error { return nil },
	}

	c.handleMessage(context.Background(), receivedMessage(1))
	c.handleMessage(context.Background(), receivedMessage(3))

	completed, deadLettered := receiver.counts()
	if completed != 2 {
		t.Errorf("completed %d messages, want 2 — a redelivery must settle like any other", completed)
	}
	if deadLettered != 0 {
		t.Errorf("a redelivery was dead-lettered %d times, want 0", deadLettered)
	}
}

// A settle that fails must not fall through to a second settle attempt. Azure
// redelivers the message on its own once the lock lapses.
func TestHandleMessage_FailedCompleteIsNotFollowedByADeadLetter(t *testing.T) {
	receiver := &fakeReceiver{completeErr: errors.New("link detached")}
	c := &Consumer{
		topic:    "cache-settle05",
		groupID:  "grp-settle",
		receiver: receiver,
		mode:     settleAutoComplete,
		handler:  func(context.Context, *connectors.Message) error { return nil },
	}

	c.handleMessage(context.Background(), receivedMessage(1))

	completed, deadLettered := receiver.counts()
	if completed != 0 {
		t.Errorf("a failed complete was recorded as completed %d times, want 0", completed)
	}
	if deadLettered != 0 {
		t.Errorf("a failed complete was escalated to a dead-letter %d times, want 0", deadLettered)
	}
}

func TestHandleMessage_LapsedLockLeavesTheMessageUnsettled(t *testing.T) {
	receiver := &fakeReceiver{}
	c := &Consumer{
		topic:    "cache-settle06",
		groupID:  "grp-settle",
		receiver: receiver,
		mode:     settleAutoComplete,
		handler: func(ctx context.Context, _ *connectors.Message) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}

	c.handleMessageWithBudget(context.Background(), receivedMessage(1), 20*time.Millisecond, time.Hour)

	completed, deadLettered := receiver.counts()
	if completed != 0 || deadLettered != 0 {
		t.Fatalf("a message whose lock lapsed was settled anyway (completed=%d dead-lettered=%d)",
			completed, deadLettered)
	}
}

type fakeSender struct {
	mu   sync.Mutex
	sent []*azservicebus.Message
	err  error
}

func (f *fakeSender) SendMessage(_ context.Context, m *azservicebus.Message, _ *azservicebus.SendMessageOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, m)
	return nil
}

func (f *fakeSender) Close(context.Context) error { return nil }

func testPublisher(sender messageSender) *Publisher {
	return &Publisher{
		newSender: func(string) (messageSender, error) { return sender, nil },
		senders:   make(map[string]messageSender),
	}
}

func TestPublish_SendsTheEncodedMessage(t *testing.T) {
	sender := &fakeSender{}
	body := []byte(`{"id":1}`)

	if err := testPublisher(sender).Publish(context.Background(), "cache-pub01",
		&connectors.Message{Value: body}); err != nil {
		t.Fatalf("Publish returned %v, want nil", err)
	}

	if len(sender.sent) != 1 {
		t.Fatalf("sender received %d messages, want 1", len(sender.sent))
	}
	if !bytes.Equal(sender.sent[0].Body, body) {
		t.Errorf("sent body is %q, want %q", sender.sent[0].Body, body)
	}
}

func TestPublish_ReturnsAnErrorOnAFailedSend(t *testing.T) {
	sender := &fakeSender{err: errors.New("entity not found")}

	if err := testPublisher(sender).Publish(context.Background(), "cache-pub02",
		&connectors.Message{Value: []byte(`{"id":1}`)}); err == nil {
		t.Fatal("Publish returned nil on a failed send, want an error")
	}

	if len(sender.sent) != 0 {
		t.Errorf("a failed send recorded %d messages, want 0", len(sender.sent))
	}
}
