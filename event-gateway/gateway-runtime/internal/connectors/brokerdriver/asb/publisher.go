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

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/connectors"
)

// messageSender is the subset of *azservicebus.Sender the publish path uses,
// for the same reason as messageReceiver in consumer.go: Publish encodes the
// message, and what a fake records is the only way to assert what actually
// reached the wire without a namespace.
type messageSender interface {
	SendMessage(ctx context.Context, message *azservicebus.Message, options *azservicebus.SendMessageOptions) error
	Close(ctx context.Context) error
}

var _ messageSender = (*azservicebus.Sender)(nil)

// Application-property keys for connectors.Message fields with no native ASB
// equivalent. ASB has no Kafka-style tombstone, so it is encoded explicitly.
const (
	applyPropertyTombstone = "x-eventgateway-tombstone"
	applyPropertyKey       = "x-eventgateway-key"
	applyPropertyHeaders   = "x-eventgateway-headers"
)

// Publisher publishes events to Azure Service Bus topics. An ASB sender is
// bound to one entity, so senders are cached per topic or queue and reused.
type Publisher struct {
	newSender func(entity string) (messageSender, error)

	mu      sync.Mutex
	senders map[string]messageSender
}

// NewPublisher creates a new ASB publisher over the shared client.
func NewPublisher(client *azservicebus.Client) *Publisher {
	return &Publisher{
		newSender: func(entity string) (messageSender, error) {
			return client.NewSender(entity, nil)
		},
		senders: make(map[string]messageSender),
	}
}

// Publish sends a message to the specified ASB topic.
func (p *Publisher) Publish(ctx context.Context, topic string, msg *connectors.Message) error {
	sender, err := p.senderFor(topic)
	if err != nil {
		return fmt.Errorf("failed to open asb sender for topic %s: %w", topic, err)
	}

	sbMsg, err := toServiceBusMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to encode message for topic %s: %w", topic, err)
	}

	if err := sender.SendMessage(ctx, sbMsg, nil); err != nil {
		return fmt.Errorf("failed to publish to topic %s: %w", topic, err)
	}
	return nil
}

// senderFor returns a cached sender, creating one on first use.
func (p *Publisher) senderFor(entity string) (messageSender, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if sender, ok := p.senders[entity]; ok {
		return sender, nil
	}

	sender, err := p.newSender(entity)
	if err != nil {
		return nil, err
	}
	p.senders[entity] = sender
	return sender, nil
}

// closeSender drops the cached sender so a deleted entity leaves no live link.
func (p *Publisher) closeSender(entity string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if sender, ok := p.senders[entity]; ok {
		if err := sender.Close(context.Background()); err != nil {
			slog.Warn("Failed to close asb sender", "entity", entity, "error", err)
		}
		delete(p.senders, entity)
	}
}

// Close closes every cached sender.
func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for entity, sender := range p.senders {
		if err := sender.Close(context.Background()); err != nil {
			slog.Warn("Failed to close asb sender", "entity", entity, "error", err)
		}
	}
	p.senders = make(map[string]messageSender)
}

// toServiceBusMessage encodes a connectors.Message for ASB.
func toServiceBusMessage(msg *connectors.Message) (*azservicebus.Message, error) {
	sbMsg := &azservicebus.Message{
		ApplicationProperties: map[string]any{},
	}

	if msg.Value == nil {
		sbMsg.ApplicationProperties[applyPropertyTombstone] = true
	} else {
		sbMsg.Body = msg.Value
	}

	if len(msg.Key) > 0 {
		sbMsg.ApplicationProperties[applyPropertyKey] = base64.StdEncoding.EncodeToString(msg.Key)
		keyStr := string(msg.Key)
		sbMsg.MessageID = &keyStr
	}

	if len(msg.Headers) > 0 {
		headerBytes, err := json.Marshal(msg.Headers)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal message headers: %w", err)
		}
		sbMsg.ApplicationProperties[applyPropertyHeaders] = string(headerBytes)
	}

	return sbMsg, nil
}
