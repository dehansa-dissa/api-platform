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

// Application-property keys used to round-trip connectors.Message fields that
// don't map onto native ASB message fields. ASB has no concept of a Kafka-style
// tombstone (a record with a nil value used to signal deletion in a compacted
// topic), so it is encoded explicitly via applyPropertyTombstone.
const (
	applyPropertyTombstone = "x-eventgateway-tombstone"
	applyPropertyKey       = "x-eventgateway-key"
	applyPropertyHeaders   = "x-eventgateway-headers"
)

// Publisher publishes events to Azure Service Bus topics.
//
// Unlike Kafka, where one client addresses every topic, an ASB sender is bound
// to a single entity — so the Publisher caches one sender per topic (or queue,
// for the replayer's snapshot and leader-lock entities) and reuses it.
type Publisher struct {
	client *azservicebus.Client

	mu      sync.Mutex
	senders map[string]*azservicebus.Sender
}

// NewPublisher creates a new ASB publisher over the shared client.
func NewPublisher(client *azservicebus.Client) *Publisher {
	return &Publisher{
		client:  client,
		senders: make(map[string]*azservicebus.Sender),
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

// senderFor returns a cached sender for the given topic or queue, creating one
// on first use.
func (p *Publisher) senderFor(entity string) (*azservicebus.Sender, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if sender, ok := p.senders[entity]; ok {
		return sender, nil
	}

	sender, err := p.client.NewSender(entity, nil)
	if err != nil {
		return nil, err
	}
	p.senders[entity] = sender
	return sender, nil
}

// closeSender closes and forgets the cached sender for one entity, so a
// deleted topic/queue doesn't leave a live AMQP link behind.
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
	p.senders = make(map[string]*azservicebus.Sender)
}

// toServiceBusMessage converts a connectors.Message into an ASB send message.
// msg.Value == nil is encoded as an explicit tombstone marker (see
// applyPropertyTombstone) rather than as an empty body, so a genuinely empty
// (but present) value is never confused with a deletion on the receive side.
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
		// MessageID drives ASB's own duplicate-detection feature (when enabled
		// on the entity) and gives operators a stable, human-inspectable
		// identifier in the Azure portal/CLI for the record's compaction key.
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
