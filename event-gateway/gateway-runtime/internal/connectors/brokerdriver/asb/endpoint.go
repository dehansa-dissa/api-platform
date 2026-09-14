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

// Package asb implements connectors.BrokerDriver for Azure Service Bus (ASB).
package asb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/Azure/go-amqp"
	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/connectors"
)

// cacheSubscriptionReleaseTimeout bounds shutdown cleanup so an unreachable
// namespace cannot stall process exit.
const cacheSubscriptionReleaseTimeout = 10 * time.Second

// ASBBrokerDriver implements connectors.BrokerDriver for Azure Service Bus.
// It owns a shared publisher and admin client and tracks per-compacted-topic
// leader-election state (see replayer.go).
type ASBBrokerDriver struct {
	publisher  *Publisher
	cfg        ConnectionConfig
	client     *azservicebus.Client
	admin      *admin.Client
	instanceID string // stable-per-process identity for this driver's internal cache subscriptions (see replayer.go)

	compactedMu sync.Mutex
	compacted   map[string]*compactedTopicState // topic -> leader-election state

	closeOnce sync.Once
}

// NewClient creates an Azure Service Bus client using the shared connection config.
func NewClient(cfg ConnectionConfig) (*azservicebus.Client, error) {
	return azservicebus.NewClientFromConnectionString(cfg.ConnectionString, nil)
}

// NewBrokerDriver creates an ASB broker-driver from the given connection config.
// runtimeID names this process's internal subscriptions (see replayer.go); it is
// a fresh UUID per boot unless runtime_id is configured, so those subscriptions
// carry AutoDeleteOnIdle to stop each restart stranding one per topic.
func NewBrokerDriver(cfg ConnectionConfig, runtimeID string) (*ASBBrokerDriver, error) {
	client, err := NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create asb client: %w", err)
	}

	adminClient, err := admin.NewClientFromConnectionString(cfg.ConnectionString, nil)
	if err != nil {
		_ = client.Close(context.Background())
		return nil, fmt.Errorf("failed to create asb admin client: %w", err)
	}

	return &ASBBrokerDriver{
		publisher:  NewPublisher(client),
		cfg:        cfg,
		client:     client,
		admin:      adminClient,
		instanceID: runtimeID,
		compacted:  make(map[string]*compactedTopicState),
	}, nil
}

// Publish sends a message to the given ASB topic.
func (e *ASBBrokerDriver) Publish(ctx context.Context, topic string, msg *connectors.Message) error {
	return e.publisher.Publish(ctx, topic, msg)
}

// TopicExists checks whether a topic exists in the ASB namespace.
func (e *ASBBrokerDriver) TopicExists(ctx context.Context, topic string) (bool, error) {
	resp, err := e.admin.GetTopic(ctx, topic, nil)
	if err != nil {
		if isNotFoundErr(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check topic %s: %w", topic, err)
	}
	return resp != nil, nil
}

// EnsureTopics creates topics if they don't already exist (idempotent).
// metadata is logged for tracking but not stored — ASB topic properties have
// no place for arbitrary key-value pairs.
func (e *ASBBrokerDriver) EnsureTopics(ctx context.Context, topics []string, metadata map[string]map[string]string) error {
	for _, topic := range topics {
		if _, err := e.admin.CreateTopic(ctx, topic, nil); err != nil {
			// A conflict is not a real failure for idempotent creates.
			if !isConflictErr(err) {
				return fmt.Errorf("failed to create topic %s: %w", topic, err)
			}
			slog.Debug("Topic already exists", "topic", topic)
		} else {
			slog.Info("Created topic", "topic", topic)
		}

		// Log metadata for debugging/tracking purposes
		if metadata != nil && metadata[topic] != nil {
			slog.Info("Topic metadata",
				"topic_hash", topic,
				"apiName", metadata[topic]["apiName"],
				"apiVersion", metadata[topic]["apiVersion"],
				"channelName", metadata[topic]["channelName"])
		}
	}

	return nil
}

// Close shuts down the publisher, client, and leader-election goroutines.
func (e *ASBBrokerDriver) Close() error {
	var closeErr error
	e.closeOnce.Do(func() {
		e.compactedMu.Lock()
		topics := make([]string, 0, len(e.compacted))
		for topic := range e.compacted {
			topics = append(topics, topic)
			e.stopCompactedTopicLocked(topic)
		}
		e.compactedMu.Unlock()

		e.releaseCacheSubscriptions(topics)
		e.publisher.Close()

		if err := e.client.Close(context.Background()); err != nil {
			closeErr = fmt.Errorf("failed to close asb client: %w", err)
		}
	})
	return closeErr
}

// releaseCacheSubscriptions drops this instance's cache subscriptions on a
// graceful shutdown, returning quota immediately rather than waiting out
// AutoDeleteOnIdle. Best-effort: a failure is a delayed reclaim, not an error.
func (e *ASBBrokerDriver) releaseCacheSubscriptions(topics []string) {
	if len(topics) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), cacheSubscriptionReleaseTimeout)
	defer cancel()

	subscriptionName := cacheSubscriptionName(e.instanceID)
	for _, topic := range topics {
		if _, err := e.admin.DeleteSubscription(ctx, topic, subscriptionName, nil); err != nil && !isNotFoundErr(err) {
			slog.Warn("Failed to release cache subscription on shutdown",
				"topic", topic, "subscription", subscriptionName, "error", err)
		}
	}
}

// DeleteTopics deletes the given topics plus any compaction side-queues.
// Best-effort like the Kafka driver: per-entity failures are logged, not
// returned, so one already-gone entity does not block a teardown.
func (e *ASBBrokerDriver) DeleteTopics(ctx context.Context, topics []string) error {
	for _, topic := range topics {
		e.publisher.closeSender(topic)
		e.stopCompactedTopic(topic)
		e.deleteCompactionQueues(ctx, topic)

		if _, err := e.admin.DeleteTopic(ctx, topic, nil); err != nil {
			slog.Warn("Failed to delete topic", "topic", topic, "error", err)
		} else {
			slog.Info("Deleted topic", "topic", topic)
		}
	}
	return nil
}

// deleteCompactionQueues removes EnsureCompactedTopic's side-entities, without
// which a redeployed API would resurrect subscriptions from a stale snapshot.
// A plain topic has no such queues, so 404 is expected.
func (e *ASBBrokerDriver) deleteCompactionQueues(ctx context.Context, topic string) {
	for _, queue := range []string{snapshotQueueName(topic), leaderLockQueueName(topic)} {
		e.publisher.closeSender(queue)
		if _, err := e.admin.DeleteQueue(ctx, queue, nil); err != nil {
			if !isNotFoundErr(err) {
				slog.Warn("Failed to delete compaction queue", "queue", queue, "error", err)
			}
			continue
		}
		slog.Info("Deleted compaction queue", "queue", queue)
	}
}

// isLockLostErr reports that a message or session lock has already expired.
// Distinct from a transport failure: the lock is provably gone rather than
// merely unconfirmed, so a caller must concede instead of retrying.
func isLockLostErr(err error) bool {
	var sbErr *azservicebus.Error
	return errors.As(err, &sbErr) && sbErr.Code == azservicebus.CodeLockLost
}

// isConflictErr reports an ASB "entity already exists" (HTTP 409) response.
func isConflictErr(err error) bool {
	var respErr *azcore.ResponseError
	return errors.As(err, &respErr) && respErr.StatusCode == http.StatusConflict
}

// isNotFoundErr reports "entity does not exist" in all three shapes the SDK
// delivers it in. The management API answers HTTP 404, and an RPC-path failure
// is wrapped as CodeNotFound — but a receiver link detaching because its
// subscription was deleted is neither: TransformError classifies amqp:not-found
// as RecoveryKindFatal and returns the raw *amqp.Error unwrapped. The receive
// loops self-heal off this function, so without the third shape they back off
// forever against a subscription that is never coming back.
func isNotFoundErr(err error) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
		return true
	}
	var sbErr *azservicebus.Error
	if errors.As(err, &sbErr) && sbErr.Code == azservicebus.CodeNotFound {
		return true
	}
	var amqpErr *amqp.Error
	return errors.As(err, &amqpErr) && amqpErr.Condition == amqp.ErrCondNotFound
}

// isSBTimeoutErr reports CodeTimeout, returned both when no session is
// available and when one is already locked. Both are expected during election.
func isSBTimeoutErr(err error) bool {
	var sbErr *azservicebus.Error
	return errors.As(err, &sbErr) && sbErr.Code == azservicebus.CodeTimeout
}
