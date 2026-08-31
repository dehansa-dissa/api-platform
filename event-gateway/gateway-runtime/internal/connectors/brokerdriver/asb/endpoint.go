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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/connectors"
)

// ASBBrokerDriver implements connectors.BrokerDriver for Azure Service Bus.
// It owns a shared publisher and admin client, creates receivers on demand,
// and tracks per-compacted-topic leader-election state (see replayer.go)
// started by EnsureCompactedTopic and torn down by Close.
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

// NewBrokerDriver creates an Azure Service Bus broker-driver backed by the
// given connection config.
//
// runtimeID identifies this process for the internal, per-instance ASB
// subscriptions EnsureCompactedTopic creates to keep a live state cache (see
// replayer.go). It should be a stable identity across restarts of the same
// instance (for example a Kubernetes pod name/ordinal set explicitly via
// runtime_id in config) — event-gateway auto-generates a fresh random
// RuntimeID on every boot when left unset, which works fine for Kafka
// (consumer groups are broker-managed and don't accumulate backlog when idle)
// but for ASB means every restart leaves one orphaned Subscription entity per
// compacted topic unless the operator pins a stable runtime_id.
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
	_, err := e.admin.GetTopic(ctx, topic, nil)
	if err == nil {
		return true, nil
	}
	if isNotFoundErr(err) {
		return false, nil
	}
	return false, fmt.Errorf("failed to check topic %s: %w", topic, err)
}

// EnsureTopics creates topics if they don't already exist (idempotent).
// metadata is an optional map of topic name -> key-value pairs for
// logging/tracking purposes. For WebSubAPI topics, metadata typically
// includes: apiName, apiVersion, channelName.
// Note: metadata is logged but not stored in ASB (topic properties have no
// place for arbitrary key-value pairs).
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

// Close shuts down the shared publisher, client, and any running
// leader-election goroutines for compacted topics.
func (e *ASBBrokerDriver) Close() error {
	var closeErr error
	e.closeOnce.Do(func() {
		e.compactedMu.Lock()
		for topic := range e.compacted {
			e.stopCompactedTopicLocked(topic)
		}
		e.compactedMu.Unlock()

		e.publisher.Close()

		if err := e.client.Close(context.Background()); err != nil {
			closeErr = fmt.Errorf("failed to close asb client: %w", err)
		}
	})
	return closeErr
}

// DeleteTopics deletes the given ASB topics, along with the snapshot and
// leader-lock queues EnsureCompactedTopic provisioned alongside any compacted
// topic. Mirrors the Kafka driver's best-effort semantics: a per-entity
// failure is logged, not returned, so a caller tearing down a binding is not
// blocked by one already-gone entity.
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

// deleteCompactionQueues removes the side-entities EnsureCompactedTopic
// creates for a compacted topic. Without this, a redeployed API reads back a
// stale snapshot and resurrects subscriptions that were deleted with the
// topic. Both are best-effort: a plain (non-compacted) topic simply has no
// such queues, and the resulting 404 is expected.
func (e *ASBBrokerDriver) deleteCompactionQueues(ctx context.Context, topic string) {
	snapshotQueue, err := snapshotQueueName(topic)
	if err != nil {
		return
	}
	lockQueue, err := leaderLockQueueName(topic)
	if err != nil {
		return
	}

	for _, queue := range []string{snapshotQueue, lockQueue} {
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

// isConflictErr reports whether err represents an ASB "entity already exists"
// (HTTP 409) response — not a real failure for idempotent create calls.
func isConflictErr(err error) bool {
	var respErr *azcore.ResponseError
	return errors.As(err, &respErr) && respErr.StatusCode == http.StatusConflict
}

// isNotFoundErr reports whether err represents an ASB "entity not found"
// (HTTP 404) response.
func isNotFoundErr(err error) bool {
	var respErr *azcore.ResponseError
	return errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound
}

// isSBTimeoutErr reports whether err is an *azservicebus.Error with
// CodeTimeout — the code returned both when no session is currently available
// and when a specific session ID is already locked by another receiver. Both
// are expected, retryable outcomes of a leader-election attempt, not failures.
func isSBTimeoutErr(err error) bool {
	var sbErr *azservicebus.Error
	return errors.As(err, &sbErr) && sbErr.Code == azservicebus.CodeTimeout
}
