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
	"maps"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/Azure/go-amqp"
	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/connectors"
)

const cacheSubscriptionReleaseTimeout = 10 * time.Second

// ASBBrokerDriver implements connectors.BrokerDriver for Azure Service Bus.
type ASBBrokerDriver struct {
	cfg        ConnectionConfig
	instanceID string // stable-per-process identity for this driver's internal cache subscriptions (see replayer.go)

	// home reaches the namespace holding the snapshot group. Without a pool it
	// is the only namespace; with one it is nil until the tenant is placed.
	home *nsConn

	// A pool driver serves one tenant whose channel topics may span
	// namespaces, so it connects to each as it finds topics there.
	pool      *namespacePool
	tenant    string
	syncTopic string
	placeMu   sync.Mutex
	conns     map[string]*nsConn // namespace -> connection
	topics    map[string]*nsConn // topic -> connection
	closed    bool

	compactedMu sync.Mutex
	compacted   map[string]*compactedTopicState // topic -> leader-election state

	closeOnce sync.Once
}

// nsConn is a driver's connection to one namespace.
type nsConn struct {
	name      string // the pool member name; empty without a pool
	host      string
	client    *azservicebus.Client
	admin     *admin.Client
	publisher *Publisher
}

func newConn(cfg ConnectionConfig, adminOpts *admin.ClientOptions) (*nsConn, error) {
	client, err := NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create asb client: %w", err)
	}
	adminClient, err := newAdminClient(cfg, adminOpts)
	if err != nil {
		_ = client.Close(context.Background())
		return nil, fmt.Errorf("failed to create asb admin client: %w", err)
	}
	return &nsConn{
		host:      namespaceHost(cfg),
		client:    client,
		admin:     adminClient,
		publisher: NewPublisher(client),
	}, nil
}

func (c *nsConn) close() error {
	c.publisher.Close()
	if err := c.client.Close(context.Background()); err != nil {
		return fmt.Errorf("failed to close asb client: %w", err)
	}
	return nil
}

var (
	credentialMu sync.Mutex
	credential   azcore.TokenCredential

	// newCredential is replaced in tests so a client can be built without Azure.
	newCredential = func() (azcore.TokenCredential, error) {
		return azidentity.NewDefaultAzureCredential(nil)
	}
)

// sharedCredential returns one Entra ID credential for the whole process.
func sharedCredential() (azcore.TokenCredential, error) {
	credentialMu.Lock()
	defer credentialMu.Unlock()

	if credential != nil {
		return credential, nil
	}
	cred, err := newCredential()
	if err != nil {
		return nil, fmt.Errorf("failed to create azure credential: %w", err)
	}
	credential = cred
	return credential, nil
}

// NewClient creates an Azure Service Bus client using the shared connection config.
func NewClient(cfg ConnectionConfig) (*azservicebus.Client, error) {
	if cfg.Namespace != "" {
		cred, err := sharedCredential()
		if err != nil {
			return nil, err
		}
		return azservicebus.NewClient(cfg.Namespace, cred, nil)
	}
	return azservicebus.NewClientFromConnectionString(cfg.ConnectionString, nil)
}

func newAdminClient(cfg ConnectionConfig, opts *admin.ClientOptions) (*admin.Client, error) {
	if cfg.Namespace != "" {
		cred, err := sharedCredential()
		if err != nil {
			return nil, err
		}
		return admin.NewClient(cfg.Namespace, cred, opts)
	}
	return admin.NewClientFromConnectionString(cfg.ConnectionString, opts)
}

// NewBrokerDriver creates an ASB broker-driver.
func NewBrokerDriver(cfg ConnectionConfig, runtimeID string) (*ASBBrokerDriver, error) {
	if cfg.SubscriptionID != "" {
		pool, err := sharedPool(cfg)
		if err != nil {
			return nil, err
		}
		return &ASBBrokerDriver{
			cfg:        cfg,
			instanceID: runtimeID,
			compacted:  make(map[string]*compactedTopicState),
			pool:       pool,
			conns:      map[string]*nsConn{},
			topics:     map[string]*nsConn{},
		}, nil
	}

	driver, err := newBrokerDriver(cfg, runtimeID, nil)
	if err != nil {
		return nil, err
	}
	return driver, nil
}

// newBrokerDriver exposes the admin client's options so a test can replay
// recorded responses instead of dialling.
func newBrokerDriver(cfg ConnectionConfig, runtimeID string, adminOpts *admin.ClientOptions) (*ASBBrokerDriver, error) {
	home, err := newConn(cfg, adminOpts)
	if err != nil {
		return nil, err
	}
	return &ASBBrokerDriver{
		cfg:        cfg,
		instanceID: runtimeID,
		home:       home,
		compacted:  make(map[string]*compactedTopicState),
	}, nil
}

var errDriverClosed = errors.New("asb driver is closed")

// connFor returns the connection for the namespace holding topic, looking a
// pool driver's topic up the first time. errNotPlaced means none holds it.
func (e *ASBBrokerDriver) connFor(ctx context.Context, topic string) (*nsConn, error) {
	if e.pool == nil {
		return e.home, nil
	}
	e.placeMu.Lock()
	defer e.placeMu.Unlock()

	if e.closed {
		return nil, errDriverClosed
	}
	if c, ok := e.topics[topic]; ok {
		return c, nil
	}
	m, err := e.pool.lookup(ctx, e.tenant, topic)
	if err != nil {
		return nil, err
	}
	return e.bindLocked(topic, m)
}

// placeChannels has the pool find or create a tenant's channel topics and
// connects to every namespace they landed in.
func (e *ASBBrokerDriver) placeChannels(ctx context.Context, topics []string, metadata map[string]map[string]string) error {
	e.placeMu.Lock()
	defer e.placeMu.Unlock()

	if e.closed {
		return errDriverClosed
	}
	if e.tenant == "" {
		e.tenant = tenantKey(topics, metadata)
	}
	placed, err := e.pool.ensureChannels(ctx, e.tenant, topics)
	if err != nil {
		return err
	}
	for topic, m := range placed {
		if _, err := e.bindLocked(topic, m); err != nil {
			return err
		}
	}
	return nil
}

// homeConn connects a pool driver to its tenant's home namespace, where the
// snapshot group lives. With create a tenant that is new gets placed.
func (e *ASBBrokerDriver) homeConn(ctx context.Context, syncTopic string, create bool) (*nsConn, error) {
	if e.pool == nil {
		return e.home, nil
	}
	e.placeMu.Lock()
	defer e.placeMu.Unlock()

	if e.closed {
		return nil, errDriverClosed
	}
	if e.home != nil {
		return e.home, nil
	}
	if e.tenant == "" {
		e.tenant = syncTopic
	}

	var m poolMember
	var err error
	if create {
		m, err = e.pool.ensureSnapshotGroup(ctx, e.tenant, syncTopic)
	} else {
		m, err = e.pool.lookup(ctx, e.tenant, syncTopic)
	}
	if err != nil {
		return nil, err
	}
	c, err := e.bindLocked(syncTopic, m)
	if err != nil {
		return nil, err
	}
	e.home, e.syncTopic = c, syncTopic
	return c, nil
}

// bindLocked records topic as living in m, connecting to m the first time.
func (e *ASBBrokerDriver) bindLocked(topic string, m poolMember) (*nsConn, error) {
	c, ok := e.conns[m.name]
	if !ok {
		var err error
		if c, err = newConn(m.cfg, nil); err != nil {
			return nil, err
		}
		c.name = m.name
		e.conns[m.name] = c
	}
	e.topics[topic] = c
	return c, nil
}

// tenantKey names the WebSubApi a pool driver serves in the namespace mapping.
// A caller passing no metadata is keyed by its first topic instead.
func tenantKey(topics []string, metadata map[string]map[string]string) string {
	first := slices.Min(topics)
	if md := metadata[first]; md["apiName"] != "" {
		return md["apiName"] + "/" + md["apiVersion"]
	}
	return first
}

var _ connectors.BrokerDriver = (*ASBBrokerDriver)(nil)

func (e *ASBBrokerDriver) Type() string { return "azureservicebus" }

// Publish sends a message to the given ASB topic.
func (e *ASBBrokerDriver) Publish(ctx context.Context, topic string, msg *connectors.Message) error {
	c, err := e.connFor(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to publish to topic %s: %w", topic, err)
	}
	return c.publisher.Publish(ctx, topic, msg)
}

// TopicExists checks whether a topic exists in the ASB namespace.
func (e *ASBBrokerDriver) TopicExists(ctx context.Context, topic string) (bool, error) {
	c, err := e.connFor(ctx, topic)
	if err != nil {
		if errors.Is(err, errNotPlaced) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check topic %s: %w", topic, err)
	}
	exists, err := topicExists(ctx, c.admin, topic)
	if err != nil {
		return false, fmt.Errorf("failed to check topic %s: %w", topic, err)
	}
	return exists, nil
}

func topicExists(ctx context.Context, client *admin.Client, topic string) (bool, error) {
	resp, err := client.GetTopic(ctx, topic, nil)
	if err != nil {
		if isNotFoundErr(err) {
			return false, nil
		}
		return false, err
	}
	return resp != nil, nil
}

// createTopic reports whether it created the topic; one that already exists
// is not an error.
func createTopic(ctx context.Context, client *admin.Client, topic string) (bool, error) {
	if _, err := client.CreateTopic(ctx, topic, nil); err != nil {
		if isConflictErr(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// EnsureTopics creates topics if absent (idempotent). A pool driver has the pool
// create them, since where each one goes depends on room.
func (e *ASBBrokerDriver) EnsureTopics(ctx context.Context, topics []string, metadata map[string]map[string]string) error {
	if e.pool != nil && len(topics) > 0 {
		if err := e.placeChannels(ctx, topics, metadata); err != nil {
			return fmt.Errorf("failed to place topics in a namespace: %w", err)
		}
	}
	for _, topic := range topics {
		if e.pool == nil {
			created, err := createTopic(ctx, e.home.admin, topic)
			if err != nil {
				return fmt.Errorf("failed to create topic %s: %w", topic, err)
			}
			if created {
				slog.Info("Created topic", "topic", topic)
			} else {
				slog.Debug("Topic already exists", "topic", topic)
			}
		}

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
		e.placeMu.Lock()
		defer e.placeMu.Unlock()
		e.closed = true

		e.compactedMu.Lock()
		topics := make([]string, 0, len(e.compacted))
		for topic := range e.compacted {
			topics = append(topics, topic)
			e.stopCompactedTopicLocked(topic)
		}
		e.compactedMu.Unlock()

		if e.home != nil {
			e.releaseCacheSubscriptions(topics)
		}

		conns := slices.Collect(maps.Values(e.conns))
		if e.pool == nil {
			conns = []*nsConn{e.home}
		}
		for _, c := range conns {
			if err := c.close(); err != nil && closeErr == nil {
				closeErr = err
			}
		}
	})
	return closeErr
}

// releaseCacheSubscriptions returns quota on a graceful shutdown rather than waiting out AutoDeleteOnIdle.
func (e *ASBBrokerDriver) releaseCacheSubscriptions(topics []string) {
	if len(topics) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), cacheSubscriptionReleaseTimeout)
	defer cancel()

	subscriptionName := cacheSubscriptionName(e.instanceID)
	for _, topic := range topics {
		if _, err := e.home.admin.DeleteSubscription(ctx, topic, subscriptionName, nil); err != nil && !isNotFoundErr(err) {
			slog.Warn("Failed to release cache subscription on shutdown",
				"topic", topic, "subscription", subscriptionName, "error", err)
		}
	}
}

// DeleteTopics deletes the topics plus any compaction side-queues. A pool
// driver then returns their room, and forgets the tenant with its sync topic.
func (e *ASBBrokerDriver) DeleteTopics(ctx context.Context, topics []string) error {
	e.placeMu.Lock()
	tenant, syncTopic := e.tenant, e.syncTopic
	e.placeMu.Unlock()

	removed := map[string]int{}
	forget := false
	for _, topic := range topics {
		c, err := e.connFor(ctx, topic)
		if errors.Is(err, errNotPlaced) {
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to find the namespace holding topic %s: %w", topic, err)
		}
		c.publisher.closeSender(topic)
		e.stopCompactedTopic(topic)
		deleted := e.deleteCompactionQueues(ctx, c, topic)
		if deleted > 0 || topic == syncTopic {
			forget = true
		}

		if _, err := c.admin.DeleteTopic(ctx, topic, nil); err != nil {
			slog.Warn("Failed to delete topic", "topic", topic, "error", err)
		} else {
			slog.Info("Deleted topic", "topic", topic)
			deleted++
		}
		removed[c.name] += deleted
	}

	if e.pool != nil && len(removed) > 0 {
		if err := e.pool.release(ctx, tenant, removed, forget); err != nil {
			slog.Warn("Failed to update the namespace mapping after deleting topics", "tenant", tenant, "error", err)
		}
	}
	return nil
}

// deleteCompactionQueues removes EnsureCompactedTopic's queues so a redeployed
// API can't restore subscribers from a stale snapshot. Returns how many it deleted.
func (e *ASBBrokerDriver) deleteCompactionQueues(ctx context.Context, c *nsConn, topic string) int {
	deleted := 0
	for _, queue := range []string{snapshotQueueName(topic), leaderLockQueueName(topic)} {
		c.publisher.closeSender(queue)
		if _, err := c.admin.DeleteQueue(ctx, queue, nil); err != nil {
			if !isNotFoundErr(err) {
				slog.Warn("Failed to delete compaction queue", "queue", queue, "error", err)
			}
			continue
		}
		slog.Info("Deleted compaction queue", "queue", queue)
		deleted++
	}
	return deleted
}

// isLockLostErr reports a lock that is provably gone rather than merely
// unconfirmed, so the caller must concede instead of retrying.
func isLockLostErr(err error) bool {
	var sbErr *azservicebus.Error
	return errors.As(err, &sbErr) && sbErr.Code == azservicebus.CodeLockLost
}

// isConflictErr reports an ASB "entity already exists" (HTTP 409) response.
func isConflictErr(err error) bool {
	var respErr *azcore.ResponseError
	return errors.As(err, &respErr) && respErr.StatusCode == http.StatusConflict
}

// sessionCannotBeLocked is returned when another receiver holds the session. The
// SDK has no Code for it, so the AMQP condition is matched directly.
const sessionCannotBeLocked = "com.microsoft:session-cannot-be-locked"

func isSessionHeldErr(err error) bool {
	var amqpErr *amqp.Error
	return errors.As(err, &amqpErr) && string(amqpErr.Condition) == sessionCannotBeLocked
}

// isNotFoundErr matches all three shapes the SDK delivers: HTTP 404, CodeNotFound,
// and a bare *amqp.Error. The receive loops self-heal off this.
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
