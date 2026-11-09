// --------------------------------------------------------------------
// Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// --------------------------------------------------------------------

package it

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/cucumber/godog"
)

const (
	// gatewayServiceName is the event-gateway service as named in every compose
	// stack in this repository.
	gatewayServiceName = "event-gateway"

	// snapshotQueueSuffix mirrors snapshotQueueName in the ASB driver.
	snapshotQueueSuffix = "-snapshot"

	// subscriptionTopicSuffix mirrors the third part binding.WebSubApiSubscriptionTopic
	// hashes when deriving an API's internal sync topic.
	subscriptionTopicSuffix = "__subscriptions"

	// snapshotPollInterval is how often the snapshot is re-read while waiting.
	// Each poll is a peek against a cloud broker, so this is deliberately not tight.
	snapshotPollInterval = 2 * time.Second

	// snapshotPeekPage matches the driver's page size. More than one message
	// means a cleanup failed, and the newest by sequence number wins.
	snapshotPeekPage = 50

	// defaultITRuntimeID mirrors the compose default. The driver names its
	// live-tail subscription from it, so the two must agree.
	defaultITRuntimeID = "egw-it-asb"
)

// cacheSubscriptionName mirrors the driver: "cache-" plus 16 hex characters of
// the runtime ID's SHA-256, keeping it inside ASB's 50-character limit.
func cacheSubscriptionName(runtimeID string) string {
	sum := sha256.Sum256([]byte(runtimeID))
	return "cache-" + fmt.Sprintf("%x", sum)[:16]
}

// itRuntimeID resolves the identity the gateway under test is running with,
// reading the same variable and default as the compose stack.
func itRuntimeID() string {
	if id := os.Getenv("APIP_EGW_RUNTIME_ID"); id != "" {
		return id
	}
	return defaultITRuntimeID
}

// asbAdmin builds a management client for the namespace under test.
func asbAdmin() (*admin.Client, error) {
	connectionString := os.Getenv("APIP_EGW_ASB_CONNECTION_STRING")
	if connectionString == "" {
		return nil, fmt.Errorf("APIP_EGW_ASB_CONNECTION_STRING is not set")
	}
	return admin.NewClientFromConnectionString(connectionString, nil)
}

// subscriptionExists reports whether a subscription is present on a topic,
// treating "not found" as a definite absence rather than an error.
func subscriptionExists(ctx context.Context, client *admin.Client, topic, subscription string) (bool, error) {
	resp, err := client.GetSubscription(ctx, topic, subscription, nil)
	if err != nil {
		if isASBNotFound(err) {
			return false, nil
		}
		return false, err
	}
	// The SDK maps a 404 to (nil, nil), so checking err alone reports everything
	// as present — how this first passed against a subscription that was gone.
	return resp != nil, nil
}

// isASBNotFound matches the two shapes a management call reports a missing
// entity in. The driver's third, bare *amqp.Error, never reaches here.
func isASBNotFound(err error) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
		return true
	}
	var sbErr *azservicebus.Error
	return errors.As(err, &sbErr) && sbErr.Code == azservicebus.CodeNotFound
}

// syncTopicName derives an API's sync topic from its metadata name, not its
// display name. Reimplemented because binding's version sits under internal/.
func syncTopicName(apiName, version string) string {
	return joinNormalizedTopic(apiName, version, subscriptionTopicSuffix)
}

// joinNormalizedTopic mirrors binding.JoinNormalizedTopic: each part is written
// as "<length>:<value>|" before hashing, so parts cannot run together.
func joinNormalizedTopic(parts ...string) string {
	var joined strings.Builder
	for _, part := range parts {
		fmt.Fprintf(&joined, "%d:%s|", len(part), part)
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(joined.String())))
}

// countSnapshotSubscribers peeks the snapshot queue and returns how many
// subscribers it holds. Peek, never receive: a consuming read would delete it.
func countSnapshotSubscribers(ctx context.Context, connectionString, syncTopic string) (int, error) {
	client, err := azservicebus.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create asb client: %w", err)
	}
	defer client.Close(context.Background())

	receiver, err := client.NewReceiverForQueue(syncTopic+snapshotQueueSuffix, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to open snapshot receiver: %w", err)
	}
	defer receiver.Close(context.Background())

	// Paged like the driver. Peek starts at the queue head, so a single window
	// would miss the newest message outright once a failed cleanup leaves more
	// copies than the window holds — the very state this step exists to catch.
	var newest *azservicebus.ReceivedMessage
	var from int64
	for paging := false; ; paging = true {
		opts := &azservicebus.PeekMessagesOptions{}
		if paging {
			opts.FromSequenceNumber = &from
		}
		batch, err := receiver.PeekMessages(ctx, snapshotPeekPage, opts)
		if err != nil {
			return 0, fmt.Errorf("failed to peek snapshot: %w", err)
		}
		if len(batch) == 0 {
			break
		}
		for _, msg := range batch {
			if msg.SequenceNumber == nil {
				continue
			}
			if newest == nil || *msg.SequenceNumber > *newest.SequenceNumber {
				newest = msg
			}
			from = *msg.SequenceNumber + 1
		}
	}
	if newest == nil {
		return 0, nil // no snapshot written yet
	}

	// RawMessage, not []byte: the driver writes a value that is already a JSON
	// object verbatim rather than base64-encoding it, and only the count matters
	// here, so neither shape has to be decoded further.
	var snapshot map[string]json.RawMessage
	if err := json.Unmarshal(newest.Body, &snapshot); err != nil {
		return 0, fmt.Errorf("failed to decode snapshot payload: %w", err)
	}
	return len(snapshot), nil
}

// RegisterASBSteps registers the steps only the Service Bus feature needs.
// compose is passed in because it is created in suite_test.go, which the
// non-test files here cannot reference.
func RegisterASBSteps(ctx *godog.ScenarioContext, compose *ComposeManager) {
	// Proves subscriber state is rebuilt from the snapshot rather than held in
	// memory: ASB has nothing to replay, so a restart would otherwise lose it.
	ctx.Step(`^I restart the event gateway$`, func() error {
		if compose == nil {
			return fmt.Errorf("compose manager unavailable: the suite must be started through TestFeatures")
		}
		return compose.RestartService(context.Background(), gatewayServiceName)
	})

	// A subscribe is durable only once the leader has written it out, well after
	// the 202. Waits on that condition rather than guessing at a delay.
	ctx.Step(`^the ASB snapshot for API "([^"]*)" version "([^"]*)" should contain (\d+) subscribers within (\d+) seconds$`,
		func(apiName, version string, want, timeoutSeconds int) error {
			connectionString := os.Getenv("APIP_EGW_ASB_CONNECTION_STRING")
			if connectionString == "" {
				return fmt.Errorf("APIP_EGW_ASB_CONNECTION_STRING is not set")
			}
			syncTopic := syncTopicName(apiName, version)

			deadline := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
			var lastCount int
			var lastErr error
			for {
				pollCtx, cancel := context.WithTimeout(context.Background(), snapshotPollInterval)
				lastCount, lastErr = countSnapshotSubscribers(pollCtx, connectionString, syncTopic)
				cancel()

				if lastErr == nil && lastCount == want {
					return nil
				}
				if time.Now().After(deadline) {
					break
				}
				time.Sleep(snapshotPollInterval)
			}

			if lastErr != nil {
				return fmt.Errorf("snapshot for %s %s (topic %s) unreadable after %ds: %w",
					apiName, version, syncTopic, timeoutSeconds, lastErr)
			}
			return fmt.Errorf("snapshot for %s %s (topic %s) held %d subscribers after %ds, want %d",
				apiName, version, syncTopic, lastCount, timeoutSeconds, want)
		})

	// Simulates AutoDeleteOnIdle reclaiming a quiet instance's subscription, or
	// an operator deleting it. Worth testing: the recovery keys off
	// isNotFoundErr, which shipped twice matching the wrong shapes.
	ctx.Step(`^I delete the ASB cache subscription for API "([^"]*)" version "([^"]*)"$`,
		func(apiName, version string) error {
			client, err := asbAdmin()
			if err != nil {
				return err
			}
			topic := syncTopicName(apiName, version)
			subscription := cacheSubscriptionName(itRuntimeID())

			delCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if _, err := client.DeleteSubscription(delCtx, topic, subscription, nil); err != nil {
				return fmt.Errorf("failed to delete %s on %s: %w", subscription, topic, err)
			}
			return nil
		})

	ctx.Step(`^the ASB cache subscription for API "([^"]*)" version "([^"]*)" should exist within (\d+) seconds$`,
		func(apiName, version string, timeoutSeconds int) error {
			client, err := asbAdmin()
			if err != nil {
				return err
			}
			topic := syncTopicName(apiName, version)
			subscription := cacheSubscriptionName(itRuntimeID())

			deadline := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
			var lastErr error
			for {
				pollCtx, cancel := context.WithTimeout(context.Background(), snapshotPollInterval)
				found, err := subscriptionExists(pollCtx, client, topic, subscription)
				cancel()

				if err == nil && found {
					return nil
				}
				lastErr = err
				if time.Now().After(deadline) {
					break
				}
				time.Sleep(snapshotPollInterval)
			}

			if lastErr != nil {
				return fmt.Errorf("could not check %s on %s after %ds: %w",
					subscription, topic, timeoutSeconds, lastErr)
			}
			return fmt.Errorf("subscription %s was still absent from %s after %ds; "+
				"the live tail did not rebuild it", subscription, topic, timeoutSeconds)
		})
}
