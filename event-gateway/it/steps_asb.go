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

	// snapshotPeekCount matches the driver's own peek depth. Normally the queue
	// holds exactly one message; more means a previous cleanup failed, and the
	// newest by sequence number wins.
	snapshotPeekCount = 10

	// defaultITRuntimeID mirrors the APIP_EGW_RUNTIME_ID default in
	// docker-compose.test.asb.yaml. The driver names its live-tail subscription
	// from this, so the two must agree or the self-heal assertions look at a
	// subscription nobody owns.
	defaultITRuntimeID = "egw-it-asb"
)

// cacheSubscriptionName mirrors the driver's naming for the per-instance
// live-tail subscription: "cache-" plus the first 16 hex characters of the
// runtime ID's SHA-256, which keeps it inside ASB's 50-character limit.
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
	// The SDK maps a 404 to (nil, nil), so a missing subscription arrives as a
	// nil response with no error. Checking err alone reports everything as
	// present — which is exactly how this assertion first passed against a
	// subscription that did not exist.
	return resp != nil, nil
}

// isASBNotFound matches the shapes Service Bus reports a missing entity in.
// The management API answers HTTP 404; the SDK also wraps it as CodeNotFound.
// A receiver detaching reports a third, bare *amqp.Error shape, but that only
// reaches the driver, never these management calls.
func isASBNotFound(err error) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
		return true
	}
	var sbErr *azservicebus.Error
	return errors.As(err, &sbErr) && sbErr.Code == azservicebus.CodeNotFound
}

// syncTopicName derives an API's internal subscription sync topic.
//
// Reimplemented rather than imported: the production functions live in
// gateway-runtime's internal/binding, and Go's internal rule puts them out of
// reach of this module. TestSyncTopicName_MatchesProductionHash pins the result
// against a hash observed in a real run, so a change to the convention breaks
// the unit test rather than silently making these assertions read the wrong
// queue.
//
// apiName is the metadata name ("asb-restart-v1-0"), not the display name.
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

// countSnapshotSubscribers peeks the snapshot queue for a sync topic and
// returns how many subscribers the latest snapshot holds.
//
// Peek, never receive: the driver's own Replay peeks too, and a consuming read
// here would delete the state the gateway depends on — the test would corrupt
// what it is measuring.
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

	messages, err := receiver.PeekMessages(ctx, snapshotPeekCount, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to peek snapshot: %w", err)
	}
	if len(messages) == 0 {
		return 0, nil // no snapshot written yet
	}

	newest := messages[0]
	for _, msg := range messages[1:] {
		if msg.SequenceNumber != nil && (newest.SequenceNumber == nil || *msg.SequenceNumber > *newest.SequenceNumber) {
			newest = msg
		}
	}

	var snapshot map[string][]byte
	if err := json.Unmarshal(newest.Body, &snapshot); err != nil {
		return 0, fmt.Errorf("failed to decode snapshot payload: %w", err)
	}
	return len(snapshot), nil
}

// RegisterASBSteps registers steps that only the Azure Service Bus feature
// needs. They are broker-agnostic in form — restarting the gateway means the
// same thing on Kafka — but they exist because ASB is the only driver whose
// correctness depends on surviving one.
//
// compose is passed in rather than read from a package variable: the manager is
// created in suite_test.go, which the non-test files in this package cannot
// reference.
func RegisterASBSteps(ctx *godog.ScenarioContext, compose *ComposeManager) {
	// Proves subscriber state is rebuilt from the snapshot queue rather than
	// held in memory. Service Bus deletes consumed messages and a new
	// Subscription never receives anything published before it existed, so
	// without the snapshot a restart would lose every subscriber.
	ctx.Step(`^I restart the event gateway$`, func() error {
		if compose == nil {
			return fmt.Errorf("compose manager unavailable: the suite must be started through TestFeatures")
		}
		return compose.RestartService(context.Background(), gatewayServiceName)
	})

	// Replaces sleeping before a restart. A subscribe is durable only once the
	// leader has written it to the snapshot queue, and that lands well after the
	// hub returns 202: the change must round-trip the sync topic, reach the
	// leader's cache, and clear a debounce. Restarting before then loses it, so
	// this waits on the condition rather than guessing at the delay.
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

	// Deleting the live-tail subscription simulates what AutoDeleteOnIdle does
	// to an instance that has been quiet, and what an operator does by hand in
	// the portal. The driver is supposed to notice the 404 and rebuild.
	//
	// Worth testing rather than trusting: the recovery keys off isNotFoundErr,
	// which shipped twice matching only error shapes this path never produces,
	// so the healing had never once run.
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
