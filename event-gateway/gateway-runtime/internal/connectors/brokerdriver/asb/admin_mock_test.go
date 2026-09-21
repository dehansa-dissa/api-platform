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
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
)

func TestTopicExists_MissingTopicAnswersFalse(t *testing.T) {
	driver, calls := mockAdmin(t, alwaysRespond(t, "topic_get_missing"))

	exists, err := driver.TopicExists(context.Background(), "absent-topic")
	if err != nil {
		t.Fatalf("a missing topic is an answer, not a failure: %v", err)
	}
	if exists {
		t.Fatal("reported a topic as present that the namespace does not have")
	}
	if len(*calls) != 1 {
		t.Fatalf("expected exactly one lookup, got %d", len(*calls))
	}
}

func TestTopicExists_PresentTopicAnswersTrue(t *testing.T) {
	driver, calls := mockAdmin(t, alwaysRespond(t, "topic_get_200"))

	exists, err := driver.TopicExists(context.Background(), "present-topic")
	if err != nil {
		t.Fatalf("TopicExists returned an error for a topic that exists: %v", err)
	}
	if !exists {
		t.Fatal("reported an existing topic as absent")
	}
	assertRequested(t, calls, http.MethodGet, "/present-topic")
}

func TestTopicExists_ServerFailureIsAnErrorNotAnAnswer(t *testing.T) {
	driver, _ := mockAdmin(t, respondStatus(http.StatusInternalServerError))

	exists, err := driver.TopicExists(context.Background(), "any-topic")
	if err == nil {
		t.Fatal("a server failure must surface as an error, not as a confident false")
	}
	if exists {
		t.Fatal("expected exists to be false alongside the error")
	}
}

func TestEnsureTopics_ConflictIsTreatedAsSuccess(t *testing.T) {
	driver, calls := mockAdmin(t, alwaysRespond(t, "topic_create_409"))

	if err := driver.EnsureTopics(context.Background(), []string{"existing-topic"}, nil); err != nil {
		t.Fatalf("a conflicting create must not fail an idempotent ensure: %v", err)
	}
	assertRequested(t, calls, http.MethodPut, "/existing-topic")
}

func TestEnsureTopics_ServerFailureAbortsStartup(t *testing.T) {
	driver, _ := mockAdmin(t, respondStatus(http.StatusInternalServerError))

	if err := driver.EnsureTopics(context.Background(), []string{"some-topic"}, nil); err == nil {
		t.Fatal("expected a server failure during ensure to abort")
	}
}

func TestReconcileSubscription_PreservesTheOtherProperties(t *testing.T) {
	driver, calls := mockAdmin(t, respondInOrder(t, "subscription_get_200", "subscription_update_200"))

	desired := &admin.SubscriptionProperties{AutoDeleteOnIdle: to.Ptr(iso8601Seconds(30 * time.Minute))}
	if err := driver.reconcileSubscription(context.Background(), "a-topic", "a-sub", desired); err != nil {
		t.Fatalf("reconcileIdleTimeout returned an error: %v", err)
	}

	if len(*calls) != 2 {
		t.Fatalf("expected a read then a write, got %d calls", len(*calls))
	}
	sent := (*calls)[1].Body
	if !strings.Contains(sent, "<AutoDeleteOnIdle>PT1800S</AutoDeleteOnIdle>") {
		t.Fatalf("the update did not carry the new idle timeout; body was:\n%s", sent)
	}
	for _, preserved := range []string{"<LockDuration>PT1M</LockDuration>", "<MaxDeliveryCount>10</MaxDeliveryCount>"} {
		if !strings.Contains(sent, preserved) {
			t.Fatalf("the update dropped %s, resetting it to a default; body was:\n%s", preserved, sent)
		}
	}
}

func TestReconcileSubscription_MatchingTimeoutSkipsTheWrite(t *testing.T) {
	driver, calls := mockAdmin(t, respondInOrder(t, "subscription_get_200"))

	desired := &admin.SubscriptionProperties{AutoDeleteOnIdle: to.Ptr(iso8601Seconds(10 * time.Minute))}
	if err := driver.reconcileSubscription(context.Background(), "a-topic", "a-sub", desired); err != nil {
		t.Fatalf("reconcileIdleTimeout returned an error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected the read alone, got %d calls — an unchanged timeout was rewritten", len(*calls))
	}
}

func TestReconcileSubscription_MissingSubscriptionIsNotAFailure(t *testing.T) {
	driver, calls := mockAdmin(t, alwaysRespond(t, "subscription_get_missing"))

	desired := &admin.SubscriptionProperties{AutoDeleteOnIdle: to.Ptr(iso8601Seconds(30 * time.Minute))}
	if err := driver.reconcileSubscription(context.Background(), "a-topic", "gone-sub", desired); err != nil {
		t.Fatalf("a reclaimed subscription is not a failure to reconcile: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected the read alone, got %d calls — it tried to update a subscription that is gone", len(*calls))
	}
}

func TestCreateSubscription_ConflictReconcilesTheIdleTimeout(t *testing.T) {
	driver, calls := mockAdmin(t, respondInOrder(t,
		"subscription_create_409", "subscription_get_200", "subscription_update_200"))

	props := &admin.SubscriptionProperties{AutoDeleteOnIdle: to.Ptr(iso8601Seconds(30 * time.Minute))}
	if err := driver.createSubscription(context.Background(), "a-topic", "a-sub", props); err != nil {
		t.Fatalf("a conflicting create must still succeed: %v", err)
	}

	if len(*calls) != 3 {
		t.Fatalf("expected create, read, update; got %d calls", len(*calls))
	}
	if !strings.Contains((*calls)[2].Body, "<AutoDeleteOnIdle>PT1800S</AutoDeleteOnIdle>") {
		t.Fatal("the conflict did not carry the configured idle timeout through to an update")
	}
}

func TestCreateSubscription_DurableSubscriptionSkipsTheReadBack(t *testing.T) {
	driver, calls := mockAdmin(t, respondInOrder(t, "subscription_create_409"))

	if err := driver.createSubscription(context.Background(), "a-topic", "a-sub", nil); err != nil {
		t.Fatalf("a conflicting durable create must succeed: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected the create alone, got %d calls", len(*calls))
	}
}

func TestDeleteCompactionQueues_MissingQueuesAreExpected(t *testing.T) {
	driver, calls := mockAdmin(t, alwaysRespond(t, "queue_delete_404"))

	driver.deleteCompactionQueues(context.Background(), "a-topic")

	if len(*calls) != 2 {
		t.Fatalf("expected the snapshot and leader-lock queues to be attempted, got %d calls", len(*calls))
	}
}

func TestConsumerStop_EphemeralDeletesItsSubscription(t *testing.T) {
	driver, calls := mockAdmin(t, alwaysRespond(t, "topic_delete_200"))

	receiver, err := driver.client.NewReceiverForSubscription("ws-topic", "grp-abc", nil)
	if err != nil {
		t.Fatalf("failed to build a receiver: %v", err)
	}
	consumer := &Consumer{
		receiver:  receiver,
		topic:     "ws-topic",
		ephemeral: true,
		driver:    driver,
		groupID:   "grp-abc",
	}

	if err := consumer.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned an error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected one delete, got %d calls", len(*calls))
	}
	if got := (*calls)[0]; got.Method != http.MethodDelete ||
		!strings.HasSuffix(got.Path, "/ws-topic/Subscriptions/grp-abc") {
		t.Fatalf("expected a DELETE of the connection's subscription, got %s %s", got.Method, got.Path)
	}
}

func TestConsumerStop_DurableKeepsItsSubscription(t *testing.T) {
	driver, calls := mockAdmin(t, alwaysRespond(t, "topic_delete_200"))

	receiver, err := driver.client.NewReceiverForSubscription("websub-topic", "grp-xyz", nil)
	if err != nil {
		t.Fatalf("failed to build a receiver: %v", err)
	}
	consumer := &Consumer{
		receiver: receiver,
		topic:    "websub-topic",
		driver:   driver,
		groupID:  "grp-xyz",
	}

	if err := consumer.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned an error: %v", err)
	}

	if len(*calls) != 0 {
		t.Fatalf("a durable consumer deleted its subscription on stop: %v", *calls)
	}
}

func TestReconcileSubscription_UpdatesAChangedMaxDeliveryCount(t *testing.T) {
	driver, calls := mockAdmin(t, respondInOrder(t, "subscription_get_200", "subscription_update_200"))

	desired := &admin.SubscriptionProperties{MaxDeliveryCount: to.Ptr(int32(20))}
	if err := driver.reconcileSubscription(context.Background(), "a-topic", "a-sub", desired); err != nil {
		t.Fatalf("reconcileSubscription returned an error: %v", err)
	}

	if len(*calls) != 2 {
		t.Fatalf("expected a read then a write, got %d calls", len(*calls))
	}
	sent := (*calls)[1].Body
	if !strings.Contains(sent, "<MaxDeliveryCount>20</MaxDeliveryCount>") {
		t.Fatalf("the update did not carry the new max delivery count; body was:\n%s", sent)
	}
	if !strings.Contains(sent, "<AutoDeleteOnIdle>PT10M</AutoDeleteOnIdle>") {
		t.Fatalf("reconciling one property reset another; body was:\n%s", sent)
	}
}

func TestReconcileSubscription_MatchingPropertiesSkipTheWrite(t *testing.T) {
	driver, calls := mockAdmin(t, respondInOrder(t, "subscription_get_200"))

	desired := &admin.SubscriptionProperties{
		AutoDeleteOnIdle: to.Ptr(iso8601Seconds(10 * time.Minute)),
		MaxDeliveryCount: to.Ptr(int32(10)),
	}
	if err := driver.reconcileSubscription(context.Background(), "a-topic", "a-sub", desired); err != nil {
		t.Fatalf("reconcileSubscription returned an error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected the read alone, got %d calls — unchanged properties were rewritten", len(*calls))
	}
}

func assertRequested(t *testing.T, calls *[]recordedCall, method, pathSuffix string) {
	t.Helper()

	for _, call := range *calls {
		if call.Method == method && strings.HasPrefix(call.Path, pathSuffix) {
			return
		}
	}
	t.Fatalf("no %s request to %q; the driver made: %v", method, pathSuffix, describeCalls(*calls))
}

func describeCalls(calls []recordedCall) []string {
	out := make([]string, 0, len(calls))
	for _, call := range calls {
		out = append(out, call.Method+" "+call.Path)
	}
	return out
}
