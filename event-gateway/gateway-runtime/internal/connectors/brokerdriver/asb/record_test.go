//go:build asbrecord

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

// Captures what a live namespace sends, so the mocked tests replay Azure's own
// bytes. Entities created here are deleted before the test returns.
//
//	go test -tags asbrecord -run TestRecordFixtures ./internal/connectors/brokerdriver/asb/

package asb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
)

var recordedHeaders = []string{"Content-Type"}

type recorder struct {
	t       *testing.T
	scrub   *strings.Replacer // live namespace identifiers, replaced on the way to disk
	name    string
	written map[string]bool
}

func (r *recorder) Do(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil || r.name == "" {
		return resp, err
	}

	raw, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	resp.Body = io.NopCloser(bytes.NewReader(raw))

	r.save(string(raw), resp)
	return resp, nil
}

// save writes one fixture, scrubbing the namespace so nothing in testdata
// identifies the account it came from.
func (r *recorder) save(body string, resp *http.Response) {
	f := fixture{
		Status:  resp.StatusCode,
		Body:    r.scrub.Replace(body),
		Headers: map[string]string{},
	}
	for _, h := range recordedHeaders {
		if v := resp.Header.Get(h); v != "" {
			f.Headers[h] = v
		}
	}

	encoded, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		r.t.Fatalf("failed to encode fixture %s: %v", r.name, err)
	}
	path := filepath.Join("testdata", r.name+".json")
	if err := os.WriteFile(path, append(encoded, '\n'), 0o600); err != nil {
		r.t.Fatalf("failed to write fixture %s: %v", path, err)
	}
	r.written[r.name] = true
	r.t.Logf("recorded %-28s %d", r.name, resp.StatusCode)
}

func (r *recorder) step(name string) { r.name = name }

// namespaceHost extracts the host from a connection string without logging any
// part of the string itself — the key travels in the same value.
func namespaceHost(t *testing.T, connectionString string) string {
	for _, part := range strings.Split(connectionString, ";") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(part), "Endpoint=sb://"); ok {
			return strings.TrimSuffix(rest, "/")
		}
	}
	t.Fatal("connection string has no Endpoint=sb:// part")
	return ""
}

// namespaceScrubber replaces the host and, separately, the bare short name Azure
// echoes in <author> and not-found details. Host first, so it wins the overlap.
func namespaceScrubber(t *testing.T, connectionString string) *strings.Replacer {
	host := namespaceHost(t, connectionString)
	shortName, _, ok := strings.Cut(host, ".")
	if !ok || shortName == "" {
		t.Fatalf("namespace host %q has no leading short name to scrub", host)
	}
	return strings.NewReplacer(host, fixtureNamespace, shortName, fixtureNamespaceName)
}

func TestRecordFixtures(t *testing.T) {
	connectionString := os.Getenv("APIP_EGW_ASB_CONNECTION_STRING")
	if connectionString == "" {
		t.Skip("APIP_EGW_ASB_CONNECTION_STRING is not set; nothing to record against")
	}
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatalf("failed to create testdata: %v", err)
	}

	rec := &recorder{t: t, scrub: namespaceScrubber(t, connectionString), written: map[string]bool{}}
	cfg := ConnectionConfig{
		ConnectionString:        connectionString,
		SubscriptionIdleTimeout: 10 * time.Minute,
	}
	driver, err := newBrokerDriver(cfg, "fixture-recorder", &admin.ClientOptions{
		ClientOptions: azcore.ClientOptions{Transport: rec},
	})
	if err != nil {
		t.Fatalf("failed to build recording driver: %v", err)
	}
	defer driver.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	topic := fmt.Sprintf("fixture-topic-%d", time.Now().UnixNano())
	const missing = "fixture-entity-that-does-not-exist"
	const subscription = "fixture-sub"

	defer func() {
		rec.step("")
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		if _, err := driver.admin.DeleteTopic(cleanupCtx, topic, nil); err != nil && !isNotFoundErr(err) {
			t.Errorf("failed to delete %s — remove it by hand: %v", topic, err)
		}
	}()

	rec.step("topic_create_201")
	if _, err := driver.admin.CreateTopic(ctx, topic, nil); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	rec.step("topic_create_409")
	if _, err := driver.admin.CreateTopic(ctx, topic, nil); err == nil {
		t.Fatal("expected a conflict creating the same topic twice")
	}

	rec.step("topic_get_200")
	if _, err := driver.admin.GetTopic(ctx, topic, nil); err != nil {
		t.Fatalf("GetTopic: %v", err)
	}

	rec.step("topic_get_missing")
	if _, err := driver.admin.GetTopic(ctx, missing, nil); err != nil {
		t.Fatalf("GetTopic(missing) returned an error rather than a nil response: %v", err)
	}

	rec.step("subscription_create_201")
	if err := driver.ensureDisposableSubscription(ctx, topic, subscription); err != nil {
		t.Fatalf("ensureDisposableSubscription: %v", err)
	}

	rec.step("subscription_create_409")
	if _, err := driver.admin.CreateSubscription(ctx, topic, subscription, &admin.CreateSubscriptionOptions{
		Properties: &admin.SubscriptionProperties{AutoDeleteOnIdle: to.Ptr(iso8601Seconds(30 * time.Minute))},
	}); err == nil {
		t.Fatal("expected a conflict creating the same subscription twice")
	}

	rec.step("subscription_get_200")
	existing, err := driver.admin.GetSubscription(ctx, topic, subscription, nil)
	if err != nil {
		t.Fatalf("GetSubscription: %v", err)
	}

	rec.step("subscription_get_missing")
	if _, err := driver.admin.GetSubscription(ctx, topic, "fixture-sub-absent", nil); err != nil {
		t.Fatalf("GetSubscription(missing) returned an error rather than a nil response: %v", err)
	}

	rec.step("subscription_update_200")
	updated := existing.SubscriptionProperties
	updated.AutoDeleteOnIdle = to.Ptr(iso8601Seconds(30 * time.Minute))
	if _, err := driver.admin.UpdateSubscription(ctx, topic, subscription, updated, nil); err != nil {
		t.Fatalf("UpdateSubscription: %v", err)
	}

	rec.step("queue_delete_404")
	if _, err := driver.admin.DeleteQueue(ctx, missing, nil); err == nil {
		t.Fatal("expected deleting a missing queue to fail")
	}

	rec.step("topic_delete_200")
	if _, err := driver.admin.DeleteTopic(ctx, topic, nil); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}

	rec.step("topic_delete_404")
	if _, err := driver.admin.DeleteTopic(ctx, missing, nil); err == nil {
		t.Fatal("expected deleting a missing topic to fail")
	}

	t.Logf("%d fixtures written to testdata/", len(rec.written))
}
