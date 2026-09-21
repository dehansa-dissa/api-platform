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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
)

const fixtureNamespaceName = "example"
const fixtureNamespace = fixtureNamespaceName + ".servicebus.windows.net"
const fixtureConnectionString = "Endpoint=sb://" + fixtureNamespace +
	";SharedAccessKeyName=fixture;SharedAccessKey=ZmFrZS1rZXktZm9yLXRlc3RzLW9ubHk="

type fixture struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body,omitempty"`
}

func loadFixture(t *testing.T, name string) fixture {
	t.Helper()

	path := filepath.Join("testdata", name+".json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("missing fixture %s: %v\nrecord it with: "+
			"go test -tags asbrecord -run TestRecordFixtures ./internal/connectors/brokerdriver/asb/", path, err)
	}

	var f fixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("fixture %s is not valid JSON: %v", path, err)
	}
	return f
}

func (f fixture) response(req *http.Request) *http.Response {
	body := []byte(f.Body)
	resp := &http.Response{
		StatusCode:    f.Status,
		Status:        fmt.Sprintf("%d %s", f.Status, http.StatusText(f.Status)),
		Header:        http.Header{},
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
	for k, v := range f.Headers {
		resp.Header.Set(k, v)
	}
	return resp
}

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) Do(req *http.Request) (*http.Response, error) { return f(req) }

type recordedCall struct {
	Method string
	Path   string
	Body   string
}

func mockAdmin(t *testing.T, respond func(*http.Request) (*http.Response, error)) (*ASBBrokerDriver, *[]recordedCall) {
	t.Helper()

	calls := make([]recordedCall, 0, 4)
	transport := transportFunc(func(req *http.Request) (*http.Response, error) {
		call := recordedCall{Method: req.Method, Path: req.URL.Path}
		if req.Body != nil {
			raw, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			call.Body = string(raw)
			req.Body = io.NopCloser(bytes.NewReader(raw))
		}
		calls = append(calls, call)
		return respond(req)
	})

	driver, err := newBrokerDriver(
		ConnectionConfig{ConnectionString: fixtureConnectionString},
		"fixture-runtime",
		&admin.ClientOptions{ClientOptions: azcore.ClientOptions{
			Transport: transport,
			Retry:     policy.RetryOptions{MaxRetries: -1},
		}},
	)
	if err != nil {
		t.Fatalf("failed to build driver over the mock transport: %v", err)
	}
	t.Cleanup(func() { _ = driver.Close() })

	return driver, &calls
}

func alwaysRespond(t *testing.T, name string) func(*http.Request) (*http.Response, error) {
	t.Helper()

	f := loadFixture(t, name)
	return func(req *http.Request) (*http.Response, error) {
		return f.response(req), nil
	}
}

func respondInOrder(t *testing.T, names ...string) func(*http.Request) (*http.Response, error) {
	t.Helper()

	loaded := make([]fixture, 0, len(names))
	for _, name := range names {
		loaded = append(loaded, loadFixture(t, name))
	}

	i := 0
	return func(req *http.Request) (*http.Response, error) {
		if i >= len(loaded) {
			return nil, fmt.Errorf("unexpected call %d: %s %s, only %d fixtures registered",
				i+1, req.Method, req.URL.Path, len(loaded))
		}
		f := loaded[i]
		i++
		return f.response(req), nil
	}
}

func respondStatus(status int) func(*http.Request) (*http.Response, error) {
	return func(req *http.Request) (*http.Response, error) {
		return fixture{Status: status}.response(req), nil
	}
}
