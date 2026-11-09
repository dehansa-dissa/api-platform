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
		t.Fatalf("missing fixture %s: %v", path, err)
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

func TestDeleteCompactionQueues_MissingQueuesAreExpected(t *testing.T) {
	driver, calls := mockAdmin(t, alwaysRespond(t, "queue_delete_404"))

	driver.deleteCompactionQueues(context.Background(), driver.home, "a-topic")

	if len(*calls) != 2 {
		t.Fatalf("expected the snapshot and leader-lock queues to be attempted, got %d calls", len(*calls))
	}
}
