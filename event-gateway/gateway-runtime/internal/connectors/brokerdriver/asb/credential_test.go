package asb

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
)

const fakeAccessToken = "fake-entra-token"

type staticCredential struct{ requests *atomic.Int32 }

func (c staticCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	c.requests.Add(1)
	return azcore.AccessToken{Token: fakeAccessToken, ExpiresOn: time.Now().Add(time.Hour)}, nil
}

// useFakeCredential swaps the process credential for one that needs no Azure,
// and reports how many times a credential was constructed.
func useFakeCredential(t *testing.T) (constructed *atomic.Int32) {
	t.Helper()

	constructed = &atomic.Int32{}
	requests := &atomic.Int32{}
	original := newCredential

	credentialMu.Lock()
	credential = nil
	credentialMu.Unlock()

	newCredential = func() (azcore.TokenCredential, error) {
		constructed.Add(1)
		return staticCredential{requests: requests}, nil
	}
	t.Cleanup(func() {
		newCredential = original
		credentialMu.Lock()
		credential = nil
		credentialMu.Unlock()
	})
	return constructed
}

func TestNamespaceConfig_AuthenticatesWithTheEntraTokenNotASharedKey(t *testing.T) {
	useFakeCredential(t)

	topic := loadFixture(t, "topic_get_200")
	var authorization, host string
	transport := transportFunc(func(req *http.Request) (*http.Response, error) {
		authorization = req.Header.Get("Authorization")
		host = req.URL.Host
		return topic.response(req), nil
	})

	driver, err := newBrokerDriver(ConnectionConfig{Namespace: fixtureNamespace}, "fixture-runtime",
		&admin.ClientOptions{ClientOptions: azcore.ClientOptions{
			Transport: transport,
			Retry:     policy.RetryOptions{MaxRetries: -1},
		}})
	if err != nil {
		t.Fatalf("failed to build a driver from a namespace: %v", err)
	}
	t.Cleanup(func() { _ = driver.Close() })

	if _, err := driver.TopicExists(context.Background(), "orders"); err != nil {
		t.Fatalf("TopicExists failed: %v", err)
	}
	if host != fixtureNamespace {
		t.Errorf("expected the request to go to %s, got %s", fixtureNamespace, host)
	}
	// The SDK sends the token itself, unprefixed, as the Authorization header.
	if authorization != fakeAccessToken {
		t.Errorf("expected the Entra token from the credential, got %q", authorization)
	}
	if strings.Contains(authorization, "SharedAccessSignature") {
		t.Error("a namespace-configured driver sent a shared access signature")
	}
}

// A driver exists per WebSubApi, so the credential has to be shared or every
// tenant would hold, and refresh, its own token.
func TestNamespaceConfig_BuildsOneCredentialForEveryDriver(t *testing.T) {
	constructed := useFakeCredential(t)

	for _, ns := range []string{"tenants-01.servicebus.windows.net", "tenants-02.servicebus.windows.net"} {
		driver, err := NewBrokerDriver(ConnectionConfig{Namespace: ns}, "fixture-runtime")
		if err != nil {
			t.Fatalf("failed to build a driver for %s: %v", ns, err)
		}
		_ = driver.Close()
	}
	if got := constructed.Load(); got != 1 {
		t.Fatalf("expected one credential for the process, built %d", got)
	}
}

func TestConnectionStringConfig_NeverBuildsACredential(t *testing.T) {
	constructed := useFakeCredential(t)

	driver, err := NewBrokerDriver(ConnectionConfig{ConnectionString: fixtureConnectionString}, "fixture-runtime")
	if err != nil {
		t.Fatalf("failed to build a driver from a connection string: %v", err)
	}
	_ = driver.Close()

	if got := constructed.Load(); got != 0 {
		t.Fatalf("expected the connection-string path to leave Entra alone, built %d credentials", got)
	}
}
