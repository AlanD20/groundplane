package runnerregistry

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type testGatewayAuthorizer func(context.Context, string, string) (Grant, error)

func (authorize testGatewayAuthorizer) AuthorizeRegistryJob(
	ctx context.Context,
	user, credential string,
) (Grant, error) {
	return authorize(ctx, user, credential)
}

type testGatewayTransport func(*http.Request) (*http.Response, error)

func (transport testGatewayTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestGatewayAuthenticatesEachScopedDistributionRequest(t *testing.T) {
	const repository = "groundplane/job-12"
	const uploadID = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
	digest := "sha256:" + strings.Repeat("a", 64)
	base := "/v2/" + repository
	granted := Pull | Push
	backend := &url.URL{Scheme: "https", Host: "127.0.0.1:5000"}
	gateway, err := NewGateway(GatewayConfig{
		Backend: backend, BackendUsername: "root", BackendCredential: "registry-private", TLSRoots: x509.NewCertPool(),
	}, testGatewayAuthorizer(func(_ context.Context, user, credential string) (Grant, error) {
		if user != "job-12" || credential != "job-credential" {
			return Grant{}, nil
		}
		return Grant{
			Subject:    user,
			Repository: repository,
			ExpiresAt:  time.Now().Add(time.Minute),
			Actions:    granted,
		}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	backend.Host = "different.example"
	backendCalls := 0
	gateway.transport = testGatewayTransport(func(request *http.Request) (*http.Response, error) {
		backendCalls++
		user, credential, ok := request.BasicAuth()
		if !ok || user != "root" || credential != "registry-private" || request.Header.Get("Cookie") != "" ||
			request.Header.Get(
				"X-Forwarded-Host",
			) != "" || request.URL.Host != "127.0.0.1:5000" || request.URL.Scheme != "https" {
			t.Fatal("Registry request did not use only the fixed backend and its own credentials")
		}
		response := &http.Response{
			StatusCode: http.StatusAccepted,
			Header:     http.Header{"Set-Cookie": {"upstream-data"}},
			Body:       io.NopCloser(strings.NewReader("")),
		}
		if request.URL.Path == base+"/blobs/uploads/" {
			response.Header.Set("Location", "https://127.0.0.1:5000"+base+"/blobs/uploads/"+uploadID+"?_state=state")
		}
		if strings.HasSuffix(request.URL.Path, "/redirect") {
			response.StatusCode = http.StatusTemporaryRedirect
			response.Header.Set("Location", "https://storage.example/temporary-grant")
		}
		return response, nil
	})

	request := func(method, path string, authenticated bool) *httptest.ResponseRecorder {
		input := httptest.NewRequest(method, path, nil)
		input.TLS = &tls.ConnectionState{}
		input.Header.Set("Cookie", "job-session")
		input.Header.Set("X-Forwarded-Host", "different.example")
		if authenticated {
			input.SetBasicAuth("job-12", "job-credential")
		}
		output := httptest.NewRecorder()
		gateway.ServeHTTP(output, input)
		return output
	}
	if output := request(http.MethodGet, "/v2/", false); output.Code != http.StatusUnauthorized ||
		output.Header().Get("Www-Authenticate") == "" {
		t.Fatal("unauthenticated Registry discovery did not challenge Docker")
	}
	if output := request(http.MethodPost, base+"/blobs/uploads/", true); output.Code != http.StatusAccepted ||
		output.Header().
			Get("Location") !=
			base+"/blobs/uploads/"+uploadID+"?_state=state" || output.Header().Get("Set-Cookie") != "" {
		t.Fatal("Registry upload did not retain its continuation on the scoped gateway")
	}
	if output := request(http.MethodPut, base+"/blobs/uploads/"+uploadID+"?_state=state&digest="+digest, true); output.Code != http.StatusAccepted {
		t.Fatal("Registry upload could not complete within its grant")
	}
	if output := request(http.MethodGet, base+"/manifests/redirect", true); output.Code != http.StatusBadGateway ||
		output.Header().Get("Location") != "" {
		t.Fatal("Registry redirect escaped the gateway")
	}
	beforeDenials := backendCalls
	for _, denied := range []string{
		"/v2/groundplane/job-13/manifests/latest", "/v2/_catalog", base + "/manifests/../latest",
		base + "/blobs/uploads/?mount=" + digest + "&from=groundplane/job-13",
		base + "/blobs/uploads/?mount=" + digest + "&from=" + repository + "&from=groundplane/job-13",
	} {
		if output := request(http.MethodPost, denied, true); output.Code != http.StatusForbidden {
			t.Errorf("Registry forwarded a disallowed route %s, status %d", denied, output.Code)
		}
	}
	granted = Pull
	if output := request(http.MethodPut, base+"/manifests/latest", true); output.Code != http.StatusForbidden {
		t.Fatal("pull-only grant permitted a write")
	}
	granted = 0
	if output := request(http.MethodGet, base+"/manifests/latest", true); output.Code != http.StatusUnauthorized {
		t.Fatal("revoked grant permitted another request")
	}
	if backendCalls != beforeDenials {
		t.Fatal("a disallowed request reached the Registry")
	}
}
