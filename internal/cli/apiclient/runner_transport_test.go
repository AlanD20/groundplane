package apiclient

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func TestScopedRunnerBearerStaysOnAllowedRoutesAtConfiguredOrigin(t *testing.T) {
	t.Parallel()

	const scopedOrigin = "http://127.0.0.1:8081"
	const outsideOrigin = "http://127.0.0.1:8082"
	serviceID := ids.New(ids.KindService)
	servicePath := "/api/v1/services/" + serviceID
	token := "gpr1_" + base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(tokenPath, 0o440); err != nil {
		t.Fatal(err)
	}
	client, err := NewRunner(scopedOrigin, tokenPath)
	if err != nil {
		t.Fatal(err)
	}

	requests := 0
	transport := client.HTTP.Transport.(*runnerTransport)
	transport.base = scopedRunnerTestRoundTripper(func(request *http.Request) (*http.Response, error) {
		requests++
		if request.URL.String() != scopedOrigin+servicePath || request.Method != http.MethodGet {
			t.Error("the Runner transport delivered a request outside the route and origin allowlist")
		}
		if seen := request.Header.Values("Authorization"); len(seen) != 1 || seen[0] != "Bearer "+token {
			t.Error("the scoped request was missing its bearer")
		}
		response := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader("{}")),
			Request:    request,
		}
		switch requests {
		case 2:
			response.StatusCode = http.StatusTemporaryRedirect
			response.Header.Set("Location", outsideOrigin+servicePath)
		case 3:
			response.StatusCode = http.StatusTemporaryRedirect
			response.Header.Set("Location", scopedOrigin+servicePath)
		}
		return response, nil
	})

	ctx := context.Background()
	if _, err := client.GetService(ctx, serviceID); err != nil {
		t.Fatalf("configured Service read failed: %v", err)
	}
	for _, path := range []string{"/api/v1/secrets", servicePath + "?limit=1"} {
		response, err := client.HTTP.Get(scopedOrigin + path)
		if err == nil {
			_ = response.Body.Close()
			t.Errorf("the Runner transport accepted off-contract path %s", path)
		}
	}
	for _, destination := range []string{"different origin", "same origin"} {
		if _, err := client.GetService(ctx, serviceID); err == nil {
			t.Errorf("redirect to %s unexpectedly succeeded", destination)
		}
	}
	client.BaseURL = outsideOrigin
	if _, err := client.GetService(ctx, serviceID); err == nil {
		t.Error("the Runner transport accepted a changed API origin")
	}
	if requests != 3 {
		t.Errorf("the Runner transport delivered %d requests, want 3", requests)
	}
}

type scopedRunnerTestRoundTripper func(*http.Request) (*http.Response, error)

func (roundTrip scopedRunnerTestRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}
