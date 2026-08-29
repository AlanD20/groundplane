package apiclient

import (
	"io"
	"net/http"
	"strings"
	"testing"

	generated "github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
)

// Rationale: a top-level nullable union decodes as a zero value in generated
// Go clients; the response envelope must preserve the null config state.
func TestGeneratedComponentConfigResponsePreservesNullConfig(t *testing.T) {
	t.Parallel()
	response, err := generated.ParseComponentConfigShowResponse(&http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"config":null}`)),
	})
	if err != nil {
		t.Fatalf("ParseComponentConfigShowResponse() error = %v", err)
	}
	if response.JSON200 == nil {
		t.Fatal("ParseComponentConfigShowResponse() returned nil response")
	}
	if response.JSON200.Config != nil {
		t.Fatalf("generated response config = %#v, want nil", response.JSON200.Config)
	}
}
