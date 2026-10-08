package handlers

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	testidempotencyowner "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: omission cannot reach publication or inherit a mode; each explicit
// mode is preserved, while other adapters must omit the unsupported decision.
func TestBackingServiceAuthenticationHTTPBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, adapter, field, mode string
		valid                      bool
	}{
		{"missing", "valkey", "", "", false},
		{"empty", "valkey", `,"authentication":""`, "", false},
		{"null", "valkey", `,"authentication":null`, "", false},
		{"invalid", "valkey", `,"authentication":"invalid"`, "", false},
		{"named", "valkey", `,"authentication":"username_password"`, "username_password", true},
		{"password", "valkey", `,"authentication":"password"`, "password", true},
		{"none", "valkey", `,"authentication":"none"`, "none", true},
		{"postgres", "postgres", "", "", true},
		{"postgres selected", "postgres", `,"authentication":"password"`, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutations := &backingAuthenticationMutations{}
			server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
				BackingServiceMutations: mutations,
			})
			version := "9"
			if tc.adapter == "postgres" {
				version = "16"
			}
			body := `{"slug":"cache","name":"Cache","adapter":"` + tc.adapter + `","adapter_version":"` + version + `"` + tc.field +
				`,"network_pool":"10.80.0.0/24","zone":{"name":"data","subnet":"10.80.0.0/24","internal":true}}`
			request := httptest.NewRequest(http.MethodPost, "/api/v1/backing-services", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Idempotency-Key", "backing-authentication-0001")
			response := httptest.NewRecorder()
			server.HTTPHandler().ServeHTTP(response, request)
			if !tc.valid {
				if response.Code != http.StatusUnprocessableEntity || mutations.calls != 0 {
					t.Fatalf("invalid choice reached mutation: status=%d calls=%d body=%s",
						response.Code, mutations.calls, response.Body.String())
				}
				return
			}
			if response.Code != http.StatusCreated || mutations.calls != 1 || mutations.mode != tc.mode {
				t.Fatalf("explicit choice changed: status=%d calls=%d mode=%q body=%s",
					response.Code, mutations.calls, mutations.mode, response.Body.String())
			}
		})
	}
}

type backingAuthenticationMutations struct {
	BackingServiceMutator
	calls int
	mode  string
}

func (mutations *backingAuthenticationMutations) CreateBackingService(
	_ context.Context, input apiTypes.BackingServiceCreate, _ string,
) (testidempotencyowner.IdempotencyResponse, error) {
	mutations.calls++
	mutations.mode = input.Authentication
	return testidempotencyowner.IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json", Body: []byte(`{}`),
	}, nil
}
