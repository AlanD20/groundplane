package controller

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: omission cannot reach publication or inherit a mode; each explicit
// mode is preserved, while other adapters must omit the unsupported decision.
func TestBackingServiceAuthenticationHTTPBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, adapter, field, mode string
		valid                      bool
	}{
		{"missing", "valkey:9", "", "", false},
		{"empty", "valkey:9", `,"authentication":""`, "", false},
		{"null", "valkey:9", `,"authentication":null`, "", false},
		{"invalid", "valkey:9", `,"authentication":"invalid"`, "", false},
		{"named", "valkey:9", `,"authentication":"username_password"`, "username_password", true},
		{"password", "valkey:9", `,"authentication":"password"`, "password", true},
		{"none", "valkey:9", `,"authentication":"none"`, "none", true},
		{"postgres", "postgres:16", "", "", true},
		{"postgres selected", "postgres:16", `,"authentication":"password"`, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutations := &backingAuthenticationMutations{}
			server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
				BackingServiceMutations: mutations,
			})
			body := `{"slug":"cache","name":"Cache","adapter":"` + tc.adapter + `"` + tc.field +
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
) (etcd.IdempotencyResponse, error) {
	mutations.calls++
	mutations.mode = input.Authentication
	return etcd.IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json", Body: []byte(`{}`),
	}, nil
}
