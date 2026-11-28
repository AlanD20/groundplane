package runneridentity

import (
	"context"
	"crypto/sha256"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnercredential"
)

func TestRegistryCredentialExchangeRequiresBaseBearerOnExactRoute(t *testing.T) {
	jobID := ids.New(ids.KindOperation)
	base, baseDigest, err := Mint()
	if err != nil {
		t.Fatal(err)
	}
	job, jobDigest, err := Mint()
	if err != nil {
		t.Fatal(err)
	}
	binding := runnercredential.Binding{
		RunnerID: ids.New(ids.KindRunner), RunnerEpoch: 1, Revision: 1, ExpiresAt: time.Now().Add(time.Minute),
	}
	jobBinding := binding
	jobBinding.RequestID = jobID
	reader := registryCredentialTestReader{baseDigest: binding, jobDigest: jobBinding}
	handler := &Handler{reader: reader, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	calls := 0
	exchange := registryCredentialTestExchange(func(
		_ context.Context,
		principal Principal,
		id string,
		proof string,
	) (RegistryCredential, error) {
		calls++
		if id != jobID || principal.Credential != binding || proof != "signed-proof" {
			t.Errorf("exchange did not receive the authenticated Runner and exact job")
		}
		return RegistryCredential{Username: id}, nil
	})
	path := "/api/v1/runner-jobs/" + jobID + "/registry-credential"
	runnable := handler.WithRegistryCredentialExchange(exchange)
	tests := []struct {
		name   string
		path   string
		bearer string
		body   string
		status int
	}{
		{"missing bearer", path, "", `{"proof":"signed-proof"}`, http.StatusForbidden},
		{"bound job bearer", path, job, `{"proof":"signed-proof"}`, http.StatusForbidden},
		{"alternate URL", path + "?a=1", base, `{"proof":"signed-proof"}`, http.StatusNotFound},
		{"duplicate proof", path, base, `{"proof":"first","proof":"second"}`, http.StatusBadRequest},
		{"exact active bearer", path, base, `{"proof":"signed-proof"}`, http.StatusCreated},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
			if test.bearer != "" {
				request.Header.Set("Authorization", "Bearer "+test.bearer)
			}
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			runnable.ServeHTTP(response, request)
			if response.Code != test.status || response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("unexpected response %d: %s", response.Code, response.Body.String())
			}
		})
	}
	if calls != 1 {
		t.Fatalf("credential provider called %d times for one permitted request", calls)
	}
}

type registryCredentialTestReader map[[sha256.Size]byte]runnercredential.Binding

func (reader registryCredentialTestReader) ReadRunnerCredential(
	_ context.Context,
	digest [sha256.Size]byte,
) (runnercredential.Binding, error) {
	return reader[digest], nil
}

type registryCredentialTestExchange func(context.Context, Principal, string, string) (RegistryCredential, error)

func (exchange registryCredentialTestExchange) IssueRegistryCredential(
	ctx context.Context,
	principal Principal,
	id string,
	proof string,
) (RegistryCredential, error) {
	return exchange(ctx, principal, id, proof)
}
