package controller

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

// QA: BACK-01, UI-01; local request admission, not backing provisioning.
// Rationale: a wrong body policy rejects a valid create before the owning
// handler can validate its typed input; this checks that dispatch prerequisite.
func TestBackingServiceCreateAcceptsJSONBody(t *testing.T) {
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/backing-services", nil)

	if policy := server.policyFor(request); policy.body != jsonBody {
		t.Fatalf("backing-service create body policy = %d, want JSON", policy.body)
	}
}
