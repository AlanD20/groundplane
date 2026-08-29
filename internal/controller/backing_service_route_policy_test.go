package controller

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBackingServiceCreateAcceptsJSONBody(t *testing.T) {
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/backing-services", nil)

	if policy := server.policyFor(request); policy.body != jsonBody {
		t.Fatalf("backing-service create body policy = %d, want JSON", policy.body)
	}
}
