package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type controllerConfigRouteStore struct {
	key              string
	expectedRevision string
	content          string
	replaceCalls     int
}

func (*controllerConfigRouteStore) Path() string {
	return "/etc/groundplane/controller.yaml"
}

func (*controllerConfigRouteStore) Current(context.Context) (string, string, bool, error) {
	return "# exact\n", "sha256:" + strings.Repeat("a", 64), false, nil
}

func (store *controllerConfigRouteStore) Replace(
	_ context.Context,
	key string,
	expectedRevision string,
	content string,
) (string, string, bool, error) {
	store.key = key
	store.expectedRevision = expectedRevision
	store.content = content
	store.replaceCalls++
	return content, "sha256:" + strings.Repeat("b", 64), true, nil
}

// Rationale: PUT is one protected human mutation, so Huma/OpenAPI and the
// handler must carry the required key to the durable store without changing
// the exact YAML or optimistic-concurrency revision.
func TestControllerConfigRouteRequiresAndForwardsIdempotencyKey(t *testing.T) {
	t.Parallel()
	store := &controllerConfigRouteStore{}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		ControllerConfig: store,
	})
	body := []byte(`{"content":"# exact\n","expected_revision":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`)

	missing := httptest.NewRecorder()
	server.HTTPHandler().ServeHTTP(
		missing,
		httptest.NewRequest(http.MethodPut, "/api/v1/controller/config", bytes.NewReader(body)),
	)
	if missing.Code != http.StatusUnprocessableEntity || store.replaceCalls != 0 {
		t.Fatalf("PUT without key = %d calls=%d body=%s", missing.Code, store.replaceCalls, missing.Body.String())
	}

	request := httptest.NewRequest(http.MethodPut, "/api/v1/controller/config", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "controller-route-key-0001")
	response := httptest.NewRecorder()
	server.HTTPHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("PUT = %d body=%s", response.Code, response.Body.String())
	}
	if store.key != "controller-route-key-0001" ||
		store.expectedRevision != "sha256:"+strings.Repeat("a", 64) ||
		store.content != "# exact\n" {
		t.Fatalf("store input = %q/%q/%q", store.key, store.expectedRevision, store.content)
	}
	var document struct {
		Content         string `json:"content"`
		Revision        string `json:"revision"`
		RestartRequired bool   `json:"restart_required"`
	}
	if err := json.NewDecoder(response.Body).Decode(&document); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if document.Content != "# exact\n" ||
		document.Revision != "sha256:"+strings.Repeat("b", 64) ||
		!document.RestartRequired {
		t.Fatalf("response = %#v", document)
	}
}

// Rationale: client generation must see Idempotency-Key as a required PUT
// parameter rather than relying only on the server's generic middleware.
func TestControllerConfigOpenAPIRequiresIdempotencyHeader(t *testing.T) {
	t.Parallel()
	document, err := New(nil, nil, Options{}).OpenAPIDocument()
	if err != nil {
		t.Fatalf("OpenAPIDocument() error = %v", err)
	}
	var contract struct {
		Paths map[string]map[string]struct {
			Parameters []struct {
				Name     string `json:"name"`
				In       string `json:"in"`
				Required bool   `json:"required"`
			} `json:"parameters"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(document, &contract); err != nil {
		t.Fatalf("decode OpenAPI: %v", err)
	}
	for _, parameter := range contract.Paths["/controller/config"]["put"].Parameters {
		if parameter.Name == "Idempotency-Key" && parameter.In == "header" && parameter.Required {
			return
		}
	}
	t.Fatal("PUT /controller/config lacks a required Idempotency-Key header")
}
