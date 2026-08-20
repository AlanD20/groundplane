package apiclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/oklog/ulid/v2"
)

func TestDoNormalizesBaseURLAndRequestsJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/tasks" {
			t.Errorf("path = %q, want %q", request.URL.Path, "/api/v1/tasks")
		}
		if accept := request.Header.Get("Accept"); accept != "application/json" {
			t.Errorf("Accept = %q, want application/json", accept)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"id": "task-1"})
	}))
	defer server.Close()

	var response map[string]string
	client := New(server.URL + "/")
	request := client.NewRequest(http.MethodGet, "/api/v1/tasks", nil, nil)
	err := client.Do(context.Background(), request, &response)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if response["id"] != "task-1" {
		t.Fatalf("response id = %q, want task-1", response["id"])
	}
}

func TestMutationRequestsCarryOneReusableRawULID(t *testing.T) {
	t.Parallel()

	var keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		keys = append(keys, request.Header.Get(idempotencyKeyHeader))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := New(server.URL)
	request := client.NewRequest(http.MethodPost, "/api/v1/tasks", nil, map[string]string{"action": "run"})
	for range 2 {
		if err := client.Do(context.Background(), request, nil); err != nil {
			t.Fatalf("Do() error = %v", err)
		}
	}

	if len(keys) != 2 || keys[0] != keys[1] {
		t.Fatalf("Idempotency-Key values = %q, want the same key twice", keys)
	}
	if len(keys[0]) != 26 {
		t.Fatalf("Idempotency-Key length = %d, want raw 26-character ULID", len(keys[0]))
	}
	if _, err := ulid.ParseStrict(keys[0]); err != nil {
		t.Fatalf("Idempotency-Key = %q, want raw ULID: %v", keys[0], err)
	}
}

func TestRequestIdempotencyKeyContract(t *testing.T) {
	t.Parallel()

	methods := []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete}
	for _, method := range methods {
		request := New("http://invalid").NewRequest(method, "/resource", nil, nil)
		if !idempotencyKeyPattern.MatchString(request.IdempotencyKey) {
			t.Errorf("%s key = %q, want 16..128 allowed characters", method, request.IdempotencyKey)
		}
	}

	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodOptions} {
		request := New("http://invalid").NewRequest(method, "/resource", nil, nil)
		if request.IdempotencyKey != "" {
			t.Errorf("%s key = %q, want empty", method, request.IdempotencyKey)
		}
	}
}

func TestDoRejectsInvalidIdempotencyKeysAsInternal(t *testing.T) {
	t.Parallel()

	tests := []Request{
		{Method: http.MethodPost, Path: "/resource"},
		{Method: http.MethodPost, Path: "/resource", IdempotencyKey: "too-short"},
		{Method: http.MethodPost, Path: "/resource", IdempotencyKey: strings.Repeat("a", 129)},
		{Method: http.MethodPost, Path: "/resource", IdempotencyKey: "invalid key spaces"},
		{Method: http.MethodGet, Path: "/resource", IdempotencyKey: strings.Repeat("a", 16)},
	}

	client := New("http://127.0.0.1:1")
	for _, request := range tests {
		err := client.Do(context.Background(), request, nil)
		if !errors.Is(err, errs.New(errs.CodeInternal, "")) {
			t.Errorf("Do(%s, key=%q) error = %v, want %q", request.Method, request.IdempotencyKey, err, errs.CodeInternal)
		}
	}
}

func TestDoRequiresExactlyOneJSONDocumentForOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status int
		body   string
		valid  bool
	}{
		{name: "one document", status: http.StatusOK, body: `{"ok":true}`, valid: true},
		{name: "empty", status: http.StatusOK},
		{name: "no content", status: http.StatusNoContent},
		{name: "malformed", status: http.StatusOK, body: `{`},
		{name: "multiple", status: http.StatusOK, body: `{}` + "\n" + `{}`},
		{name: "trailing malformed", status: http.StatusOK, body: `{} trailing`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()

			client := New(server.URL)
			request := client.NewRequest(http.MethodGet, "/resource", nil, nil)
			var output map[string]any
			err := client.Do(context.Background(), request, &output)
			if test.valid {
				if err != nil {
					t.Fatalf("Do() error = %v", err)
				}
				return
			}
			if !errors.Is(err, errs.New(errs.CodeInternal, "")) {
				t.Fatalf("Do() error = %v, want %q", err, errs.CodeInternal)
			}
		})
	}
}

func TestDoAcceptsNoContentOnlyWithoutOutput(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := New(server.URL)
	request := client.NewRequest(http.MethodDelete, "/resource", nil, nil)
	if err := client.Do(context.Background(), request, nil); err != nil {
		t.Fatalf("Do() error = %v", err)
	}
}

func TestStreamRequestsEventStream(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if accept := request.Header.Get("Accept"); accept != "text/event-stream" {
			t.Errorf("Accept = %q, want text/event-stream", accept)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: ready\n\n"))
	}))
	defer server.Close()

	var events []string
	err := New(server.URL+"/").Stream(context.Background(), "/events", nil, func(event string) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	if len(events) != 1 || events[0] != "ready" {
		t.Fatalf("events = %#v, want [ready]", events)
	}
}
