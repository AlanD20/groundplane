package apiclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/oklog/ulid/v2"
)

// Rationale: ordinary JSON calls must preserve the canonical Accept header and
// normalized BaseURL behavior while using the typed request boundary.
func TestDoNormalizesBaseURLAndRequestsJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			if request.URL.Path != "/api/v1/tasks" {
				t.Errorf("path = %q, want %q", request.URL.Path, "/api/v1/tasks")
			}
			if accept := request.Header.Get("Accept"); accept != "application/json" {
				t.Errorf("Accept = %q, want application/json", accept)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"id": "task-1"})
		}),
	)
	defer server.Close()

	var response map[string]string
	client := New(server.URL + "/")
	request := client.NewRequest(http.MethodGet, "/api/v1/tasks", nil, nil, http.StatusOK)
	err := client.Do(context.Background(), request, &response)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if response["id"] != "task-1" {
		t.Fatalf("response id = %q, want task-1", response["id"])
	}
}

// Rationale: every mutation verb must send exactly one key and preserve that
// same key when the same human-intent request is retried.
func TestMutationRequestsCarryOneReusableRawULID(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		method string
		status int
	}{
		{method: http.MethodPost, status: http.StatusCreated},
		{method: http.MethodPut, status: http.StatusOK},
		{method: http.MethodPatch, status: http.StatusOK},
		{method: http.MethodDelete, status: http.StatusNoContent},
	} {
		method := test.method
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			var keys [][]string
			server := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
					keys = append(keys, request.Header.Values(idempotencyKeyHeader))
					w.WriteHeader(test.status)
				}),
			)
			defer server.Close()

			client := New(server.URL)
			request := client.NewRequest(
				method,
				"/resource",
				nil,
				map[string]string{"action": "run"},
				test.status,
			)
			for range 2 {
				if err := client.Do(context.Background(), request, nil); err != nil {
					t.Fatalf("Do() error = %v", err)
				}
			}

			if len(keys) != 2 || len(keys[0]) != 1 || len(keys[1]) != 1 ||
				keys[0][0] != keys[1][0] {
				t.Fatalf("Idempotency-Key values = %q, want one identical key per request", keys)
			}
			if len(keys[0][0]) != 26 {
				t.Fatalf("Idempotency-Key length = %d, want raw 26-character ULID", len(keys[0][0]))
			}
			if _, err := ulid.ParseStrict(keys[0][0]); err != nil {
				t.Fatalf("Idempotency-Key = %q, want raw ULID: %v", keys[0][0], err)
			}
		})
	}
}

// Rationale: generated requests must attach keys to all and only the four
// accepted human mutation methods.
func TestRequestIdempotencyKeyContract(t *testing.T) {
	t.Parallel()

	methods := map[string]int{
		http.MethodPost: http.StatusCreated, http.MethodPut: http.StatusOK,
		http.MethodPatch: http.StatusOK, http.MethodDelete: http.StatusNoContent,
	}
	for method, status := range methods {
		request := New(
			"http://invalid",
		).NewRequest(strings.ToLower(method), "/resource", nil, nil, status)
		if request.Method != method {
			t.Errorf(
				"NewRequest(%q) method = %q, want %q",
				strings.ToLower(method),
				request.Method,
				method,
			)
		}
		if !idempotencyKeyPattern.MatchString(request.IdempotencyKey) {
			t.Errorf("%s key = %q, want 16..128 allowed characters", method, request.IdempotencyKey)
		}
	}

	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodOptions} {
		request := New("http://invalid").NewRequest(method, "/resource", nil, nil, http.StatusOK)
		if request.IdempotencyKey != "" {
			t.Errorf("%s key = %q, want empty", method, request.IdempotencyKey)
		}
	}
}

// Rationale: key validation must enforce both ASCII grammar boundaries for
// every mutation verb and reject any key on safe methods before transport.
func TestDoRejectsInvalidIdempotencyKeysAsInternal(t *testing.T) {
	t.Parallel()

	validKeys := []string{
		strings.Repeat("a", 16),
		strings.Repeat("Z", 128),
		"A0._:-A0._:-A0._:-",
	}
	client := New("http://127.0.0.1:1")
	mutationMethods := []string{
		http.MethodPost,
		http.MethodPut,
		http.MethodPatch,
		http.MethodDelete,
	}
	for _, method := range mutationMethods {
		for _, key := range validKeys {
			if err := validateIdempotencyKey(
				Request{Method: method, Path: "/resource", IdempotencyKey: key},
			); err != nil {
				t.Errorf("validateIdempotencyKey(%s, %q) error = %v", method, key, err)
			}
		}
		for _, key := range []string{
			"",
			strings.Repeat("a", 15),
			strings.Repeat("a", 129),
			"invalid key spaces",
			strings.Repeat("é", 16),
		} {
			request := Request{Method: method, Path: "/resource", IdempotencyKey: key}
			err := client.Do(context.Background(), request, nil)
			if !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Errorf("Do(%s, key=%q) error = %v, want %q", method, key, err, errs.CodeInternal)
			}
		}
	}
	request := Request{Method: "post", Path: "/resource"}
	if err := client.Do(
		context.Background(),
		request,
		nil,
	); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Errorf("Do(lowercase POST without key) error = %v, want %q", err, errs.CodeInternal)
	}
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodOptions} {
		request := Request{
			Method:         method,
			Path:           "/resource",
			IdempotencyKey: strings.Repeat("a", 16),
		}
		if err := client.Do(
			context.Background(),
			request,
			nil,
		); !errors.Is(
			err,
			errs.New(errs.KindInternal, ""),
		) {
			t.Errorf("Do(%s with key) error = %v, want %q", method, err, errs.CodeInternal)
		}
	}
}

// Rationale: an output target requires one complete JSON document; transport
// truncation, ambiguity, or non-JSON success bodies must fail canonically.
func TestDoRequiresExactlyOneJSONDocumentForOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status int
		body   string
		valid  bool
	}{
		{name: "one document", status: http.StatusOK, body: `{"ok":true}`, valid: true},
		{
			name:   "one document and trailing whitespace",
			status: http.StatusOK,
			body:   "{\"ok\":true}\n\t  ",
			valid:  true,
		},
		{name: "empty", status: http.StatusOK},
		{name: "no content", status: http.StatusNoContent},
		{name: "malformed", status: http.StatusOK, body: `{`},
		{name: "multiple", status: http.StatusOK, body: `{}` + "\n" + `{}`},
		{name: "trailing malformed", status: http.StatusOK, body: `{} trailing`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
					w.WriteHeader(test.status)
					_, _ = w.Write([]byte(test.body))
				}),
			)
			defer server.Close()

			client := New(server.URL)
			request := client.NewRequest(http.MethodGet, "/resource", nil, nil, http.StatusOK)
			var output map[string]any
			err := client.Do(context.Background(), request, &output)
			if test.valid {
				if err != nil {
					t.Fatalf("Do() error = %v", err)
				}
				return
			}
			if !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("Do() error = %v, want %q", err, errs.CodeInternal)
			}
		})
	}
}

// Rationale: 204 is a valid success only when the caller explicitly expects
// no representation.
func TestDoAcceptsNoContentOnlyWithoutOutput(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}),
	)
	defer server.Close()

	client := New(server.URL)
	request := client.NewRequest(http.MethodDelete, "/resource", nil, nil, http.StatusNoContent)
	if err := client.Do(context.Background(), request, nil); err != nil {
		t.Fatalf("Do() error = %v", err)
	}
}

func TestDoRejectsUnexpectedSuccessfulStatus(t *testing.T) {
	// Rationale: accepting any 2xx would collapse create, update, Task, and
	// bodyless-delete contracts into transport success and hide API drift.
	t.Parallel()

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"task_id":"task_1"}`))
		}),
	)
	defer server.Close()

	client := New(server.URL)
	request := client.NewRequest(http.MethodGet, "/resource", nil, nil, http.StatusOK)
	var output map[string]any
	if err := client.Do(
		context.Background(),
		request,
		&output,
	); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("Do() error = %v, want %q", err, errs.CodeInternal)
	}
}

// Rationale: callers must opt into exactly the semantic status allowed by
// their method; broad "any 2xx" handling would hide surface drift.
func TestValidExpectedStatusUsesHumanContractMatrix(t *testing.T) {
	t.Parallel()

	accepted := map[string][]int{
		http.MethodGet:    {http.StatusOK},
		http.MethodPost:   {http.StatusOK, http.StatusCreated, http.StatusAccepted},
		http.MethodPut:    {http.StatusOK, http.StatusAccepted},
		http.MethodPatch:  {http.StatusOK},
		http.MethodDelete: {http.StatusAccepted, http.StatusNoContent},
	}
	for method, statuses := range accepted {
		for _, status := range []int{http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent} {
			want := slices.Contains(statuses, status)
			if got := validExpectedStatus(method, status); got != want {
				t.Errorf("validExpectedStatus(%s, %d) = %t, want %t", method, status, got, want)
			}
		}
	}
}

// Rationale: event consumers require the SSE Accept contract rather than the
// ordinary JSON transport representation.
func TestStreamRequestsEventStream(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			if accept := request.Header.Get("Accept"); accept != "text/event-stream" {
				t.Errorf("Accept = %q, want text/event-stream", accept)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: ready\n\n"))
		}),
	)
	defer server.Close()

	var events []string
	err := New(
		server.URL+"/",
	).Stream(context.Background(), "/events", nil, func(event string) error {
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

// Rationale: accepting a successful non-SSE response would feed arbitrary
// bytes into the event parser and silently violate the streaming contract.
func TestStreamRejectsNonEventStreamResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ready"}`))
		}),
	)
	defer server.Close()

	err := New(server.URL).Stream(context.Background(), "/events", nil, func(string) error {
		t.Fatal("onEvent called for a non-SSE response")
		return nil
	})
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("Stream() error = %v, want %q", err, errs.CodeInternal)
	}
}
