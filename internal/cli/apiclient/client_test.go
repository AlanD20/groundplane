package apiclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
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
	err := New(server.URL+"/").Do(context.Background(), http.MethodGet, "/api/v1/tasks", nil, nil, &response)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if response["id"] != "task-1" {
		t.Fatalf("response id = %q, want task-1", response["id"])
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
