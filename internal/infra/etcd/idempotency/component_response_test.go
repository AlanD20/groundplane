package idempotency

import (
	"bytes"
	"net/http"
	"testing"
	"time"
)

// Rationale: a settings save must persist its original result for replay, but
// cannot publish a response naming a different Task or a second response shape.
func TestComponentConfigTaskMarkerPreservesBoundReplayResponse(t *testing.T) {
	marker := testDirectMarker()
	marker.Kind, marker.State = IdempotencyMarkerTask, IdempotencyMarkerPending
	marker.TaskID = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	marker.TerminalAt, marker.RetainUntil = time.Time{}, time.Time{}
	marker.Locator.ScopeKind, marker.Locator.ScopeID = IdempotencyScopePlatform, "-"
	marker.Locator.Method, marker.Locator.Route = http.MethodPut, "/components/{id}/config"
	marker.Response.Body = []byte(
		`{"resource":{"upstream_auto":true},"reconcile_task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`,
	)
	encoded, err := EncodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeIdempotencyMarker(encoded, marker.Locator)
	if err != nil || !bytes.Equal(decoded.Response.Body, marker.Response.Body) || decoded.TaskID != marker.TaskID {
		t.Fatalf("settings replay did not retain the accepted result: %v", err)
	}
	for _, change := range []struct {
		name  string
		apply func(*IdempotencyMarker)
	}{
		{"different task", func(m *IdempotencyMarker) { m.TaskID = "task_01ARZ3NDEKTSV4RRFFQ69G5FAW" }},
		{"different endpoint", func(m *IdempotencyMarker) { m.Locator.Route = "/components/{id}/update" }},
		{"unaccepted status", func(m *IdempotencyMarker) { m.Response.Status = http.StatusAccepted }},
		{"missing task", func(m *IdempotencyMarker) { m.Response.Body = []byte(`{"resource":{},"reconcile_task_id":null}`) }},
		{"alternate response", func(m *IdempotencyMarker) {
			m.Response.Body = []byte(`{"resource":{},"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`)
		}},
	} {
		t.Run(change.name, func(t *testing.T) {
			candidate := CloneIdempotencyMarker(marker)
			change.apply(&candidate)
			if _, err := EncodeIdempotencyMarker(candidate); err == nil {
				t.Fatal("published invalid settings replay evidence")
			}
		})
	}
}
