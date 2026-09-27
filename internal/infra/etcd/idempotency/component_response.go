package idempotency

import (
	"bytes"
	"encoding/json"
	"net/http"
)

// Component configuration returns the saved resource and its reconciliation
// Task. Bind that response to the same Task before publishing replay evidence.
func validComponentConfigTaskResponse(marker IdempotencyMarker) bool {
	if marker.Locator.ScopeKind != IdempotencyScopePlatform || marker.Response.Status != http.StatusOK ||
		marker.Response.ContentKind != "application/json" {
		return false
	}
	var body struct {
		Resource        json.RawMessage `json:"resource"`
		ReconcileTaskID string          `json:"reconcile_task_id"`
	}
	decoder := json.NewDecoder(bytes.NewReader(marker.Response.Body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&body) != nil || body.ReconcileTaskID != marker.TaskID ||
		len(body.Resource) == 0 || body.Resource[0] != '{' {
		return false
	}
	var compact bytes.Buffer
	return json.Compact(&compact, marker.Response.Body) == nil && bytes.Equal(marker.Response.Body, compact.Bytes())
}
