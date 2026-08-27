package apiclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/oklog/ulid/v2"
)

// Rationale: backup rotate must call the generated operation with its generated
// header contract and decode the generated TaskAccepted response.
func TestRotateBackupKeyUsesGeneratedOperation(t *testing.T) {
	taskID := "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost ||
			request.URL.Path != "/api/v1/environments/env_01ARZ3NDEKTSV4RRFFQ69G5FAV/rotate-key" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		if _, err := ulid.ParseStrict(request.Header.Get(idempotencyKeyHeader)); err != nil {
			t.Fatalf("Idempotency-Key = %q: %v", request.Header.Get(idempotencyKeyHeader), err)
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(writer).Encode(apiTypes.TaskAccepted{TaskID: taskID}); err != nil {
			t.Fatal(err)
		}
	}))
	defer server.Close()

	accepted, err := New(server.URL).RotateBackupKey(
		context.Background(),
		"env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
	)
	if err != nil {
		t.Fatalf("RotateBackupKey() error = %v", err)
	}
	if accepted.TaskID != taskID {
		t.Fatalf("TaskID = %q, want %q", accepted.TaskID, taskID)
	}
}
