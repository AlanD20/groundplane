package apiclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Rationale: CLI deletion must use the generated Environment operation with a
// protected idempotency key and render the Controller's accepted Task identity.
// QA: OWN-05/06; deletion request and Task identity only, not descendant removal.
func TestDeleteEnvironmentUsesGeneratedAcceptedTaskOperation(t *testing.T) {
	t.Parallel()
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodDelete || request.URL.Path != "/api/v1/environments/"+environmentID {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		if key := request.Header.Get("Idempotency-Key"); len(key) < 16 {
			t.Errorf("Idempotency-Key = %q", key)
		}
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusAccepted)
		_, _ = response.Write([]byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`))
	}))
	defer server.Close()

	accepted, err := New(server.URL).DeleteEnvironment(context.Background(), environmentID)
	if err != nil {
		t.Fatalf("DeleteEnvironment() error = %v", err)
	}
	if accepted.TaskID != "task_01ARZ3NDEKTSV4RRFFQ69G5FAV" {
		t.Fatalf("DeleteEnvironment().TaskID = %q", accepted.TaskID)
	}
}
