package idempotency

import (
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	http "net/http"
	testing "testing"
)

func TestValidTaskResponseAcceptsCreatedResource(t *testing.T) {
	taskID := "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	response := IdempotencyResponse{
		Status:      http.StatusCreated,
		ContentKind: "application/json",
		Body: []byte(
			`{"backing_service":{"project_id":"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"},"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`,
		),
	}

	if !ValidTaskResponse(response, taskID) {
		t.Fatal("created Task response was rejected")
	}
}

func TestValidEntryMutationTaskResponseBindsResourceToReplayTarget(t *testing.T) {
	entryID := "ev_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	marker := IdempotencyMarker{
		Locator:      IdempotencyLocator{Method: http.MethodPost, Route: "/entries"},
		ReplayTarget: &IdempotencyReplayTarget{Kind: IdempotencyReplayTargetEntry, ID: entryID},
		Response: IdempotencyResponse{
			Status: http.StatusCreated, ContentKind: "application/json",
			Body: []byte(`{"id":"ev_01ARZ3NDEKTSV4RRFFQ69G5FAV","environment_id":"env_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
		},
	}
	if ids.Validate(ids.KindEnvEntry, entryID) != nil || !validEntryMutationTaskResponse(marker) {
		t.Fatal("Entry resource Task response was rejected")
	}
	marker.Response.Body = []byte(`{"id":"ev_01ARZ3NDEKTSV4RRFFQ69G5FAW"}`)
	if validEntryMutationTaskResponse(marker) {
		t.Fatal("Entry resource Task response accepted a mismatched replay target")
	}
}

func TestValidTaskResponseAcceptsUpdatedResource(t *testing.T) {
	taskID := "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	response := IdempotencyResponse{
		Status:      http.StatusOK,
		ContentKind: "application/json",
		Body:        []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV","volume":{"slug":"data"}}`),
	}

	if !ValidTaskResponse(response, taskID) {
		t.Fatal("updated Task response was rejected")
	}
}
