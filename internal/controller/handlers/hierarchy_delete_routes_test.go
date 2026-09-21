package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
)

type recordingHierarchyDeletionService struct {
	request hierarchydeletion.DeleteRequest
}

func (service *recordingHierarchyDeletionService) Delete(
	_ context.Context,
	request hierarchydeletion.DeleteRequest,
) (hierarchydeletion.TaskAccepted, error) {
	service.request = request
	return hierarchydeletion.TaskAccepted{TaskID: "task_01K3D7R40G0000000000000000"}, nil
}

func TestHierarchyDeletionRoutesReplacePlaceholders(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		path   string
		id     string
		target hierarchydeletion.TargetKind
	}{
		{name: "Tenant", path: "/api/v1/tenants/tnt_01K3D7R40G0000000000000001", id: "tnt_01K3D7R40G0000000000000001", target: hierarchydeletion.TargetTenant},
		{name: "Project", path: "/api/v1/projects/prj_01K3D7R40G0000000000000002", id: "prj_01K3D7R40G0000000000000002", target: hierarchydeletion.TargetProject},
		{name: "Environment", path: "/api/v1/environments/env_01K3D7R40G0000000000000003", id: "env_01K3D7R40G0000000000000003", target: hierarchydeletion.TargetEnvironment},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service := &recordingHierarchyDeletionService{}
			server := New(nil, nil, Options{HierarchyDeletions: service})
			request := httptest.NewRequest(http.MethodDelete, test.path, nil)
			request.Header.Set("Idempotency-Key", "hierarchy-delete-key-0001")
			response := httptest.NewRecorder()
			server.HTTPHandler().ServeHTTP(response, request)
			if response.Code != http.StatusAccepted {
				t.Fatalf("status/body = %d/%s", response.Code, response.Body.String())
			}
			var body struct {
				TaskID string `json:"task_id"`
			}
			if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.TaskID != "task_01K3D7R40G0000000000000000" || service.request.TargetKind != test.target ||
				service.request.TargetID != test.id ||
				service.request.IdempotencyKey != "hierarchy-delete-key-0001" {
				t.Fatalf("response/request = %#v/%#v", body, service.request)
			}
		})
	}
}
