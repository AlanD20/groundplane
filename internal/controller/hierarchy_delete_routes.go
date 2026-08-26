package controller

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type HierarchyDeletionService interface {
	Delete(context.Context, hierarchydeletion.DeleteRequest) (hierarchydeletion.TaskAccepted, error)
}

type hierarchyDeleteInput struct {
	ID             string `path:"id"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

type hierarchyDeleteOutput struct {
	Status int
	Body   struct {
		TaskID string `json:"task_id"`
	}
}

func registerHierarchyDeletionRoutes(api huma.API, service HierarchyDeletionService) {
	registerHierarchyDeletionRoute(api, service, "tenant.delete", hierarchydeletion.TenantDeleteRoute, hierarchydeletion.TargetTenant)
	registerHierarchyDeletionRoute(api, service, "project.delete", hierarchydeletion.ProjectDeleteRoute, hierarchydeletion.TargetProject)
	registerHierarchyDeletionRoute(api, service, "environment.delete", hierarchydeletion.EnvironmentDeleteRoute, hierarchydeletion.TargetEnvironment)
}

func registerHierarchyDeletionRoute(api huma.API, service HierarchyDeletionService, operationID, path string, target hierarchydeletion.TargetKind) {
	tag := "Tenant"
	if target == hierarchydeletion.TargetProject {
		tag = "Project"
	} else if target == hierarchydeletion.TargetEnvironment {
		tag = "Environment"
	}
	huma.Register(api, huma.Operation{
		OperationID:   operationID,
		Method:        http.MethodDelete,
		Path:          path,
		Summary:       "Delete a " + tag,
		Tags:          []string{tag},
		DefaultStatus: http.StatusAccepted,
	}, func(ctx context.Context, input *hierarchyDeleteInput) (*hierarchyDeleteOutput, error) {
		if service == nil {
			return nil, hierarchyDeletionUnavailable()
		}
		accepted, err := service.Delete(ctx, hierarchydeletion.DeleteRequest{TargetKind: target, TargetID: input.ID, IdempotencyKey: input.IdempotencyKey})
		if err != nil {
			return nil, err
		}
		output := &hierarchyDeleteOutput{Status: http.StatusAccepted}
		output.Body.TaskID = accepted.TaskID
		return output, nil
	})
}

func hierarchyDeletionUnavailable() error {
	return errs.New(errs.KindInternal, "hierarchy deletion service is not configured")
}
