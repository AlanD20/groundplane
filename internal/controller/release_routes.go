package controller

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type ReleaseReader interface {
	Get(context.Context, string) (etcd.ReleaseView, error)
	List(context.Context, etcd.ReleasePageRequest) (etcd.ReleasePage, error)
}

type releaseListInput struct {
	EnvironmentID string `query:"environment_id" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	ServiceID     string `query:"service_id" required:"false" pattern:"^svc_[0-9A-HJKMNP-TV-Z]{26}$"`
	Limit         int    `query:"limit" required:"false" minimum:"1" maximum:"200"`
	Cursor        string `query:"cursor" required:"false"`
}

type releaseShowInput struct {
	ID string `path:"id" pattern:"^dep_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type releaseServiceDeployInput struct {
	ID             string `path:"id" pattern:"^svc_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.DeployRequest
}

type releaseServiceRollbackInput struct {
	ID             string `path:"id" pattern:"^svc_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.RollbackRequest
}

type releaseOutput struct{ Body apiTypes.ReleaseDetail }
type releasePageOutput struct {
	Body apiTypes.Page[apiTypes.ReleaseSummary]
}

func (s *Server) registerReleases() {
	taskSchema := openAPISchema[apiTypes.ReleaseTaskAccepted](s.API.OpenAPI().Components.Schemas, "ReleaseTaskAccepted")
	huma.Register(s.API, huma.Operation{
		OperationID: "release.list", Method: http.MethodGet, Path: "/releases",
		Summary: "List releases", Tags: []string{"Release"},
	}, s.listReleases)
	huma.Register(s.API, huma.Operation{
		OperationID: "release.show", Method: http.MethodGet, Path: "/releases/{id}",
		Summary: "Show a release", Tags: []string{"Release"},
	}, s.showRelease)
	huma.Register(s.API, huma.Operation{
		OperationID: "service.deploy", Method: http.MethodPost, Path: "/services/{id}/deploy",
		Summary: "Deploy a service", Tags: []string{"Service"}, DefaultStatus: http.StatusAccepted,
		Responses: map[string]*huma.Response{
			"202": {Description: http.StatusText(http.StatusAccepted), Content: map[string]*huma.MediaType{
				"application/json": {Schema: taskSchema},
			}},
		},
	}, s.deployService)
	huma.Register(s.API, huma.Operation{
		OperationID: "service.rollback", Method: http.MethodPost, Path: "/services/{id}/rollback",
		Summary: "Roll back a service", Tags: []string{"Service"}, DefaultStatus: http.StatusAccepted,
		Responses: map[string]*huma.Response{
			"202": {Description: http.StatusText(http.StatusAccepted), Content: map[string]*huma.MediaType{
				"application/json": {Schema: taskSchema},
			}},
		},
	}, s.rollbackService)
	s.setRoutePolicy("POST /api/v1/services/{id}/deploy", routePolicy{body: jsonBody})
	s.setRoutePolicy("POST /api/v1/services/{id}/rollback", routePolicy{body: jsonBody})
}

func (s *Server) listReleases(ctx context.Context, input *releaseListInput) (*releasePageOutput, error) {
	if s.releases == nil {
		return nil, errs.New(errs.KindInternal, "release reader is not configured")
	}
	limit := input.Limit
	if limit == 0 {
		limit = 50
	}
	page, err := s.releases.List(ctx, etcd.ReleasePageRequest{
		EnvironmentID: input.EnvironmentID, ServiceID: input.ServiceID, Limit: limit, Cursor: input.Cursor,
	})
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &releasePageOutput{Body: releasePageResponse(page)}, nil
}

func releasePageResponse(page etcd.ReleasePage) apiTypes.Page[apiTypes.ReleaseSummary] {
	items := make([]apiTypes.ReleaseSummary, len(page.Items))
	for index, item := range page.Items {
		items[index] = releaseSummary(item)
	}
	return apiTypes.Page[apiTypes.ReleaseSummary]{
		Items: items, NextCursor: page.NextCursor, Revision: page.Revision,
	}
}

func (s *Server) showRelease(ctx context.Context, input *releaseShowInput) (*releaseOutput, error) {
	if s.releases == nil {
		return nil, errs.New(errs.KindInternal, "release reader is not configured")
	}
	view, err := s.releases.Get(ctx, input.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &releaseOutput{Body: releaseDetail(view)}, nil
}

func (s *Server) deployService(
	ctx context.Context,
	input *releaseServiceDeployInput,
) (*releaseGroupMutationOutput, error) {
	if s.releaseOperations == nil {
		return nil, errs.New(errs.KindInternal, "release operator is not configured")
	}
	response, err := s.releaseOperations.DeployService(
		ctx,
		input.ID,
		domain.ServiceDeployInput{
			Tag:       input.Body.Tag,
			Strategy:  domain.Strategy(input.Body.Strategy),
			OnFailure: domain.OnFailure(input.Body.OnFailure),
		},
		input.IdempotencyKey,
	)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Error(
				"controller: deploy Service",
				slog.String("service_id", input.ID),
				slog.Any("error", err),
			)
		}
		return nil, normalizeProjectError(err)
	}
	return s.releaseGroupMutationResponse(response), nil
}

func (s *Server) rollbackService(
	ctx context.Context,
	input *releaseServiceRollbackInput,
) (*releaseGroupMutationOutput, error) {
	if s.releaseOperations == nil {
		return nil, errs.New(errs.KindInternal, "release operator is not configured")
	}
	response, err := s.releaseOperations.RollbackService(
		ctx,
		input.ID,
		domain.ServiceRollbackInput{Tag: input.Body.Tag},
		input.IdempotencyKey,
	)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Error(
				"controller: roll back Service",
				slog.String("service_id", input.ID),
				slog.Any("error", err),
			)
		}
		return nil, normalizeProjectError(err)
	}
	return s.releaseGroupMutationResponse(response), nil
}

func releaseSummary(view etcd.ReleaseView) apiTypes.ReleaseSummary {
	completedAt := (*string)(nil)
	if view.Terminal != nil {
		formatted := view.Terminal.CompletedAt.Format(time.RFC3339Nano)
		completedAt = &formatted
	}
	return apiTypes.ReleaseSummary{
		ID: view.Intent.ID, EnvironmentID: view.Intent.EnvironmentID, ServiceID: view.Intent.ServiceID,
		OperationID: view.Intent.OperationID, OperationKind: string(view.Intent.OperationKind),
		GroupOperationID: view.Intent.GroupOperationID, GroupMemberOrdinal: view.Intent.GroupMemberOrdinal,
		Image: view.Intent.CandidateWorkload.RequestedReference, Tag: view.Intent.Tag,
		Strategy: string(view.Intent.Strategy), Slot: string(view.Intent.Slot),
		OnFailure: apiTypes.OnFailure(view.Intent.OnFailure), State: apiTypes.ReleaseState(view.Checkpoint.State),
		CreatedAt: view.Intent.CreatedAt.Format(time.RFC3339Nano), CompletedAt: completedAt,
		Serving:           view.Projection.ServingReleaseID == view.Intent.ID,
		CurrentSuccessful: view.Projection.CurrentSuccessfulReleaseID == view.Intent.ID,
	}
}

func releaseDetail(view etcd.ReleaseView) apiTypes.ReleaseDetail {
	attempts := make([]apiTypes.ReleaseAttempt, len(view.Attempts))
	for index, attempt := range view.Attempts {
		attempts[index] = apiTypes.ReleaseAttempt{
			ID: attempt.ID, TaskID: attempt.TaskID, RetryOf: attempt.RetryOf,
			StartedAt: attempt.StartedAt.Format(time.RFC3339Nano),
		}
	}
	var material *apiTypes.ReleaseRollbackMaterial
	if view.Retention != nil {
		material = &apiTypes.ReleaseRollbackMaterial{
			Status: string(view.Retention.Status), Revision: view.Retention.Revision,
		}
		if view.Retention.ExpiredAt != nil {
			formatted := view.Retention.ExpiredAt.Format(time.RFC3339Nano)
			material.ExpiredAt = &formatted
		}
	}
	return apiTypes.ReleaseDetail{
		ReleaseSummary: releaseSummary(view), RollbackSourceReleaseID: view.Intent.RollbackSourceReleaseID,
		PriorServingReleaseID:    view.Intent.PriorServingReleaseID,
		PriorSuccessfulReleaseID: view.Intent.PriorSuccessfulReleaseID,
		RenderInputDigest:        view.Intent.RenderInputDigest, OriginatingTaskID: view.Intent.OriginatingTaskID,
		Attempts: attempts, RollbackMaterial: material,
	}
}
