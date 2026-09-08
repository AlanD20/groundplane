package controller

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type BackingServiceReader interface {
	GetBackingService(context.Context, string) (etcd.Versioned[etcd.BackingServiceRecord], error)
	ListBackingServices(context.Context, etcd.PageRequest) (etcd.Page[etcd.BackingServiceRecord], error)
}

type BackingServiceMutator interface {
	CreateBackingService(context.Context, apiTypes.BackingServiceCreate, string) (etcd.IdempotencyResponse, error)
	StartBackingService(context.Context, string, string) (etcd.IdempotencyResponse, error)
	StopBackingService(context.Context, string, string) (etcd.IdempotencyResponse, error)
	DestroyBackingService(context.Context, string, string) (etcd.IdempotencyResponse, error)
}

type backingServiceListInput struct {
	Limit  int    `query:"limit" required:"false"`
	Cursor string `query:"cursor" required:"false"`
}

type backingServiceShowInput struct {
	ProjectID string `path:"project_id" pattern:"^prj_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type backingServiceActionInput struct {
	ProjectID      string `path:"project_id" pattern:"^prj_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

type backingServiceCreateInput struct {
	IdempotencyKey string                        `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.BackingServiceCreate `required:"true"`
}

type backingServicePageOutput struct {
	Body apiTypes.Page[apiTypes.BackingService]
}

type backingServiceOutput struct {
	Body apiTypes.BackingService
}

func (s *Server) registerBackingServices() {
	taskAcceptedSchema := openAPISchema[apiTypes.TaskAccepted](s.API.OpenAPI().Components.Schemas, "TaskAccepted")
	createdSchema := openAPISchema[apiTypes.BackingServiceCreated](
		s.API.OpenAPI().Components.Schemas,
		"BackingServiceCreated",
	)
	huma.Register(s.API, huma.Operation{
		OperationID: "backing-service.list", Method: http.MethodGet, Path: "/backing-services",
		Summary: "List backing services", Tags: []string{"Backing service"},
		Middlewares: huma.Middlewares{s.validateBackingServiceListQuery},
	}, s.listBackingServices)
	huma.Register(s.API, huma.Operation{
		OperationID: "backing-service.create", Method: http.MethodPost, Path: "/backing-services",
		Summary: "Create a backing service", Tags: []string{"Backing service"}, DefaultStatus: http.StatusCreated,
		Middlewares: huma.Middlewares{s.rejectTaskMutationQuery},
		Responses: map[string]*huma.Response{
			strconv.Itoa(http.StatusCreated): {
				Description: http.StatusText(http.StatusCreated),
				Content:     map[string]*huma.MediaType{"application/json": {Schema: createdSchema}},
			},
		},
	}, s.createBackingService)
	s.setRoutePolicy("POST /api/v1/backing-services", routePolicy{body: jsonBody})
	huma.Register(s.API, huma.Operation{
		OperationID: "backing-service.show", Method: http.MethodGet, Path: "/backing-services/{project_id}",
		Summary: "Show a backing service", Tags: []string{"Backing service"},
	}, s.showBackingService)
	for _, action := range []struct {
		id      string
		path    string
		handler func(context.Context, *backingServiceActionInput) (*backingServiceMutationOutput, error)
	}{
		{id: "backing-service.start", path: "/backing-services/{project_id}/start", handler: s.startBackingService},
		{id: "backing-service.stop", path: "/backing-services/{project_id}/stop", handler: s.stopBackingService},
		{id: "backing-service.destroy", path: "/backing-services/{project_id}/destroy", handler: s.destroyBackingService},
	} {
		huma.Register(s.API, huma.Operation{
			OperationID: action.id, Method: http.MethodPost, Path: action.path,
			Summary: action.id, Tags: []string{"Backing service"}, DefaultStatus: http.StatusAccepted,
			Middlewares: huma.Middlewares{s.rejectTaskMutationBody, s.rejectTaskMutationQuery},
			Responses:   attachMutationResponses(taskAcceptedSchema),
		}, action.handler)
	}
}

type backingServiceMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) createBackingService(
	ctx context.Context,
	request *backingServiceCreateInput,
) (*backingServiceMutationOutput, error) {
	if s.backingServiceMutations == nil {
		return nil, errs.New(errs.KindInternal, "Backing-service mutator is not configured")
	}
	response, err := s.backingServiceMutations.CreateBackingService(ctx, request.Body, request.IdempotencyKey)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Error("controller: create backing service", slog.Any("error", err))
		}
		return nil, normalizeProjectError(err)
	}
	return &backingServiceMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			_, _ = ctx.BodyWriter().Write(response.Body)
		},
	}, nil
}

func (s *Server) startBackingService(
	ctx context.Context,
	request *backingServiceActionInput,
) (*backingServiceMutationOutput, error) {
	if s.backingServiceMutations == nil {
		return nil, errs.New(errs.KindInternal, "Backing-service mutator is not configured")
	}
	return s.mutateBackingService(ctx, request, s.backingServiceMutations.StartBackingService)
}

func (s *Server) stopBackingService(
	ctx context.Context,
	request *backingServiceActionInput,
) (*backingServiceMutationOutput, error) {
	if s.backingServiceMutations == nil {
		return nil, errs.New(errs.KindInternal, "Backing-service mutator is not configured")
	}
	return s.mutateBackingService(ctx, request, s.backingServiceMutations.StopBackingService)
}

func (s *Server) destroyBackingService(
	ctx context.Context,
	request *backingServiceActionInput,
) (*backingServiceMutationOutput, error) {
	if s.backingServiceMutations == nil {
		return nil, errs.New(errs.KindInternal, "Backing-service mutator is not configured")
	}
	return s.mutateBackingService(ctx, request, s.backingServiceMutations.DestroyBackingService)
}

func (s *Server) mutateBackingService(
	ctx context.Context,
	request *backingServiceActionInput,
	mutate func(context.Context, string, string) (etcd.IdempotencyResponse, error),
) (*backingServiceMutationOutput, error) {
	if s.backingServiceMutations == nil || mutate == nil {
		return nil, errs.New(errs.KindInternal, "Backing-service mutator is not configured")
	}
	response, err := mutate(ctx, request.ProjectID, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &backingServiceMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			_, _ = ctx.BodyWriter().Write(response.Body)
		},
	}, nil
}

func (s *Server) listBackingServices(
	ctx context.Context,
	request *backingServiceListInput,
) (*backingServicePageOutput, error) {
	if s.backingServices == nil {
		return nil, errs.New(errs.KindInternal, "Backing-service reader is not configured")
	}
	pageRequest, err := backingServiceListRequest(request.Limit, request.Cursor)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	page, err := s.backingServices.ListBackingServices(ctx, pageRequest)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response := apiTypes.Page[apiTypes.BackingService]{
		Items: make([]apiTypes.BackingService, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = backingServiceResponse(item.Record)
	}
	return &backingServicePageOutput{Body: response}, nil
}

func (s *Server) showBackingService(
	ctx context.Context,
	request *backingServiceShowInput,
) (*backingServiceOutput, error) {
	if s.backingServices == nil {
		return nil, errs.New(errs.KindInternal, "Backing-service reader is not configured")
	}
	stored, err := s.backingServices.GetBackingService(ctx, request.ProjectID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &backingServiceOutput{Body: backingServiceResponse(stored.Record)}, nil
}

func backingServiceListRequest(limit int, cursor string) (etcd.PageRequest, error) {
	if limit < 0 {
		return etcd.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Backing-service list limit must be a positive integer",
		)
	}
	return etcd.PageRequest{Limit: limit, Cursor: cursor}, nil
}

func backingServiceResponse(record etcd.BackingServiceRecord) apiTypes.BackingService {
	return apiTypes.BackingService{
		Authentication: string(record.Authentication),
		ProjectID:      record.ProjectID, EnvironmentID: record.EnvironmentID, ServiceID: record.ServiceID,
		BackingNetworkID: record.BackingNetworkID,
	}
}

func (s *Server) validateBackingServiceListQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	for key, values := range requestURL.Query() {
		if key != "limit" && key != "cursor" {
			s.writeBackingServiceProblem(ctx, "Backing-service list query is invalid")
			return
		}
		if len(values) != 1 {
			s.writeBackingServiceProblem(ctx, "Backing-service list query contains duplicate values")
			return
		}
	}
	next(ctx)
}

func (s *Server) writeBackingServiceProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write backing-service request problem", slog.Any("error", err))
	}
}
