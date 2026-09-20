package handlers

import (
	"context"
	"errors"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"io"
	"net/http"
	"reflect"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type serviceLifecycleInput struct {
	ID             string `path:"id" pattern:"^svc_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

func (s *Server) registerServiceLifecycleRoutes() {
	taskAcceptedSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.TaskAccepted](), true, "TaskAccepted",
	)
	for _, operation := range []struct {
		id      string
		path    string
		summary string
		handler func(context.Context, *serviceLifecycleInput) (*serviceMutationOutput, error)
	}{
		{id: "service.start", path: "/services/{id}/start", summary: "Start a service", handler: s.startService},
		{id: "service.stop", path: "/services/{id}/stop", summary: "Stop a service", handler: s.stopService},
		{id: "service.destroy", path: "/services/{id}/destroy", summary: "Destroy service runtime", handler: s.destroyService},
	} {
		huma.Register(s.API, huma.Operation{
			OperationID: operation.id, Method: http.MethodPost, Path: operation.path,
			Summary: operation.summary, Tags: []string{"Service"}, DefaultStatus: http.StatusAccepted,
			Middlewares: huma.Middlewares{s.rejectServiceLifecycleBody, s.rejectServiceLifecycleQuery},
			Responses:   attachMutationResponses(taskAcceptedSchema),
		}, operation.handler)
	}
}

func (s *Server) startService(
	ctx context.Context,
	input *serviceLifecycleInput,
) (*serviceMutationOutput, error) {
	return s.runServiceLifecycle(ctx, input, "start")
}

func (s *Server) stopService(
	ctx context.Context,
	input *serviceLifecycleInput,
) (*serviceMutationOutput, error) {
	return s.runServiceLifecycle(ctx, input, "stop")
}

func (s *Server) destroyService(
	ctx context.Context,
	input *serviceLifecycleInput,
) (*serviceMutationOutput, error) {
	return s.runServiceLifecycle(ctx, input, "destroy")
}

func (s *Server) runServiceLifecycle(
	ctx context.Context,
	input *serviceLifecycleInput,
	action string,
) (*serviceMutationOutput, error) {
	if s.serviceMutations == nil {
		return nil, errs.New(errs.KindInternal, "Service mutator is not configured")
	}
	result, err := serviceLifecycleMutation(ctx, s.serviceMutations, input.ID, input.IdempotencyKey, action)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.serviceMutationResponse(result), nil
}

func serviceLifecycleMutation(
	ctx context.Context,
	mutator ServiceMutator,
	serviceID string,
	idempotencyKey string,
	action string,
) (idempotencyrecord.IdempotencyResponse, error) {
	switch action {
	case "start":
		return mutator.StartService(ctx, serviceID, idempotencyKey)
	case "stop":
		return mutator.StopService(ctx, serviceID, idempotencyKey)
	case "destroy":
		return mutator.DestroyService(ctx, serviceID, idempotencyKey)
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service lifecycle action is invalid")
	}
}

func (s *Server) rejectServiceLifecycleBody(ctx huma.Context, next func(huma.Context)) {
	var probe [1]byte
	count, err := ctx.BodyReader().Read(probe[:])
	if count != 0 || (err != nil && !errors.Is(err, io.EOF)) {
		s.writeServiceProblem(ctx, "Service lifecycle body is not allowed")
		return
	}
	next(ctx)
}

func (s *Server) rejectServiceLifecycleQuery(ctx huma.Context, next func(huma.Context)) {
	if ctx.URL().RawQuery != "" {
		s.writeServiceProblem(ctx, "Service lifecycle query parameters are not allowed")
		return
	}
	next(ctx)
}
