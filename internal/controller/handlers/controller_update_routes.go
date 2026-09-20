package handlers

import (
	"context"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/jcs"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type ControllerUpdater interface {
	UpdateController(context.Context, string, string) (idempotencyrecord.IdempotencyResponse, error)
}

type controllerUpdateInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.ControllerUpdateRequest
}

type controllerUpdateOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerControllerUpdate() {
	schema := openAPISchema[apiTypes.TaskAccepted](s.API.OpenAPI().Components.Schemas, "TaskAccepted")
	huma.Register(s.API, huma.Operation{
		OperationID: "controller.update", Method: http.MethodPost, Path: "/controller/update",
		Summary: "Update the native Controller from a staged immutable release", Tags: []string{"Controller"},
		DefaultStatus: http.StatusAccepted, Responses: attachMutationResponses(schema),
		Middlewares: huma.Middlewares{s.rejectControllerUpdateQuery},
	}, s.updateController)
	s.setRoutePolicy(
		"POST /api/v1/controller/update",
		routePolicy{body: jsonBody, validateJSON: validateControllerUpdateJSON},
	)
}

func (s *Server) updateController(
	ctx context.Context,
	request *controllerUpdateInput,
) (*controllerUpdateOutput, error) {
	if s.controllerUpdates == nil {
		return nil, errs.New(errs.KindInternal, "Controller update service is not configured")
	}
	response, err := s.controllerUpdates.UpdateController(ctx, request.Body.Release, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &controllerUpdateOutput{
		Status:      response.Status,
		ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
				s.Logger.Error("controller: write native update acceptance", "error", err)
			}
		},
	}, nil
}

func (s *Server) rejectControllerUpdateQuery(ctx huma.Context, next func(huma.Context)) {
	if ctx.URL().RawQuery != "" {
		if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, "Controller update does not accept query parameters"); err != nil &&
			s.Logger != nil {
			s.Logger.Error("controller: write update request rejection", "error", err)
		}
		return
	}
	next(ctx)
}

func validateControllerUpdateJSON(raw []byte) error {
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return err
	}
	_, err = jcs.Decode[apiTypes.ControllerUpdateRequest](canonical)
	return err
}
