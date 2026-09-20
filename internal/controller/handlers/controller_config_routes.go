package handlers

import (
	"context"
	"net/http"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type ControllerConfigStore interface {
	Path() string
	Current(context.Context) (content string, revision string, restartRequired bool, err error)
	Replace(
		ctx context.Context,
		idempotencyKey string,
		expectedRevision string,
		content string,
	) (current string, revision string, restartRequired bool, err error)
}

type controllerConfigReplacementInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.ControllerConfigReplacement
}

type controllerConfigOutput struct {
	Body apiTypes.ControllerConfigDocument
}

func (s *Server) registerControllerConfig() {
	huma.Register(s.API, huma.Operation{
		OperationID: "controller.config.show", Method: http.MethodGet, Path: "/controller/config",
		Summary: "Show the exact Controller startup configuration", Tags: []string{"Controller"},
	}, s.showControllerConfig)
	huma.Register(s.API, huma.Operation{
		OperationID: "controller.config.set", Method: http.MethodPut, Path: "/controller/config",
		Summary: "Validate and replace the Controller startup configuration", Tags: []string{"Controller"},
	}, s.replaceControllerConfig)
	s.setRoutePolicy("PUT /api/v1/controller/config", routePolicy{body: jsonBody})
}

func (s *Server) showControllerConfig(
	ctx context.Context,
	_ *struct{},
) (*controllerConfigOutput, error) {
	if s.controllerConfig == nil {
		return nil, errs.New(errs.KindInternal, "controller config store is not configured")
	}
	content, revision, restartRequired, err := s.controllerConfig.Current(ctx)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.controllerConfigResponse(content, revision, restartRequired), nil
}

func (s *Server) replaceControllerConfig(
	ctx context.Context,
	request *controllerConfigReplacementInput,
) (*controllerConfigOutput, error) {
	if s.controllerConfig == nil {
		return nil, errs.New(errs.KindInternal, "controller config store is not configured")
	}
	content, revision, restartRequired, err := s.controllerConfig.Replace(
		ctx,
		request.IdempotencyKey,
		request.Body.ExpectedRevision,
		request.Body.Content,
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.controllerConfigResponse(content, revision, restartRequired), nil
}

func (s *Server) controllerConfigResponse(
	content string,
	revision string,
	restartRequired bool,
) *controllerConfigOutput {
	return &controllerConfigOutput{Body: apiTypes.ControllerConfigDocument{
		Path:            s.controllerConfig.Path(),
		Content:         content,
		Revision:        revision,
		RestartRequired: restartRequired,
	}}
}
