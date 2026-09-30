package handlers

import (
	"context"
	"net/http"

	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type EtcdConfigApplier interface {
	Apply(context.Context, string, string) (idempotencyrecord.IdempotencyResponse, error)
}
type etcdConfigOutput struct{ Body apiTypes.EtcdConfigDocument }
type etcdConfigReplacementInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.EtcdConfigReplacement
}
type etcdConfigApplyInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.EtcdConfigApplyRequest
}

func (s *Server) registerEtcdConfig() {
	huma.Register(
		s.API,
		huma.Operation{
			OperationID: "etcd.config.show",
			Method:      http.MethodGet,
			Path:        "/etcd/config",
			Summary:     "Show saved etcd configuration",
			Tags:        []string{"Host"},
		},
		s.showEtcdConfig,
	)
	huma.Register(
		s.API,
		huma.Operation{
			OperationID: "etcd.config.set",
			Method:      http.MethodPut,
			Path:        "/etcd/config",
			Summary:     "Validate and save etcd configuration without restarting",
			Tags:        []string{"Host"},
		},
		s.replaceEtcdConfig,
	)
	schema := openAPISchema[apiTypes.TaskAccepted](s.API.OpenAPI().Components.Schemas, "TaskAccepted")
	huma.Register(
		s.API,
		huma.Operation{
			OperationID:   "etcd.config.apply",
			Method:        http.MethodPost,
			Path:          "/etcd/config/apply",
			Summary:       "Apply saved etcd configuration through a native Task",
			Tags:          []string{"Host"},
			DefaultStatus: http.StatusAccepted,
			Responses:     attachMutationResponses(schema),
		},
		s.applyEtcdConfig,
	)
	s.setRoutePolicy("PUT /api/v1/etcd/config", routePolicy{body: jsonBody})
	s.setRoutePolicy("POST /api/v1/etcd/config/apply", routePolicy{body: jsonBody})
}
func (s *Server) showEtcdConfig(ctx context.Context, _ *struct{}) (*etcdConfigOutput, error) {
	if s.etcdConfig == nil {
		return nil, errs.New(errs.KindInternal, "etcd configuration store is not configured")
	}
	content, revision, required, err := s.etcdConfig.Current(ctx)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &etcdConfigOutput{
		Body: apiTypes.EtcdConfigDocument{
			Path:          s.etcdConfig.Path(),
			Content:       content,
			Revision:      revision,
			ApplyRequired: required,
		},
	}, nil
}

func (s *Server) replaceEtcdConfig(
	ctx context.Context,
	request *etcdConfigReplacementInput,
) (*etcdConfigOutput, error) {
	if s.etcdConfig == nil {
		return nil, errs.New(errs.KindInternal, "etcd configuration store is not configured")
	}
	content, revision, required, err := s.etcdConfig.Replace(
		ctx,
		request.IdempotencyKey,
		request.Body.ExpectedRevision,
		request.Body.Content,
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &etcdConfigOutput{
		Body: apiTypes.EtcdConfigDocument{
			Path:          s.etcdConfig.Path(),
			Content:       content,
			Revision:      revision,
			ApplyRequired: required,
		},
	}, nil
}
func (s *Server) applyEtcdConfig(ctx context.Context, request *etcdConfigApplyInput) (*controllerUpdateOutput, error) {
	if s.etcdConfigApplier == nil {
		return nil, errs.New(errs.KindInternal, "etcd configuration activation is not configured")
	}
	response, err := s.etcdConfigApplier.Apply(ctx, request.Body.ExpectedRevision, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &controllerUpdateOutput{
		Status:      response.Status,
		ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
				s.Logger.Error("controller: write etcd Apply acceptance", "error", err)
			}
		},
	}, nil
}
