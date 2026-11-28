package handlers

import (
	"context"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/controller/remotehost"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/danielgtaylor/huma/v2"
)

type RemoteHostOperator interface {
	ListRemoteHosts(context.Context, keyvalue.PageRequest) (api.Page[api.RemoteHost], error)
	ShowRemoteHost(context.Context, string) (api.RemoteHost, error)
	CreateRemoteHost(context.Context, string, string) (idempotency.IdempotencyResponse, error)
	StageRemoteHostEnrollment(
		context.Context,
		string,
		api.RemoteHostEnrollmentRequest,
		string,
	) (idempotency.IdempotencyResponse, error)
	RevokeRemoteHostEnrollment(context.Context, string, uint64, string) (idempotency.IdempotencyResponse, error)
}

type RemoteHostPath struct {
	ID string `path:"id" pattern:"^host_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type RemoteHostWrite struct {
	Key string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

type remoteHostListInput struct {
	Limit  int    `query:"limit" minimum:"1" maximum:"200"`
	Cursor string `query:"cursor" maxLength:"4096"`
}

type remoteHostCreateInput struct {
	RemoteHostWrite
	Body api.RemoteHostCreateRequest
}

type remoteHostStageInput struct {
	RemoteHostPath
	RemoteHostWrite
	Body api.RemoteHostEnrollmentRequest
}

type remoteHostRevokeInput struct {
	RemoteHostPath
	RemoteHostWrite
	Generation uint64 `query:"generation" minimum:"1" required:"true"`
}

type remoteHostOutput struct{ Body api.RemoteHost }
type remoteHostListOutput struct{ Body api.Page[api.RemoteHost] }

type remoteHostRoutes struct {
	server   *Server
	operator RemoteHostOperator
}

func (s *Server) registerRemoteHosts(operator RemoteHostOperator) {
	routes := remoteHostRoutes{server: s, operator: operator}
	host := remotehost.OperatorRoute + "/{id}"
	huma.Register(s.API, routes.operation("remote-host.list", http.MethodGet,
		remotehost.OperatorRoute, "List remote Hosts"), routes.list)
	huma.Register(s.API, routes.operation("remote-host.show", http.MethodGet, host, "Show a remote Host"), routes.show)

	create := routes.operation(
		"remote-host.create",
		http.MethodPost,
		remotehost.OperatorRoute,
		"Register a remote Host",
	)
	stage := routes.operation("remote-host.stage-enrollment", http.MethodPost, remotehost.EnrollmentRoute,
		"Stage an Agent certificate from a signed CSR")
	revoke := routes.operation("remote-host.revoke-enrollment", http.MethodDelete, remotehost.EnrollmentRoute,
		"Revoke a remote Host's exact Agent generation")
	registry := s.API.OpenAPI().Components.Schemas
	huma.Register(s.API, remoteHostMutation(create, http.StatusCreated,
		openAPISchema[api.RemoteHost](registry, "RemoteHost")), routes.create)
	huma.Register(s.API, remoteHostMutation(stage, http.StatusOK,
		openAPISchema[api.RemoteHostEnrollment](registry, "RemoteHostEnrollment")), routes.stage)
	huma.Register(s.API, remoteHostMutation(revoke, http.StatusNoContent, nil), routes.revoke)
	s.setRoutePolicy("POST /api/v1"+remotehost.OperatorRoute, routePolicy{body: jsonBody})
	s.setRoutePolicy("POST /api/v1"+remotehost.EnrollmentRoute, routePolicy{body: jsonBody})
}

func (routes remoteHostRoutes) operation(id, method, path, summary string) huma.Operation {
	return huma.Operation{
		OperationID: id, Method: method, Path: path, Summary: summary, Tags: []string{"Remote Hosts"},
		MaxBodyBytes: 32 * 1024, Errors: []int{http.StatusServiceUnavailable},
		Middlewares: huma.Middlewares{func(ctx huma.Context, next func(huma.Context)) {
			if routes.operator == nil {
				_ = huma.WriteErr(
					routes.server.API,
					ctx,
					http.StatusServiceUnavailable,
					"Remote Host enrollment is unavailable",
				)
				return
			}
			next(ctx)
		}},
	}
}

func remoteHostMutation(operation huma.Operation, status int, schema *huma.Schema) huma.Operation {
	operation.DefaultStatus = status
	response := &huma.Response{Description: http.StatusText(status)}
	if schema != nil {
		response.Content = map[string]*huma.MediaType{"application/json": {Schema: schema}}
	}
	operation.Responses = map[string]*huma.Response{strconv.Itoa(status): response}
	return operation
}

func (routes remoteHostRoutes) list(ctx context.Context, input *remoteHostListInput) (*remoteHostListOutput, error) {
	page, err := routes.operator.ListRemoteHosts(ctx, keyvalue.PageRequest{Limit: input.Limit, Cursor: input.Cursor})
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &remoteHostListOutput{Body: page}, nil
}

func (routes remoteHostRoutes) show(ctx context.Context, input *RemoteHostPath) (*remoteHostOutput, error) {
	view, err := routes.operator.ShowRemoteHost(ctx, input.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &remoteHostOutput{Body: view}, nil
}

func (routes remoteHostRoutes) create(
	ctx context.Context,
	input *remoteHostCreateInput,
) (*serviceMutationOutput, error) {
	return routes.mutation(routes.operator.CreateRemoteHost(ctx, input.Body.Name, input.Key))
}

func (routes remoteHostRoutes) stage(ctx context.Context, input *remoteHostStageInput) (*serviceMutationOutput, error) {
	return routes.mutation(routes.operator.StageRemoteHostEnrollment(ctx, input.ID, input.Body, input.Key))
}

func (routes remoteHostRoutes) revoke(
	ctx context.Context,
	input *remoteHostRevokeInput,
) (*serviceMutationOutput, error) {
	return routes.mutation(routes.operator.RevokeRemoteHostEnrollment(ctx, input.ID, input.Generation, input.Key))
}

func (routes remoteHostRoutes) mutation(
	response idempotency.IdempotencyResponse,
	err error,
) (*serviceMutationOutput, error) {
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return routes.server.serviceMutationResponse(response), nil
}
