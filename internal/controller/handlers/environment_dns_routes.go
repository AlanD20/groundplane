package handlers

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type EnvironmentDNSService interface {
	Get(context.Context, string) (apiTypes.EnvironmentDNS, error)
	Create(
		context.Context,
		string,
		string,
		apiTypes.EnvironmentDNSWrite,
		string,
	) (idempotency.IdempotencyResponse, error)
	Edit(
		context.Context,
		string,
		string,
		string,
		apiTypes.EnvironmentDNSWrite,
		string,
	) (idempotency.IdempotencyResponse, error)
	Remove(context.Context, string, string, string, string) (idempotency.IdempotencyResponse, error)
}

type environmentDNSInput struct {
	ID string `path:"id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
}
type environmentDNSOutput struct {
	ETag string `header:"ETag"`
	Body apiTypes.EnvironmentDNS
}
type environmentDNSCreateInput struct {
	ID      string `path:"id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key     string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	IfMatch string `header:"If-Match" required:"true"`
	Body    apiTypes.EnvironmentDNSWrite
}
type environmentDNSEditInput struct {
	ID      string `path:"id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Record  string `path:"record" pattern:"^dns_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key     string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	IfMatch string `header:"If-Match" required:"true"`
	Body    apiTypes.EnvironmentDNSWrite
}
type environmentDNSRemoveInput struct {
	ID      string `path:"id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Record  string `path:"record" pattern:"^dns_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key     string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	IfMatch string `header:"If-Match" required:"true"`
}

func (s *Server) registerEnvironmentDNS() {
	resource := openAPISchema[apiTypes.EnvironmentDNSSaved](s.API.OpenAPI().Components.Schemas, "EnvironmentDNSSaved")
	responses := map[string]*huma.Response{
		"200": {
			Description: "DNS intention saved; read DNS status for resolver activation",
			Content:     map[string]*huma.MediaType{"application/json": {Schema: resource}},
		},
		"201": {
			Description: "DNS intention created; read DNS status for resolver activation",
			Content:     map[string]*huma.MediaType{"application/json": {Schema: resource}},
		},
	}
	huma.Register(s.API, huma.Operation{OperationID: "environment.dns.list", Method: http.MethodGet,
		Path: "/environments/{id}/dns-records", Summary: "Read Environment DNS intentions and resolver status", Tags: []string{"Environment DNS"}}, s.getEnvironmentDNS)
	huma.Register(s.API, huma.Operation{OperationID: "environment.dns.create", Method: http.MethodPost,
		Path: "/environments/{id}/dns-records", Summary: "Create an Environment DNS record", Tags: []string{"Environment DNS"},
		DefaultStatus: http.StatusCreated, Responses: responses}, s.createEnvironmentDNS)
	huma.Register(s.API, huma.Operation{OperationID: "environment.dns.edit", Method: http.MethodPut,
		Path: "/environments/{id}/dns-records/{record}", Summary: "Replace an Environment DNS target or name", Tags: []string{"Environment DNS"},
		DefaultStatus: http.StatusOK, Responses: responses}, s.editEnvironmentDNS)
	huma.Register(s.API, huma.Operation{OperationID: "environment.dns.remove", Method: http.MethodDelete,
		Path: "/environments/{id}/dns-records/{record}", Summary: "Withdraw an Environment DNS record", Tags: []string{"Environment DNS"},
		DefaultStatus: http.StatusNoContent, Responses: map[string]*huma.Response{
			"204": {Description: "DNS removal saved; read DNS status for resolver withdrawal"},
		},
		Middlewares: huma.Middlewares{s.rejectTaskMutationBody, s.rejectTaskMutationQuery}}, s.removeEnvironmentDNS)
	s.setRoutePolicy("POST /api/v1/environments/{id}/dns-records", routePolicy{body: jsonBody})
	s.setRoutePolicy("PUT /api/v1/environments/{id}/dns-records/{record}", routePolicy{body: jsonBody})
}

func (s *Server) getEnvironmentDNS(ctx context.Context, input *environmentDNSInput) (*environmentDNSOutput, error) {
	if s.environmentDNS == nil {
		return nil, errs.New(errs.KindInternal, "Environment DNS is not configured")
	}
	result, err := s.environmentDNS.Get(ctx, input.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &environmentDNSOutput{ETag: "\"" + result.Revision + "\"", Body: result}, nil
}

func (s *Server) createEnvironmentDNS(
	ctx context.Context,
	input *environmentDNSCreateInput,
) (*componentMutationOutput, error) {
	if s.environmentDNS == nil {
		return nil, errs.New(errs.KindInternal, "Environment DNS is not configured")
	}
	revision, err := parseBlueprintIfMatch(input.IfMatch)
	if err != nil {
		return nil, err
	}
	response, err := s.environmentDNS.Create(ctx, input.ID, revision, input.Body, input.Key)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return componentMutationResponse(response), nil
}

func (s *Server) editEnvironmentDNS(
	ctx context.Context,
	input *environmentDNSEditInput,
) (*componentMutationOutput, error) {
	if s.environmentDNS == nil {
		return nil, errs.New(errs.KindInternal, "Environment DNS is not configured")
	}
	revision, err := parseBlueprintIfMatch(input.IfMatch)
	if err != nil {
		return nil, err
	}
	response, err := s.environmentDNS.Edit(ctx, input.ID, input.Record, revision, input.Body, input.Key)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return componentMutationResponse(response), nil
}

func (s *Server) removeEnvironmentDNS(
	ctx context.Context,
	input *environmentDNSRemoveInput,
) (*componentMutationOutput, error) {
	if s.environmentDNS == nil {
		return nil, errs.New(errs.KindInternal, "Environment DNS is not configured")
	}
	revision, err := parseBlueprintIfMatch(input.IfMatch)
	if err != nil {
		return nil, err
	}
	response, err := s.environmentDNS.Remove(ctx, input.ID, input.Record, revision, input.Key)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	output := componentMutationResponse(response)
	output.ContentType = ""
	return output, nil
}
