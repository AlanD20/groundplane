package handlers

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type DNSChecker interface {
	CheckConnections(context.Context, string, apiTypes.DNSCheckRequest, string) (idempotency.IdempotencyResponse, error)
	GetConnectionCheck(context.Context, string, string) (apiTypes.DNSCheckResult, error)
}

type dnsCheckInput struct {
	ID   string `path:"id" pattern:"^cmp_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key  string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body apiTypes.DNSCheckRequest
}
type dnsCheckShowInput struct {
	ID   string `path:"id" pattern:"^cmp_[0-9A-HJKMNP-TV-Z]{26}$"`
	Task string `query:"task" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
}
type dnsCheckOutput struct{ Body apiTypes.DNSCheckResult }

func (s *Server) registerDNSChecks() {
	accepted := openAPISchema[apiTypes.TaskAccepted](s.API.OpenAPI().Components.Schemas, "TaskAccepted")
	huma.Register(s.API, huma.Operation{OperationID: "component.dns-check", Method: http.MethodPost,
		Path: "/components/{id}/dns-checks", Summary: "Query CoreDNS and its configured upstreams", Tags: []string{"Component"},
		DefaultStatus: http.StatusAccepted, Responses: attachMutationResponses(accepted)}, s.checkDNS)
	huma.Register(s.API, huma.Operation{OperationID: "component.dns-check.show", Method: http.MethodGet,
		Path: "/components/{id}/dns-checks", Summary: "Show a DNS check Task or the latest retained result", Tags: []string{"Component"}}, s.showDNSCheck)
	s.setRoutePolicy("POST /api/v1/components/{id}/dns-checks", routePolicy{body: jsonBody})
}

func (s *Server) checkDNS(ctx context.Context, request *dnsCheckInput) (*componentMutationOutput, error) {
	if s.dnsChecks == nil {
		return nil, errs.New(errs.KindInternal, "DNS checks are not configured")
	}
	response, err := s.dnsChecks.CheckConnections(ctx, request.ID, request.Body, request.Key)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	if s.controllerTaskWake != nil {
		s.controllerTaskWake()
	}
	return componentMutationResponse(response), nil
}

func (s *Server) showDNSCheck(ctx context.Context, request *dnsCheckShowInput) (*dnsCheckOutput, error) {
	if s.dnsChecks == nil {
		return nil, errs.New(errs.KindInternal, "DNS checks are not configured")
	}
	result, err := s.dnsChecks.GetConnectionCheck(ctx, request.ID, request.Task)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &dnsCheckOutput{Body: result}, nil
}
