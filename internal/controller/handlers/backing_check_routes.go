package handlers

import (
	"context"
	"net/http"
	"reflect"

	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type BackingChecker interface {
	CheckAccess(context.Context, string, apiTypes.BackingCheckRequest, string) (idempotency.IdempotencyResponse, error)
	GetAccessCheck(context.Context, string, string) (apiTypes.BackingCheckResult, error)
	GetLatestAccessCheck(context.Context, string, string, string) (apiTypes.BackingCheckResult, error)
}

type backingCheckInput struct {
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key         string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body        apiTypes.BackingCheckRequest
}

type backingCheckShowInput struct {
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Task        string `query:"task" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	Attach      string `query:"attach" pattern:"^att_[0-9A-HJKMNP-TV-Z]{26}$"`
	Grant       string `query:"grant" pattern:"^att_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type backingCheckOutput struct{ Body apiTypes.BackingCheckResult }

func (s *Server) registerBackingChecks() {
	accepted := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.TaskAccepted](),
		true,
		"TaskAccepted",
	)
	huma.Register(s.API, huma.Operation{OperationID: "attach.check", Method: http.MethodPost, Path: "/attaches/checks",
		Summary: "Check Backing access from the consumer workload network", Tags: []string{"Attach"},
		DefaultStatus: http.StatusAccepted, Responses: attachMutationResponses(accepted)}, s.checkBackingAccess)
	huma.Register(
		s.API,
		huma.Operation{OperationID: "attach.check.show", Method: http.MethodGet, Path: "/attaches/checks",
			Summary: "Show an access check Task or an Attach's latest retained check", Tags: []string{"Attach"}},
		s.showBackingCheck,
	)
	s.setRoutePolicy("POST /api/v1/attaches/checks", routePolicy{body: jsonBody})
}

func (s *Server) checkBackingAccess(ctx context.Context, request *backingCheckInput) (*attachMutationOutput, error) {
	if s.backingChecks == nil {
		return nil, errs.New(errs.KindInternal, "Backing checks are not configured")
	}
	response, err := s.backingChecks.CheckAccess(ctx, request.Environment, request.Body, request.Key)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	if s.controllerTaskWake != nil {
		s.controllerTaskWake()
	}
	return s.attachMutationResponse(response, "check"), nil
}

func (s *Server) showBackingCheck(ctx context.Context, request *backingCheckShowInput) (*backingCheckOutput, error) {
	if s.backingChecks == nil {
		return nil, errs.New(errs.KindInternal, "Backing checks are not configured")
	}
	if (request.Task == "") == (request.Attach == "") || request.Task != "" && request.Grant != "" {
		return nil, errs.New(
			errs.KindValidationFailed,
			"access check lookup requires a Task or an Attach with an optional grant",
		)
	}
	var result apiTypes.BackingCheckResult
	var err error
	if request.Task != "" {
		result, err = s.backingChecks.GetAccessCheck(ctx, request.Environment, request.Task)
	} else {
		result, err = s.backingChecks.GetLatestAccessCheck(ctx, request.Environment, request.Attach, request.Grant)
	}
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &backingCheckOutput{Body: result}, nil
}
