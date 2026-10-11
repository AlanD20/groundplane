package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"unicode/utf8"

	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type ConnectorChecker interface {
	CheckConnection(
		context.Context,
		string,
		apiTypes.ConnectorCheckRequest,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
	GetConnectionCheck(context.Context, string, string) (apiTypes.ConnectorCheckResult, error)
	GetLatestConnectionCheck(context.Context, string, string) (apiTypes.ConnectorCheckResult, error)
}

type connectorCheckShowInput struct {
	Task        string `query:"task" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	Connector   string `query:"connector" pattern:"^con_[0-9A-HJKMNP-TV-Z]{26}$"`
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type connectorCheckOutput struct{ Body apiTypes.ConnectorCheckResult }

func (s *Server) registerConnectorChecks() {
	registry := s.API.OpenAPI().Components.Schemas
	// Reuse the write-only credential variants without letting Huma's validation
	// diagnostics interpolate submitted credential values into error responses.
	connectorCreateRequestSchema(registry)
	connectorEditRequestSchema(registry)
	schema := registry.Schema(reflect.TypeFor[apiTypes.ConnectorCheckRequest](), true, "ConnectorCheckRequest")
	accepted := registry.Schema(reflect.TypeFor[apiTypes.TaskAccepted](), true, "TaskAccepted")
	huma.Register(s.API, huma.Operation{OperationID: "connector.check", Method: http.MethodPost,
		Path: "/connectors/checks", Summary: "Test saved or unsaved Connector permissions without saving",
		Tags: []string{"Connector"}, DefaultStatus: http.StatusAccepted, SkipValidateBody: true,
		RequestBody: &huma.RequestBody{Required: true, Content: map[string]*huma.MediaType{
			"application/json": {Schema: schema},
		}}, Responses: attachMutationResponses(accepted),
	}, s.checkConnector)
	huma.Register(s.API, huma.Operation{OperationID: "connector.check.show", Method: http.MethodGet,
		Path: "/connectors/checks", Summary: "Show a Task's Connector check or the latest saved Connector check",
		Tags: []string{"Connector"},
	}, s.showConnectorCheck)
	s.setRoutePolicy("POST /api/v1/connectors/checks", routePolicy{body: jsonBody})
}

func (s *Server) checkConnector(ctx context.Context, request *connectorCreateInput) (*connectorMutationOutput, error) {
	if s.connectorChecks == nil {
		return nil, errs.New(errs.KindInternal, "Connector checks are not configured")
	}
	defer clear(request.RawBody)
	if !utf8.Valid(request.RawBody) || rejectConnectorDuplicateMembers(request.RawBody) != nil {
		return nil, errs.New(errs.KindMalformedRequest, "Connector check body is malformed")
	}
	decoder := json.NewDecoder(bytes.NewReader(request.RawBody))
	decoder.DisallowUnknownFields()
	var input apiTypes.ConnectorCheckRequest
	if err := decoder.Decode(&input); err != nil {
		return nil, errs.New(errs.KindMalformedRequest, "Connector check body is malformed")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errs.New(errs.KindMalformedRequest, "Connector check body has trailing data")
	}
	response, err := s.connectorChecks.CheckConnection(ctx, request.Env, input, request.Key)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &connectorMutationOutput{Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
				s.Logger.Error("controller: write Connector check acceptance", slog.Any("error", err))
			}
		},
	}, nil
}

func (s *Server) showConnectorCheck(
	ctx context.Context,
	request *connectorCheckShowInput,
) (*connectorCheckOutput, error) {
	if s.connectorChecks == nil {
		return nil, errs.New(errs.KindInternal, "Connector checks are not configured")
	}
	if (request.Task == "") == (request.Connector == "") {
		return nil, errs.New(
			errs.KindValidationFailed,
			"connection check lookup requires exactly one Task or Connector",
		)
	}
	var result apiTypes.ConnectorCheckResult
	var err error
	if request.Task != "" {
		result, err = s.connectorChecks.GetConnectionCheck(ctx, request.Environment, request.Task)
	} else {
		result, err = s.connectorChecks.GetLatestConnectionCheck(ctx, request.Environment, request.Connector)
	}
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &connectorCheckOutput{Body: result}, nil
}
