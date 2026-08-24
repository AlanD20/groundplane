package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strconv"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type ConnectorReader interface {
	GetConnector(context.Context, string) (etcd.Versioned[etcd.ConnectorRecord], error)
	ListConnectors(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ConnectorRecord], error)
}

type ConnectorMutator interface {
	CreateConnector(
		context.Context,
		string,
		apiTypes.ConnectorCreateRequest,
		string,
	) (etcd.IdempotencyResponse, error)
}

type ConnectorDeleter interface {
	DeleteConnector(context.Context, string, string) (etcd.IdempotencyResponse, error)
}

type connectorCreateInput struct {
	Env     string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key     string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody []byte
}

type connectorListInput struct {
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Limit       int    `query:"limit"       required:"false"`
	Cursor      string `query:"cursor"      required:"false"`
}

type connectorShowInput struct {
	ID string `path:"id" pattern:"^con_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type connectorRemoveInput struct {
	ID  string `path:"id" pattern:"^con_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

type connectorPageOutput struct {
	Body apiTypes.Page[apiTypes.Connector]
}

type connectorOutput struct {
	Body apiTypes.Connector
}

type connectorMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerConnectors() {
	connectorSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.Connector](),
		true,
		"Connector",
	)
	operation := huma.Operation{
		OperationID: "connector.create", Method: http.MethodPost, Path: "/connectors",
		Summary: "Create an Environment connector", Tags: []string{"Connector"},
		DefaultStatus: http.StatusCreated, SkipValidateBody: true,
	}
	operation.RequestBody = &huma.RequestBody{
		Required: true,
		Content: map[string]*huma.MediaType{
			"application/json": {
				Schema: s.API.OpenAPI().Components.Schemas.Schema(
					reflect.TypeFor[apiTypes.ConnectorCreateRequest](),
					true,
					"ConnectorCreateRequest",
				),
			},
		},
	}
	operation.Responses = map[string]*huma.Response{
		strconv.Itoa(http.StatusCreated): {
			Description: http.StatusText(http.StatusCreated),
			Content: map[string]*huma.MediaType{
				"application/json": {Schema: connectorSchema},
			},
		},
	}
	huma.Register(s.API, operation, s.createConnector)
	huma.Register(s.API, huma.Operation{
		OperationID: "connector.list", Method: http.MethodGet, Path: "/connectors",
		Summary: "List Environment connectors", Tags: []string{"Connector"},
	}, s.listConnectors)
	huma.Register(s.API, huma.Operation{
		OperationID: "connector.show", Method: http.MethodGet, Path: "/connectors/{id}",
		Summary: "Show connector metadata", Tags: []string{"Connector"},
	}, s.showConnector)
	taskAcceptedSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.TaskAccepted](), true, "TaskAccepted",
	)
	huma.Register(s.API, huma.Operation{
		OperationID: "connector.remove", Method: http.MethodDelete, Path: "/connectors/{id}",
		Summary: "Remove an Environment connector", Tags: []string{"Connector"},
		DefaultStatus: http.StatusAccepted,
		Middlewares:   huma.Middlewares{s.rejectConnectorDeleteBody, s.rejectConnectorDeleteQuery},
		Responses:     attachMutationResponses(taskAcceptedSchema),
	}, s.removeConnector)
	s.setRoutePolicy("POST /api/v1/connectors", routePolicy{body: jsonBody})
}

func (s *Server) createConnector(
	ctx context.Context,
	request *connectorCreateInput,
) (*connectorMutationOutput, error) {
	if s.connectorMutations == nil {
		return nil, errs.New(errs.KindInternal, "Connector mutator is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeConnectorCreate(request.RawBody)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response, err := s.connectorMutations.CreateConnector(
		ctx,
		request.Env,
		input,
		request.Key,
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &connectorMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, writeErr := ctx.BodyWriter().Write(response.Body); writeErr != nil && s.Logger != nil {
				s.Logger.Error("controller: write Connector creation response", slog.Any("error", writeErr))
			}
		},
	}, nil
}

func (s *Server) listConnectors(
	ctx context.Context,
	request *connectorListInput,
) (*connectorPageOutput, error) {
	if s.connectors == nil {
		return nil, errs.New(errs.KindInternal, "Connector reader is not configured")
	}
	pageRequest, err := serviceListRequest(request.Environment, request.Limit, request.Cursor)
	if err != nil {
		return nil, err
	}
	page, err := s.connectors.ListConnectors(ctx, request.Environment, pageRequest)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	items := make([]apiTypes.Connector, len(page.Items))
	for index, item := range page.Items {
		items[index] = connectorAPI(item.Record)
	}
	return &connectorPageOutput{Body: apiTypes.Page[apiTypes.Connector]{
		Items: items, NextCursor: page.NextCursor,
	}}, nil
}

func (s *Server) showConnector(
	ctx context.Context,
	request *connectorShowInput,
) (*connectorOutput, error) {
	if s.connectors == nil {
		return nil, errs.New(errs.KindInternal, "Connector reader is not configured")
	}
	record, err := s.connectors.GetConnector(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &connectorOutput{Body: connectorAPI(record.Record)}, nil
}

func (s *Server) removeConnector(
	ctx context.Context,
	request *connectorRemoveInput,
) (*connectorMutationOutput, error) {
	if s.connectorDeletions == nil {
		return nil, errs.New(errs.KindInternal, "connector deleter is not configured")
	}
	response, err := s.connectorDeletions.DeleteConnector(ctx, request.ID, request.Key)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &connectorMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, writeErr := ctx.BodyWriter().Write(response.Body); writeErr != nil && s.Logger != nil {
				s.Logger.Error("controller: write Connector deletion response", slog.Any("error", writeErr))
			}
		},
	}, nil
}

func (s *Server) rejectConnectorDeleteBody(ctx huma.Context, next func(huma.Context)) {
	var probe [1]byte
	count, err := ctx.BodyReader().Read(probe[:])
	if count != 0 || (err != nil && !errors.Is(err, io.EOF)) {
		s.writeConnectorProblem(ctx, "connector deletion body is not allowed")
		return
	}
	next(ctx)
}

func (s *Server) rejectConnectorDeleteQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	if len(requestURL.Query()) != 0 {
		s.writeConnectorProblem(ctx, "connector deletion query is invalid")
		return
	}
	next(ctx)
}

func (s *Server) writeConnectorProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Connector request problem", slog.Any("error", err))
	}
}

func connectorAPI(record etcd.ConnectorRecord) apiTypes.Connector {
	connector := record.Connector
	credentials := make(map[string]apiTypes.ConnectorCredential, len(connector.Credentials))
	for name, credential := range connector.Credentials {
		credentials[name] = apiTypes.ConnectorCredential{
			Kind: apiTypes.ConnectorCredentialKind(credential.Kind), SecretRef: credential.SecretRef,
		}
	}
	return apiTypes.Connector{
		ID: connector.ID, EnvironmentID: connector.EnvironmentID, Name: connector.Name,
		Kind: string(connector.Kind), Endpoint: connector.Endpoint, Bucket: connector.Bucket,
		Prefix: connector.Prefix, Region: connector.Region, PathStyle: connector.PathStyle,
		Credentials: credentials,
	}
}

func decodeConnectorCreate(body []byte) (apiTypes.ConnectorCreateRequest, error) {
	if !utf8.Valid(body) {
		return apiTypes.ConnectorCreateRequest{}, errs.New(
			errs.KindMalformedRequest,
			"Connector creation body is not valid UTF-8",
		)
	}
	if err := rejectConnectorDuplicateMembers(body); err != nil {
		return apiTypes.ConnectorCreateRequest{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	input := apiTypes.ConnectorCreateRequest{}
	if err := decoder.Decode(&input); err != nil {
		return apiTypes.ConnectorCreateRequest{}, connectorCreateJSONError(err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return apiTypes.ConnectorCreateRequest{}, connectorCreateJSONError(err)
	}
	return input, nil
}

func rejectConnectorDuplicateMembers(body []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := scanConnectorJSONValue(decoder); err != nil {
		return connectorCreateJSONError(err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return connectorCreateJSONError(err)
	}
	return nil
}

func scanConnectorJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			member, memberErr := decoder.Token()
			if memberErr != nil {
				return memberErr
			}
			name, ok := member.(string)
			if !ok {
				return errs.New(errs.KindMalformedRequest, "Connector creation member name is invalid")
			}
			if _, duplicate := seen[name]; duplicate {
				return errs.New(errs.KindMalformedRequest, "Connector creation body contains a duplicate member")
			}
			seen[name] = struct{}{}
			if err := scanConnectorJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return errs.New(errs.KindMalformedRequest, "Connector creation body is malformed")
		}
		return nil
	case '[':
		for decoder.More() {
			if err := scanConnectorJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return errs.New(errs.KindMalformedRequest, "Connector creation body is malformed")
		}
		return nil
	default:
		return errs.New(errs.KindMalformedRequest, "Connector creation body is malformed")
	}
}

func connectorCreateJSONError(err error) error {
	if err == nil || errorsIsMalformedConnector(err) {
		return err
	}
	return errs.New(errs.KindMalformedRequest, "Connector creation body is malformed")
}

func errorsIsMalformedConnector(err error) bool {
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindMalformedRequest
}
