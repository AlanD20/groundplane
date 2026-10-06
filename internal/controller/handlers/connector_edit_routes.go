package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strconv"
	"unicode/utf8"
)

type connectorEditInput struct {
	ID      string `path:"id" pattern:"^con_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key     string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	IfMatch string `header:"If-Match" required:"true"`
	RawBody []byte
}

func (s *Server) registerConnectorEditing() {
	schemas := s.API.OpenAPI().Components.Schemas
	operation := huma.Operation{
		OperationID: "connector.edit", Method: http.MethodPatch, Path: "/connectors/{id}",
		Summary: "Edit an Environment connector", Tags: []string{"Connector"}, SkipValidateBody: true,
		RequestBody: &huma.RequestBody{Required: true, Content: map[string]*huma.MediaType{
			"application/json": {
					Schema: connectorEditRequestSchema(schemas),
			},
		}},
		Responses: map[string]*huma.Response{
			"200": {Description: "Updated connector", Content: map[string]*huma.MediaType{
				"application/json": {Schema: schemas.Schema(reflect.TypeFor[apiTypes.Connector](), true, "Connector")},
			}},
		},
	}
	huma.Register(s.API, operation, s.editConnector)
	s.setRoutePolicy("PATCH /api/v1/connectors/{id}", routePolicy{body: jsonBody})
}

func (s *Server) editConnector(ctx context.Context, request *connectorEditInput) (*connectorMutationOutput, error) {
	if s.connectorMutations == nil {
		return nil, errs.New(errs.KindInternal, "connector mutator is not configured")
	}
	defer clear(request.RawBody)
	quoted, err := strconv.Unquote(request.IfMatch)
	if err != nil || strconv.Quote(quoted) != request.IfMatch {
		return nil, errs.New(errs.KindMalformedRequest, "If-Match must be the exact quoted connector ETag")
	}
	revision, err := strconv.ParseInt(quoted, 10, 64)
	if err != nil || revision <= 0 || strconv.FormatInt(revision, 10) != quoted {
		return nil, errs.New(errs.KindMalformedRequest, "If-Match must contain a positive connector revision")
	}
	if !utf8.Valid(request.RawBody) {
		return nil, errs.New(errs.KindMalformedRequest, "connector edit body is not valid UTF-8")
	}
	if err := rejectConnectorDuplicateMembers(request.RawBody); err != nil {
		return nil, err
	}
	if err := rejectConnectorEditNull(request.RawBody); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(request.RawBody))
	decoder.DisallowUnknownFields()
	input := apiTypes.ConnectorEditRequest{}
	if err := decoder.Decode(&input); err != nil {
		return nil, connectorCreateJSONError(err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, connectorCreateJSONError(err)
	}
	response, err := s.connectorMutations.EditConnector(ctx, request.ID, revision, input, request.Key)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &connectorMutationOutput{
		Status:      response.Status,
		ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
				s.Logger.Error("controller: write connector edit response", slog.Any("error", err))
			}
		},
	}, nil
}

func rejectConnectorEditNull(body []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return connectorCreateJSONError(err)
		}
		if token == nil {
			return errs.New(
				errs.KindMalformedRequest,
				"connector edit fields cannot be null; omit fields to keep them unchanged",
			)
		}
	}
}
