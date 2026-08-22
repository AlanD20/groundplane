package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type attachRenameInput struct {
	ID             string `path:"id" required:"true" pattern:"^att_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

func (s *Server) registerAttachRename() {
	registry := s.API.OpenAPI().Components.Schemas
	requestSchema := registry.Schema(reflect.TypeFor[apiTypes.AttachRenameRequest](), true, "AttachRenameRequest")
	requestShape := registry.SchemaFromRef(requestSchema.Ref)
	nameLimit := 255
	requestShape.Properties["name"].MaxLength = &nameLimit
	requestShape.Properties["name"].Pattern = `^[a-z0-9]+(?:-[a-z0-9]+)*$`
	attachSchema := registry.Schema(reflect.TypeFor[apiTypes.Attach](), true, "Attach")
	huma.Register(s.API, huma.Operation{
		OperationID: "attach.rename", Method: http.MethodPost, Path: "/attaches/{id}/rename",
		Summary: "Rename an attach", Tags: []string{"Attach"}, DefaultStatus: http.StatusOK,
		Middlewares: huma.Middlewares{s.rejectAttachQuery}, SkipValidateBody: true,
		RequestBody: &huma.RequestBody{Required: true, Content: map[string]*huma.MediaType{
			"application/json": {Schema: requestSchema},
		}},
		Responses: map[string]*huma.Response{
			strconv.Itoa(http.StatusOK): {
				Description: http.StatusText(http.StatusOK),
				Content:     map[string]*huma.MediaType{"application/json": {Schema: attachSchema}},
			},
		},
	}, s.renameAttach)
	s.setRoutePolicy("POST /api/v1/attaches/{id}/rename", routePolicy{body: jsonBody})
}

func (s *Server) renameAttach(
	ctx context.Context,
	request *attachRenameInput,
) (*attachMutationOutput, error) {
	if s.attachMutations == nil {
		return nil, errs.New(errs.KindInternal, "Attach mutator is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeAttachRename(request.RawBody)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response, err := s.attachMutations.RenameAttach(ctx, request.ID, input, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.attachMutationResponse(response, "rename"), nil
}

func decodeAttachRename(body []byte) (apiTypes.AttachRenameRequest, error) {
	if !utf8.Valid(body) {
		return apiTypes.AttachRenameRequest{}, errs.New(
			errs.KindMalformedRequest,
			"Attach rename body is not valid UTF-8",
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return apiTypes.AttachRenameRequest{}, projectCreateJSONError(err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return apiTypes.AttachRenameRequest{}, errs.New(
			errs.KindMalformedRequest,
			"Attach rename body must be an object",
		)
	}
	request := apiTypes.AttachRenameRequest{}
	seen := false
	for decoder.More() {
		token, tokenErr := decoder.Token()
		if tokenErr != nil {
			return apiTypes.AttachRenameRequest{}, projectCreateJSONError(tokenErr)
		}
		member, ok := token.(string)
		if !ok {
			return apiTypes.AttachRenameRequest{}, errs.New(
				errs.KindMalformedRequest,
				"Attach rename member name is invalid",
			)
		}
		if member != "name" {
			return apiTypes.AttachRenameRequest{}, errs.New(
				errs.KindMalformedRequest,
				"Attach rename body contains an unknown member",
			)
		}
		if seen {
			return apiTypes.AttachRenameRequest{}, errs.New(
				errs.KindMalformedRequest,
				"Attach rename body contains a duplicate member",
			)
		}
		seen = true
		if err := decoder.Decode(&request.Name); err != nil {
			var typeError *json.UnmarshalTypeError
			if errors.As(err, &typeError) {
				return apiTypes.AttachRenameRequest{}, errs.New(
					errs.KindValidationFailed,
					"Attach name must be a string",
				)
			}
			return apiTypes.AttachRenameRequest{}, projectCreateJSONError(err)
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return apiTypes.AttachRenameRequest{}, projectCreateJSONError(err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return apiTypes.AttachRenameRequest{}, errs.New(errs.KindMalformedRequest, "Attach rename body is malformed")
	}
	if _, err = decoder.Token(); err != io.EOF {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return apiTypes.AttachRenameRequest{}, projectCreateJSONError(err)
	}
	if err := etcd.ValidateAttachName(request.Name); err != nil {
		return apiTypes.AttachRenameRequest{}, err
	}
	return request, nil
}
