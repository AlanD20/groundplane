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

type AttachMutator interface {
	ListAttaches(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.AttachRecord], error)
	CreateAttach(context.Context, apiTypes.AttachRequest, string) (etcd.IdempotencyResponse, error)
	DetachAttach(context.Context, string, string) (etcd.IdempotencyResponse, error)
	RenameAttach(context.Context, string, apiTypes.AttachRenameRequest, string) (etcd.IdempotencyResponse, error)
}

type attachCreateInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

type attachDetachInput struct {
	ID             string `path:"id"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

type attachMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerAttaches() {
	attachRequestSchema := s.attachRequestSchema()
	taskAcceptedSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.TaskAccepted](),
		true,
		"TaskAccepted",
	)
	createOperation := huma.Operation{
		OperationID: "attach.create", Method: http.MethodPost, Path: "/attaches",
		Summary: "Attach a backing service", Tags: []string{"Attach"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectAttachQuery}, SkipValidateBody: true,
		RequestBody: &huma.RequestBody{
			Required: true,
			Content: map[string]*huma.MediaType{
				"application/json": {
					Schema: attachRequestSchema,
				},
			},
		},
		Responses: attachMutationResponses(taskAcceptedSchema),
	}
	huma.Register(s.API, createOperation, s.createAttach)
	huma.Register(s.API, huma.Operation{
		OperationID: "attach.detach", Method: http.MethodDelete, Path: "/attaches/{id}",
		Summary: "Detach a backing service", Tags: []string{"Attach"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectAttachQuery},
		Responses:   attachMutationResponses(taskAcceptedSchema),
	}, s.detachAttach)
	s.setRoutePolicy("POST /api/v1/attaches", routePolicy{body: jsonBody})
	s.registerAttachList()
	s.registerAttachFactReveal()
	s.registerAttachRename()
}

func (s *Server) attachRequestSchema() *huma.Schema {
	registry := s.API.OpenAPI().Components.Schemas
	reference := registry.Schema(reflect.TypeFor[apiTypes.AttachRequest](), true, "AttachRequest")
	schema := registry.SchemaFromRef(reference.Ref)
	if schema == nil {
		return reference
	}
	one := 1
	eight := 8
	nameLimit := 255
	serviceIDs := schema.Properties["service_ids"]
	serviceIDs.Nullable = false
	serviceIDs.MinItems = &one
	serviceIDs.MaxItems = &one
	serviceIDs.UniqueItems = true
	serviceIDs.Items.Pattern = `^svc_[0-9A-HJKMNP-TV-Z]{26}$`
	grants := schema.Properties["grant_attach_ids"]
	grants.Nullable = false
	grants.MaxItems = &eight
	grants.UniqueItems = true
	grants.Items.Pattern = `^att_[0-9A-HJKMNP-TV-Z]{26}$`
	schema.Properties["backing_service_id"].Pattern = `^svc_[0-9A-HJKMNP-TV-Z]{26}$`
	name := schema.Properties["name"]
	name.MaxLength = &nameLimit
	name.Pattern = `^[a-z0-9]+(?:-[a-z0-9]+)*$`
	return reference
}

func attachMutationResponses(taskAcceptedSchema *huma.Schema) map[string]*huma.Response {
	return map[string]*huma.Response{
		strconv.Itoa(http.StatusAccepted): {
			Description: http.StatusText(http.StatusAccepted),
			Content: map[string]*huma.MediaType{
				"application/json": {Schema: taskAcceptedSchema},
			},
		},
	}
}

func (s *Server) rejectAttachQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	if len(requestURL.Query()) != 0 {
		if err := huma.WriteErr(
			s.API,
			ctx,
			http.StatusBadRequest,
			"Attach mutation query is invalid",
		); err != nil &&
			s.Logger != nil {
			s.Logger.Error("controller: write Attach request problem", slog.Any("error", err))
		}
		return
	}
	next(ctx)
}

func (s *Server) createAttach(
	ctx context.Context,
	request *attachCreateInput,
) (*attachMutationOutput, error) {
	if s.attachMutations == nil {
		return nil, errs.New(errs.KindInternal, "Attach mutator is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeAttachCreate(request.RawBody)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response, err := s.attachMutations.CreateAttach(ctx, input, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.attachMutationResponse(response, "create"), nil
}

func (s *Server) detachAttach(
	ctx context.Context,
	request *attachDetachInput,
) (*attachMutationOutput, error) {
	if s.attachMutations == nil {
		return nil, errs.New(errs.KindInternal, "Attach mutator is not configured")
	}
	response, err := s.attachMutations.DetachAttach(ctx, request.ID, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.attachMutationResponse(response, "detach"), nil
}

func decodeAttachCreate(body []byte) (apiTypes.AttachRequest, error) {
	if !utf8.Valid(body) {
		return apiTypes.AttachRequest{}, errs.New(errs.KindMalformedRequest, "Attach body is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return apiTypes.AttachRequest{}, projectCreateJSONError(err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return apiTypes.AttachRequest{}, errs.New(errs.KindMalformedRequest, "Attach body must be an object")
	}
	request := apiTypes.AttachRequest{}
	seen := make(map[string]struct{}, 4)
	for decoder.More() {
		token, tokenErr := decoder.Token()
		if tokenErr != nil {
			return apiTypes.AttachRequest{}, projectCreateJSONError(tokenErr)
		}
		member, ok := token.(string)
		if !ok {
			return apiTypes.AttachRequest{}, errs.New(errs.KindMalformedRequest, "Attach member name is invalid")
		}
		if _, duplicate := seen[member]; duplicate {
			return apiTypes.AttachRequest{}, errs.New(
				errs.KindMalformedRequest,
				"Attach body contains a duplicate member",
			)
		}
		seen[member] = struct{}{}
		switch member {
		case "service_ids":
			request.ServiceIDs, err = decodeAttachStringList(decoder, member)
		case "backing_service_id":
			err = decoder.Decode(&request.BackingServiceID)
		case "name":
			err = decoder.Decode(&request.Name)
		case "grant_attach_ids":
			request.GrantAttachIDs, err = decodeAttachStringList(decoder, member)
		default:
			return apiTypes.AttachRequest{}, errs.New(
				errs.KindMalformedRequest,
				"Attach body contains an unknown member",
			)
		}
		if err != nil {
			var typeError *json.UnmarshalTypeError
			if errors.As(err, &typeError) {
				return apiTypes.AttachRequest{}, errs.New(
					errs.KindValidationFailed,
					"Attach members must use their declared string or string-array type",
				)
			}
			return apiTypes.AttachRequest{}, projectCreateJSONError(err)
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return apiTypes.AttachRequest{}, projectCreateJSONError(err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return apiTypes.AttachRequest{}, errs.New(errs.KindMalformedRequest, "Attach body is malformed")
	}
	if _, err = decoder.Token(); err != io.EOF {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return apiTypes.AttachRequest{}, projectCreateJSONError(err)
	}
	return request, nil
}

func decodeAttachStringList(decoder *json.Decoder, member string) ([]string, error) {
	var values []string
	if err := decoder.Decode(&values); err != nil {
		return nil, err
	}
	if values == nil {
		return nil, errs.Newf(errs.KindValidationFailed, "Attach %s must be an array", member)
	}
	return values, nil
}

func (s *Server) attachMutationResponse(
	response etcd.IdempotencyResponse,
	action string,
) *attachMutationOutput {
	return &attachMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
				s.Logger.Error(
					"controller: write Attach mutation response",
					"action",
					action,
					slog.Any("error", err),
				)
			}
		},
	}
}
