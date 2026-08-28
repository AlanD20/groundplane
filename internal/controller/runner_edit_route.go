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

	"github.com/AlanD20/groundplane/internal/common/slug"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type runnerEditInput struct {
	ID             string `path:"id" required:"true" pattern:"^run_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

type runnerMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerRunnerEdit() {
	registry := s.API.OpenAPI().Components.Schemas
	requestSchema := registry.Schema(reflect.TypeFor[apiTypes.RunnerEditRequest](), true, "RunnerEditRequest")
	requestShape := registry.SchemaFromRef(requestSchema.Ref)
	slugLimit := 63
	requestShape.Properties["slug"].MaxLength = &slugLimit
	requestShape.Properties["slug"].Pattern = `^[a-z0-9]+(?:-[a-z0-9]+)*$`
	runnerSchema := registry.Schema(reflect.TypeFor[apiTypes.Runner](), true, "Runner")
	huma.Register(s.API, huma.Operation{
		OperationID: "runner.edit", Method: http.MethodPatch, Path: "/runners/{id}",
		Summary: "Replace a Runner slug", Tags: []string{"Runner"}, DefaultStatus: http.StatusOK,
		Middlewares: huma.Middlewares{s.rejectRunnerEditQuery}, SkipValidateBody: true,
		RequestBody: &huma.RequestBody{Required: true, Content: map[string]*huma.MediaType{
			"application/json": {Schema: requestSchema},
		}},
		Responses: map[string]*huma.Response{strconv.Itoa(http.StatusOK): {
			Description: http.StatusText(http.StatusOK),
			Content:     map[string]*huma.MediaType{"application/json": {Schema: runnerSchema}},
		}},
	}, s.editRunner)
	s.setRoutePolicy("PATCH /api/v1/runners/{id}", routePolicy{body: jsonBody})
}

func (s *Server) editRunner(
	ctx context.Context,
	request *runnerEditInput,
) (*runnerMutationOutput, error) {
	if s.runnerMutations == nil {
		return nil, errs.New(errs.KindInternal, "Runner mutator is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeRunnerEdit(request.RawBody)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response, err := s.runnerMutations.RenameRunner(ctx, request.ID, input, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.runnerMutationResponse(response), nil
}

func decodeRunnerEdit(body []byte) (apiTypes.RunnerEditRequest, error) {
	if !utf8.Valid(body) {
		return apiTypes.RunnerEditRequest{}, errs.New(errs.KindMalformedRequest, "Runner edit body is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return apiTypes.RunnerEditRequest{}, projectCreateJSONError(err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return apiTypes.RunnerEditRequest{}, errs.New(errs.KindMalformedRequest, "Runner edit body must be an object")
	}
	request := apiTypes.RunnerEditRequest{}
	seen := false
	for decoder.More() {
		token, tokenErr := decoder.Token()
		if tokenErr != nil {
			return apiTypes.RunnerEditRequest{}, projectCreateJSONError(tokenErr)
		}
		member, ok := token.(string)
		if !ok || member != "slug" {
			return apiTypes.RunnerEditRequest{}, errs.New(errs.KindMalformedRequest, "Runner edit body contains an unknown member")
		}
		if seen {
			return apiTypes.RunnerEditRequest{}, errs.New(errs.KindMalformedRequest, "Runner edit body contains a duplicate member")
		}
		seen = true
		if err := decoder.Decode(&request.Slug); err != nil {
			var typeError *json.UnmarshalTypeError
			if errors.As(err, &typeError) {
				return apiTypes.RunnerEditRequest{}, errs.New(errs.KindValidationFailed, "Runner slug must be a string")
			}
			return apiTypes.RunnerEditRequest{}, projectCreateJSONError(err)
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return apiTypes.RunnerEditRequest{}, projectCreateJSONError(err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return apiTypes.RunnerEditRequest{}, errs.New(errs.KindMalformedRequest, "Runner edit body is malformed")
	}
	if _, err = decoder.Token(); err != io.EOF {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return apiTypes.RunnerEditRequest{}, projectCreateJSONError(err)
	}
	if !seen || !slug.Valid(request.Slug) {
		return apiTypes.RunnerEditRequest{}, errs.New(
			errs.KindValidationFailed,
			"Runner slug must be a lowercase ASCII label of 1-63 bytes",
		)
	}
	return request, nil
}

func (s *Server) rejectRunnerEditQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	if len(requestURL.Query()) != 0 {
		if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, "Runner edit query is invalid"); err != nil && s.Logger != nil {
			s.Logger.Error("controller: write Runner edit problem", slog.Any("error", err))
		}
		return
	}
	next(ctx)
}

func (s *Server) runnerMutationResponse(response etcd.IdempotencyResponse) *runnerMutationOutput {
	return &runnerMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
				s.Logger.Error("controller: write Runner edit response", slog.Any("error", err))
			}
		},
	}
}
