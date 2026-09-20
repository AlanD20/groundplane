package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strconv"
	"unicode/utf8"

	environmentcapability "github.com/AlanD20/groundplane/internal/controller/environment"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type EnvironmentMutator interface {
	CreateEnvironment(
		context.Context,
		environmentcapability.CreateEnvironmentInput,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
}

type environmentListInput struct {
	Project string `query:"project" required:"true"`
	Limit   int    `query:"limit" required:"false"`
	Cursor  string `query:"cursor" required:"false"`
}

type environmentShowInput struct {
	ID string `path:"id"`
}

type environmentCreateInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

type environmentRenameInput struct {
	ID             string `path:"id"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

type environmentEditInput struct {
	ID             string `path:"id"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.EnvironmentEdit
}

type environmentDeleteInput struct {
	ID  string `path:"id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

type environmentOutput struct {
	Body apiTypes.Environment
}

type environmentPageOutput struct {
	Body apiTypes.EnvironmentPage
}

type environmentMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerEnvironments() {
	environmentSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.Environment](),
		true,
		"Environment",
	)
	taskAcceptedSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.TaskAccepted](),
		true,
		"TaskAccepted",
	)
	registerEnvironmentMutation[apiTypes.EnvironmentCreate](
		s,
		huma.Operation{
			OperationID: "environment.create", Method: http.MethodPost, Path: "/environments",
			Summary: "Create an environment", Tags: []string{"Environment"}, DefaultStatus: http.StatusAccepted,
			Middlewares: huma.Middlewares{s.rejectEnvironmentQuery},
		},
		taskAcceptedSchema,
		s.createEnvironment,
		"EnvironmentCreate",
	)
	huma.Register(s.API, huma.Operation{
		OperationID: "environment.list", Method: http.MethodGet, Path: "/environments",
		Summary: "List environments", Tags: []string{"Environment"},
		Middlewares: huma.Middlewares{s.validateEnvironmentListQuery},
	}, s.listEnvironments)
	huma.Register(s.API, huma.Operation{
		OperationID: "environment.show", Method: http.MethodGet, Path: "/environments/{id}",
		Summary: "Show an environment", Tags: []string{"Environment"},
		Middlewares: huma.Middlewares{s.rejectEnvironmentQuery},
	}, s.showEnvironment)
	registerEnvironmentMutation[apiTypes.EnvironmentEdit](
		s,
		huma.Operation{
			OperationID: "environment.edit", Method: http.MethodPatch, Path: "/environments/{id}",
			Summary: "Edit an environment network pool", Tags: []string{"Environment"}, DefaultStatus: http.StatusOK,
			Middlewares: huma.Middlewares{s.rejectEnvironmentQuery},
		},
		environmentSchema,
		s.editEnvironment,
		"EnvironmentEdit",
	)
	registerEnvironmentMutation[apiTypes.EnvironmentRename](
		s,
		huma.Operation{
			OperationID: "environment.rename", Method: http.MethodPost, Path: "/environments/{id}/rename",
			Summary: "Rename an environment", Tags: []string{"Environment"}, DefaultStatus: http.StatusOK,
			Middlewares: huma.Middlewares{s.rejectEnvironmentQuery},
		},
		environmentSchema,
		s.renameEnvironment,
		"EnvironmentRename",
	)
	for _, pattern := range []string{
		"POST /api/v1/environments",
		"POST /api/v1/environments/{id}/rename",
	} {
		s.setRoutePolicy(pattern, routePolicy{body: jsonBody})
	}
	s.setRoutePolicy("PATCH /api/v1/environments/{id}", routePolicy{
		body: jsonBody, validateJSON: validateEnvironmentEditJSON,
	})
}

func registerEnvironmentMutation[InputBody any, Input any](
	s *Server,
	operation huma.Operation,
	responseSchema *huma.Schema,
	handler func(context.Context, *Input) (*environmentMutationOutput, error),
	requestName string,
) {
	operation.SkipValidateBody = true
	operation.RequestBody = &huma.RequestBody{
		Required: true,
		Content: map[string]*huma.MediaType{
			"application/json": {
				Schema: s.API.OpenAPI().Components.Schemas.Schema(
					reflect.TypeFor[InputBody](),
					true,
					requestName,
				),
			},
		},
	}
	operation.Responses = map[string]*huma.Response{
		strconv.Itoa(operation.DefaultStatus): {
			Description: http.StatusText(operation.DefaultStatus),
			Content: map[string]*huma.MediaType{
				"application/json": {Schema: responseSchema},
			},
		},
	}
	huma.Register(s.API, operation, handler)
}

func (s *Server) validateEnvironmentListQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	query := requestURL.Query()
	for key, values := range query {
		if key != "project" && key != "limit" && key != "cursor" {
			s.writeEnvironmentHumaProblem(ctx, "Environment list query is invalid")
			return
		}
		if len(values) != 1 {
			s.writeEnvironmentHumaProblem(ctx, "Environment list query contains duplicate values")
			return
		}
	}
	next(ctx)
}

func (s *Server) rejectEnvironmentQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	if len(requestURL.Query()) != 0 {
		s.writeEnvironmentHumaProblem(ctx, "Environment request query is invalid")
		return
	}
	next(ctx)
}

func (s *Server) writeEnvironmentHumaProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Environment request problem", slog.Any("error", err))
	}
}

func (s *Server) createEnvironment(
	ctx context.Context,
	request *environmentCreateInput,
) (*environmentMutationOutput, error) {
	if s.environmentMutations == nil {
		return nil, errs.New(errs.KindInternal, "Environment mutator is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeEnvironmentCreate(request.RawBody)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response, err := s.environmentMutations.CreateEnvironment(
		ctx, input, request.IdempotencyKey,
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.environmentMutationResponse(response, "create"), nil
}

func decodeEnvironmentCreate(body []byte) (environmentcapability.CreateEnvironmentInput, error) {
	if !utf8.Valid(body) {
		return environmentcapability.CreateEnvironmentInput{}, errs.New(
			errs.KindMalformedRequest,
			"Environment creation body is not valid UTF-8",
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return environmentcapability.CreateEnvironmentInput{}, projectCreateJSONError(err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return environmentcapability.CreateEnvironmentInput{}, errs.New(
			errs.KindMalformedRequest,
			"Environment creation body must be an object",
		)
	}
	input := environmentcapability.CreateEnvironmentInput{}
	seen := make(map[string]struct{}, 3)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return environmentcapability.CreateEnvironmentInput{}, projectCreateJSONError(err)
		}
		member, ok := token.(string)
		if !ok {
			return environmentcapability.CreateEnvironmentInput{}, errs.New(
				errs.KindMalformedRequest,
				"Environment creation member name is invalid",
			)
		}
		if _, duplicate := seen[member]; duplicate {
			return environmentcapability.CreateEnvironmentInput{}, errs.New(
				errs.KindMalformedRequest,
				"Environment creation body contains a duplicate member",
			)
		}
		seen[member] = struct{}{}
		if member != "project_id" && member != "name" && member != "network_pool" {
			return environmentcapability.CreateEnvironmentInput{}, errs.New(
				errs.KindMalformedRequest,
				"Environment creation body contains an unknown member",
			)
		}
		var value string
		if err := decoder.Decode(&value); err != nil {
			var typeError *json.UnmarshalTypeError
			if errors.As(err, &typeError) {
				return environmentcapability.CreateEnvironmentInput{}, errs.New(
					errs.KindValidationFailed,
					"Environment creation members must be strings",
				)
			}
			return environmentcapability.CreateEnvironmentInput{}, projectCreateJSONError(err)
		}
		if member == "project_id" {
			input.ProjectID = value
		} else if member == "name" {
			input.Name = value
		} else {
			input.NetworkPool = value
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return environmentcapability.CreateEnvironmentInput{}, projectCreateJSONError(err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return environmentcapability.CreateEnvironmentInput{}, errs.New(
			errs.KindMalformedRequest,
			"Environment creation body is malformed",
		)
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return environmentcapability.CreateEnvironmentInput{}, projectCreateJSONError(err)
	}
	if err := environmentcapability.ValidateEnvironmentCreateInput(input); err != nil {
		return environmentcapability.CreateEnvironmentInput{}, err
	}
	return input, nil
}

func (s *Server) listEnvironments(
	ctx context.Context,
	request *environmentListInput,
) (*environmentPageOutput, error) {
	page, err := s.environments.List(ctx, environmentcapability.ListInput{
		ProjectID: request.Project,
		Limit:     request.Limit,
		Cursor:    request.Cursor,
	})
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response := apiTypes.EnvironmentPage{
		Items: make([]apiTypes.Environment, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = environmentReadResponse(item)
	}
	return &environmentPageOutput{Body: response}, nil
}

func (s *Server) showEnvironment(
	ctx context.Context,
	request *environmentShowInput,
) (*environmentOutput, error) {
	result, err := s.environments.Get(ctx, environmentcapability.GetInput{ID: request.ID})
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &environmentOutput{Body: environmentReadResponse(result)}, nil
}

func environmentReadResponse(result environmentcapability.Environment) apiTypes.Environment {
	return apiTypes.Environment{
		ID: result.ID, ProjectID: result.ProjectID, Name: result.Name,
		NetworkPool: result.NetworkPool, VolumeDir: result.VolumeDir,
		ProvisioningState: apiTypes.EnvironmentProvisioningState(result.ProvisioningState),
		CreateTaskID:      result.CreateTaskID,
		DeletionTaskID:    result.DeletionTaskID,
		NetworkCapacity: apiTypes.EnvironmentNetworkCapacity{
			TotalAddresses:     result.NetworkCapacity.TotalAddresses,
			AllocatedAddresses: result.NetworkCapacity.AllocatedAddresses,
			AvailableAddresses: result.NetworkCapacity.AvailableAddresses,
			ZoneCount:          result.NetworkCapacity.ZoneCount,
		},
	}
}

func (s *Server) environmentMutationResponse(
	response idempotencyrecord.IdempotencyResponse,
	action string,
) *environmentMutationOutput {
	return &environmentMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
				s.Logger.Error(
					"controller: write Environment mutation response",
					"action",
					action,
					slog.Any("error", err),
				)
			}
		},
	}
}
