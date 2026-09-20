package handlers

import (
	"bytes"
	"context"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"io"
	"net/http"
	"reflect"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

// EnvironmentBlueprintService owns the canonical authoring projection,
// side-effect-free validation, and revision-fenced desired-state replacement.
type EnvironmentBlueprintService interface {
	GetBlueprint(context.Context, string) (apiTypes.EnvironmentBlueprintDocument, error)
	ValidateBlueprint(
		context.Context,
		string,
		core.BlueprintBundle,
		string,
	) (apiTypes.EnvironmentBlueprintValidation, error)
	ApplyBlueprint(context.Context, string, core.BlueprintBundle, string, string) (idempotencyrecord.IdempotencyResponse, error)
}

type environmentBlueprintApplyInput struct {
	ID             string `path:"id"`
	IfMatch        string `header:"If-Match" required:"true"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	ContentType    string `header:"Content-Type" hidden:"true"`
	RawBody        []byte `contentType:"multipart/form-data"`
}

type environmentBlueprintShowInput struct {
	ID string `path:"id"`
}

type environmentBlueprintValidateInput struct {
	ID          string `path:"id"`
	IfMatch     string `header:"If-Match" required:"true"`
	ContentType string `header:"Content-Type" hidden:"true"`
	RawBody     []byte `contentType:"multipart/form-data"`
}

type environmentBlueprintShowOutput struct {
	ETag string `header:"ETag"`
	Body apiTypes.EnvironmentBlueprintDocument
}

type environmentBlueprintValidationOutput struct {
	Body apiTypes.EnvironmentBlueprintValidation
}

func (s *Server) registerEnvironmentBlueprints() {
	taskAcceptedSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.TaskAccepted](),
		true,
		"TaskAccepted",
	)
	huma.Register(s.API, huma.Operation{
		OperationID: "blueprint.show", Method: http.MethodGet, Path: "/environments/{id}/blueprint",
		Summary: "Show the current environment Blueprint", Tags: []string{"Blueprint"},
		Middlewares: huma.Middlewares{s.rejectEnvironmentQuery},
	}, s.showEnvironmentBlueprint)
	validationSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.EnvironmentBlueprintValidation](),
		true,
		"EnvironmentBlueprintValidation",
	)
	validate := huma.Operation{
		OperationID: "blueprint.validate", Method: http.MethodPost, Path: "/environments/{id}/blueprint/validate",
		Summary: "Validate an environment Blueprint", Tags: []string{"Blueprint"}, DefaultStatus: http.StatusOK,
		Middlewares: huma.Middlewares{s.rejectEnvironmentQuery}, SkipValidateBody: true,
		MaxBodyBytes: productionBlueprintBodyLimit + 1,
		RequestBody: &huma.RequestBody{Required: true, Content: map[string]*huma.MediaType{
			"multipart/form-data": {Schema: blueprintMultipartSchema()},
		}},
		Responses: map[string]*huma.Response{strconv.Itoa(http.StatusOK): {
			Description: http.StatusText(http.StatusOK),
			Content:     map[string]*huma.MediaType{"application/json": {Schema: validationSchema}},
		}},
	}
	huma.Register(s.API, validate, s.validateEnvironmentBlueprint)
	apply := huma.Operation{
		OperationID: "blueprint.apply", Method: http.MethodPut, Path: "/environments/{id}/blueprint",
		Summary: "Apply an environment Blueprint", Tags: []string{"Environment"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectEnvironmentQuery}, SkipValidateBody: true,
		// Huma's raw-body limit treats the configured byte count as exclusive.
		// The lifecycle enforces the authoritative inclusive production limit.
		MaxBodyBytes: productionBlueprintBodyLimit + 1,
		RequestBody: &huma.RequestBody{
			Required: true,
			Content: map[string]*huma.MediaType{
				"multipart/form-data": {Schema: blueprintMultipartSchema()},
			},
		},
		Responses: map[string]*huma.Response{
			strconv.Itoa(http.StatusAccepted): {
				Description: http.StatusText(http.StatusAccepted),
				Content: map[string]*huma.MediaType{
					"application/json": {Schema: taskAcceptedSchema},
				},
			},
		},
	}
	huma.Register(s.API, apply, s.applyEnvironmentBlueprint)
	s.setRoutePolicy("POST /api/v1/environments/{id}/blueprint/validate", routePolicy{body: blueprintBody})
	s.setRoutePolicy("PUT /api/v1/environments/{id}/blueprint", routePolicy{body: blueprintBody})
}

func blueprintMultipartSchema() *huma.Schema {
	return &huma.Schema{
		Type:        "string",
		Format:      "binary",
		Description: "Deterministic Blueprint multipart stream: manifest first, then declared file parts in order.",
	}
}

func (s *Server) showEnvironmentBlueprint(
	ctx context.Context,
	request *environmentBlueprintShowInput,
) (*environmentBlueprintShowOutput, error) {
	if s.environmentBlueprints == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint service is not configured")
	}
	if ids.Validate(ids.KindEnvironment, request.ID) != nil {
		return nil, normalizeProjectError(errs.New(errs.KindValidationFailed, "Environment id is invalid"))
	}
	document, err := s.environmentBlueprints.GetBlueprint(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &environmentBlueprintShowOutput{ETag: quoteBlueprintRevision(document.Revision), Body: document}, nil
}

func (s *Server) validateEnvironmentBlueprint(
	ctx context.Context,
	request *environmentBlueprintValidateInput,
) (*environmentBlueprintValidationOutput, error) {
	if s.environmentBlueprints == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint service is not configured")
	}
	revision, err := parseBlueprintIfMatch(request.IfMatch)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	bundle, err := decodeEnvironmentBlueprintRequest(request.ID, request.ContentType, request.RawBody)
	defer clear(request.RawBody)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	defer clearBlueprintBundle(bundle)
	validation, err := s.environmentBlueprints.ValidateBlueprint(ctx, request.ID, bundle, revision)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &environmentBlueprintValidationOutput{Body: validation}, nil
}

func (s *Server) applyEnvironmentBlueprint(
	ctx context.Context,
	request *environmentBlueprintApplyInput,
) (*environmentMutationOutput, error) {
	if s.environmentBlueprints == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint service is not configured")
	}
	revision, err := parseBlueprintIfMatch(request.IfMatch)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	bundle, err := decodeEnvironmentBlueprintRequest(request.ID, request.ContentType, request.RawBody)
	defer clear(request.RawBody)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	defer clearBlueprintBundle(bundle)
	response, err := s.environmentBlueprints.ApplyBlueprint(
		ctx, request.ID, bundle, revision, request.IdempotencyKey,
	)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Error("controller: apply Environment Blueprint", "environment_id", request.ID, "error", err)
		}
		return nil, normalizeProjectError(err)
	}
	return s.environmentMutationResponse(response, "apply Blueprint"), nil
}

func decodeEnvironmentBlueprintRequest(id, contentType string, rawBody []byte) (core.BlueprintBundle, error) {
	if ids.Validate(ids.KindEnvironment, id) != nil {
		return core.BlueprintBundle{}, errs.New(errs.KindValidationFailed, "Environment id is invalid")
	}
	multipartRequest := &http.Request{
		Header: http.Header{"Content-Type": []string{contentType}},
		Body:   io.NopCloser(bytes.NewReader(rawBody)),
	}
	return decodeBlueprintMultipart(multipartRequest)
}

func parseBlueprintIfMatch(value string) (string, error) {
	if len(value) < 3 || value[0] != '"' || value[len(value)-1] != '"' {
		return "", errs.New(errs.KindValidationFailed, "If-Match must contain one quoted Blueprint revision")
	}
	revision := value[1 : len(value)-1]
	if revision != apiTypes.EnvironmentBlueprintInitialRevision && ids.Validate(ids.KindTask, revision) != nil {
		return "", errs.New(errs.KindValidationFailed, "If-Match Blueprint revision is invalid")
	}
	return revision, nil
}

func quoteBlueprintRevision(revision string) string {
	return "\"" + revision + "\""
}

func clearBlueprintBundle(bundle core.BlueprintBundle) {
	for index := range bundle.Files {
		clear(bundle.Files[index].Content)
	}
}
