package controller

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"reflect"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

// EnvironmentBlueprintMutator applies one verified closed Blueprint bundle to
// an existing Environment and returns the accepted reconcile Task.
type EnvironmentBlueprintMutator interface {
	ApplyBlueprint(context.Context, string, core.BlueprintBundle, string) (etcd.IdempotencyResponse, error)
}

type environmentBlueprintApplyInput struct {
	ID             string `path:"id"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	ContentType    string `header:"Content-Type" hidden:"true"`
	RawBody        []byte `contentType:"multipart/form-data"`
}

func (s *Server) registerEnvironmentBlueprints() {
	taskAcceptedSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.TaskAccepted](),
		true,
		"TaskAccepted",
	)
	operation := huma.Operation{
		OperationID: "environment.apply", Method: http.MethodPut, Path: "/environments/{id}/blueprint",
		Summary: "Apply an environment Blueprint", Tags: []string{"Environment"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectEnvironmentQuery}, SkipValidateBody: true,
		// Huma's raw-body limit treats the configured byte count as exclusive.
		// The lifecycle enforces the authoritative inclusive production limit.
		MaxBodyBytes: productionBlueprintBodyLimit + 1,
		RequestBody: &huma.RequestBody{
			Required: true,
			Content: map[string]*huma.MediaType{
				"multipart/form-data": {Schema: &huma.Schema{
					Type:        "string",
					Format:      "binary",
					Description: "Deterministic Blueprint multipart stream: manifest first, then declared file parts in order.",
				}},
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
	huma.Register(s.API, operation, s.applyEnvironmentBlueprint)
	s.setRoutePolicy("PUT /api/v1/environments/{id}/blueprint", routePolicy{body: blueprintBody})
}

func (s *Server) applyEnvironmentBlueprint(
	ctx context.Context,
	request *environmentBlueprintApplyInput,
) (*environmentMutationOutput, error) {
	if s.environmentBlueprints == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint mutator is not configured")
	}
	defer clear(request.RawBody)
	if ids.Validate(ids.KindEnvironment, request.ID) != nil {
		return nil, normalizeProjectError(errs.New(errs.KindValidationFailed, "Environment id is invalid"))
	}
	multipartRequest := &http.Request{
		Header: http.Header{"Content-Type": []string{request.ContentType}},
		Body:   io.NopCloser(bytes.NewReader(request.RawBody)),
	}
	bundle, err := decodeBlueprintMultipart(multipartRequest)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	defer clearBlueprintBundle(bundle)
	response, err := s.environmentBlueprints.ApplyBlueprint(
		ctx, request.ID, bundle, request.IdempotencyKey,
	)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Error("controller: apply Environment Blueprint", "environment_id", request.ID, "error", err)
		}
		return nil, normalizeProjectError(err)
	}
	return s.environmentMutationResponse(response, "apply Blueprint"), nil
}

func clearBlueprintBundle(bundle core.BlueprintBundle) {
	for index := range bundle.Files {
		clear(bundle.Files[index].Content)
	}
}
