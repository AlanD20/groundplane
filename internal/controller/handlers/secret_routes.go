package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type SecretReader interface {
	GetSecret(context.Context, string) (etcd.Versioned[secretrecord.Record], error)
	ListSecrets(context.Context, core.SecretScope, string, etcd.PageRequest) (etcd.Page[secretrecord.Record], error)
	RevealSecret(context.Context, string) (string, error)
}

type SecretMutator interface {
	CreateSecret(
		context.Context,
		apiTypes.SecretCreateRequest,
		string,
	) (etcd.IdempotencyResponse, error)
}

type SecretDeleter interface {
	DeleteSecret(context.Context, string, string) (etcd.IdempotencyResponse, error)
}

type secretCreateInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

type secretListInput struct {
	Project  string `query:"project"  required:"false" pattern:"^prj_[0-9A-HJKMNP-TV-Z]{26}$"`
	Platform bool   `query:"platform" required:"false"`
	Limit    int    `query:"limit"    required:"false"`
	Cursor   string `query:"cursor"   required:"false"`
}

type secretShowInput struct {
	ID string `path:"id" pattern:"^sec_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type secretRemoveInput struct {
	ID             string `path:"id" pattern:"^sec_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

type secretPageOutput struct {
	Body apiTypes.Page[apiTypes.Secret]
}

type secretOutput struct {
	Body apiTypes.Secret
}

type secretValueOutput struct {
	Body apiTypes.SecretValue
}

type secretMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerSecrets() {
	secretSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.Secret](),
		true,
		"Secret",
	)
	taskAcceptedSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.TaskAccepted](),
		true,
		"TaskAccepted",
	)
	registerSecretMutation[apiTypes.SecretCreateRequest](
		s,
		huma.Operation{
			OperationID: "secret.create", Method: http.MethodPost, Path: "/secrets",
			Summary: "Create a reusable secret", Tags: []string{"Secret"},
			DefaultStatus: http.StatusCreated,
			Middlewares:   huma.Middlewares{s.rejectSecretQuery},
		},
		secretSchema,
		s.createSecret,
	)
	huma.Register(s.API, huma.Operation{
		OperationID: "secret.list", Method: http.MethodGet, Path: "/secrets",
		Summary: "List reusable secrets", Tags: []string{"Secret"},
		Middlewares: huma.Middlewares{s.validateSecretListQuery},
	}, s.listSecrets)
	huma.Register(s.API, huma.Operation{
		OperationID: "secret.show", Method: http.MethodGet, Path: "/secrets/{id}",
		Summary: "Show reusable secret metadata", Tags: []string{"Secret"},
	}, s.showSecret)
	huma.Register(s.API, huma.Operation{
		OperationID: "secret.reveal", Method: http.MethodGet, Path: "/secrets/{id}/value",
		Summary: "Reveal a reusable secret value", Tags: []string{"Secret"},
	}, s.revealSecret)
	huma.Register(s.API, huma.Operation{
		OperationID: "secret.remove", Method: http.MethodDelete, Path: "/secrets/{id}",
		Summary: "Remove a reusable secret", Tags: []string{"Secret"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectSecretDeleteBody, s.rejectSecretQuery},
		Responses:   attachMutationResponses(taskAcceptedSchema),
	}, s.removeSecret)
	s.setRoutePolicy("POST /api/v1/secrets", routePolicy{body: jsonBody})
}

func (s *Server) rejectSecretDeleteBody(ctx huma.Context, next func(huma.Context)) {
	var probe [1]byte
	count, err := ctx.BodyReader().Read(probe[:])
	if count != 0 || (err != nil && !errors.Is(err, io.EOF)) {
		if writeErr := huma.WriteErr(
			s.API,
			ctx,
			http.StatusBadRequest,
			"Secret deletion body is not allowed",
		); writeErr != nil && s.Logger != nil {
			s.Logger.Error("controller: write Secret request problem", slog.Any("error", writeErr))
		}
		return
	}
	next(ctx)
}

func (s *Server) removeSecret(
	ctx context.Context,
	request *secretRemoveInput,
) (*secretMutationOutput, error) {
	if s.secretDeletions == nil {
		return nil, errs.New(errs.KindInternal, "Secret deleter is not configured")
	}
	response, err := s.secretDeletions.DeleteSecret(ctx, request.ID, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &secretMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, writeErr := ctx.BodyWriter().Write(response.Body); writeErr != nil && s.Logger != nil {
				s.Logger.Error("controller: write Secret deletion response", slog.Any("error", writeErr))
			}
		},
	}, nil
}

func registerSecretMutation[InputBody any, Input any](
	s *Server,
	operation huma.Operation,
	secretSchema *huma.Schema,
	handler func(context.Context, *Input) (*secretMutationOutput, error),
) {
	operation.SkipValidateBody = true
	operation.RequestBody = &huma.RequestBody{
		Required: true,
		Content: map[string]*huma.MediaType{
			"application/json": {
				Schema: s.API.OpenAPI().Components.Schemas.Schema(
					reflect.TypeFor[InputBody](),
					true,
					"SecretCreateRequest",
				),
			},
		},
	}
	operation.Responses = map[string]*huma.Response{
		strconv.Itoa(operation.DefaultStatus): {
			Description: http.StatusText(operation.DefaultStatus),
			Content: map[string]*huma.MediaType{
				"application/json": {Schema: secretSchema},
			},
		},
	}
	huma.Register(s.API, operation, handler)
}

func (s *Server) createSecret(
	ctx context.Context,
	request *secretCreateInput,
) (*secretMutationOutput, error) {
	if s.secretMutations == nil {
		return nil, errs.New(errs.KindInternal, "Secret mutator is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeSecretCreate(request.RawBody)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response, err := s.secretMutations.CreateSecret(ctx, input, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &secretMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, writeErr := ctx.BodyWriter().Write(response.Body); writeErr != nil && s.Logger != nil {
				s.Logger.Error("controller: write Secret creation response", slog.Any("error", writeErr))
			}
		},
	}, nil
}

func decodeSecretCreate(body []byte) (apiTypes.SecretCreateRequest, error) {
	if !utf8.Valid(body) {
		return apiTypes.SecretCreateRequest{}, errs.New(
			errs.KindMalformedRequest,
			"Secret creation body is not valid UTF-8",
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return apiTypes.SecretCreateRequest{}, secretCreateJSONError(err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return apiTypes.SecretCreateRequest{}, errs.New(
			errs.KindMalformedRequest,
			"Secret creation body must be an object",
		)
	}
	input := apiTypes.SecretCreateRequest{}
	seen := make(map[string]struct{}, 6)
	for decoder.More() {
		token, tokenErr := decoder.Token()
		if tokenErr != nil {
			return apiTypes.SecretCreateRequest{}, secretCreateJSONError(tokenErr)
		}
		key, ok := token.(string)
		if !ok {
			return apiTypes.SecretCreateRequest{}, errs.New(
				errs.KindMalformedRequest,
				"Secret creation member name is invalid",
			)
		}
		if _, duplicate := seen[key]; duplicate {
			return apiTypes.SecretCreateRequest{}, errs.New(
				errs.KindMalformedRequest,
				"Secret creation body contains a duplicate member",
			)
		}
		seen[key] = struct{}{}
		switch key {
		case "platform":
			if err := decoder.Decode(&input.Platform); err != nil {
				return apiTypes.SecretCreateRequest{}, secretCreateMemberError(err)
			}
		case "project_id", "key", "kind", "path", "value":
			var value string
			if err := decoder.Decode(&value); err != nil {
				return apiTypes.SecretCreateRequest{}, secretCreateMemberError(err)
			}
			switch key {
			case "project_id":
				input.ProjectID = value
			case "key":
				input.Key = value
			case "kind":
				input.Kind = value
			case "path":
				input.Path = value
			case "value":
				input.Value = value
			}
		default:
			return apiTypes.SecretCreateRequest{}, errs.New(
				errs.KindMalformedRequest,
				"Secret creation body contains an unknown member",
			)
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return apiTypes.SecretCreateRequest{}, secretCreateJSONError(err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return apiTypes.SecretCreateRequest{}, errs.New(
			errs.KindMalformedRequest,
			"Secret creation body is malformed",
		)
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return apiTypes.SecretCreateRequest{}, secretCreateJSONError(err)
	}
	return input, nil
}

func secretCreateMemberError(err error) error {
	var typeError *json.UnmarshalTypeError
	if errors.As(err, &typeError) {
		return errs.New(
			errs.KindValidationFailed,
			"Secret creation member has an invalid type",
		)
	}
	return secretCreateJSONError(err)
}

func secretCreateJSONError(err error) error {
	return errs.Wrap(errs.KindMalformedRequest, err)
}

func (s *Server) listSecrets(ctx context.Context, request *secretListInput) (*secretPageOutput, error) {
	if s.secrets == nil {
		return nil, errs.New(errs.KindInternal, "Secret reader is not configured")
	}
	scope, projectID, pageRequest, err := secretListRequest(
		request.Project,
		request.Platform,
		request.Limit,
		request.Cursor,
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	page, err := s.secrets.ListSecrets(ctx, scope, projectID, pageRequest)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response := apiTypes.Page[apiTypes.Secret]{
		Items: make([]apiTypes.Secret, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = secretResponse(item.Record)
	}
	return &secretPageOutput{Body: response}, nil
}

func (s *Server) showSecret(ctx context.Context, request *secretShowInput) (*secretOutput, error) {
	if s.secrets == nil {
		return nil, errs.New(errs.KindInternal, "Secret reader is not configured")
	}
	stored, err := s.secrets.GetSecret(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &secretOutput{Body: secretResponse(stored.Record)}, nil
}

func (s *Server) revealSecret(ctx context.Context, request *secretShowInput) (*secretValueOutput, error) {
	if s.secrets == nil {
		return nil, errs.New(errs.KindInternal, "Secret reader is not configured")
	}
	value, err := s.secrets.RevealSecret(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &secretValueOutput{Body: apiTypes.SecretValue{Value: value}}, nil
}

func secretListRequest(
	projectID string,
	platform bool,
	limit int,
	cursor string,
) (core.SecretScope, string, etcd.PageRequest, error) {
	if projectID != "" && platform || projectID == "" && !platform {
		return "", "", etcd.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Secret list requires exactly one owner selector",
		)
	}
	if projectID != "" && ids.Validate(ids.KindProject, projectID) != nil {
		return "", "", etcd.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Secret list requires a stable Project id",
		)
	}
	if limit < 0 {
		return "", "", etcd.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Secret list limit must be a positive integer",
		)
	}
	if platform {
		return core.SecretScopePlatform, "", etcd.PageRequest{Limit: limit, Cursor: cursor}, nil
	}
	return core.SecretScopeProject, projectID, etcd.PageRequest{Limit: limit, Cursor: cursor}, nil
}

func secretResponse(record secretrecord.Record) apiTypes.Secret {
	secret := record.Secret
	return apiTypes.Secret{
		ID: secret.ID, Scope: string(secret.Scope), ProjectID: secret.ProjectID,
		Key: secret.Key, Kind: string(secret.Kind), Ref: secret.Ref,
		UpdatedAt: secret.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func (s *Server) validateSecretListQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	query := requestURL.Query()
	for key, values := range query {
		if key != "project" && key != "platform" && key != "limit" && key != "cursor" {
			s.writeSecretProblem(ctx, "Secret list query is invalid")
			return
		}
		if len(values) != 1 {
			s.writeSecretProblem(ctx, "Secret list query contains duplicate values")
			return
		}
	}
	projectValues, hasProject := query["project"]
	platformValues, hasPlatform := query["platform"]
	if hasProject == hasPlatform || hasProject && projectValues[0] == "" ||
		hasPlatform && platformValues[0] != "true" {
		s.writeSecretProblem(ctx, "Secret list requires exactly one owner selector")
		return
	}
	next(ctx)
}

func (s *Server) rejectSecretQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	if len(requestURL.Query()) != 0 {
		s.writeSecretProblem(ctx, "Secret request query is invalid")
		return
	}
	next(ctx)
}

func (s *Server) writeSecretProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Secret request problem", slog.Any("error", err))
	}
}
