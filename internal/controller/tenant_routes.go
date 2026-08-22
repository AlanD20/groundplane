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

	"github.com/AlanD20/groundplane/internal/controller/hierarchy"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type TenantReader interface {
	GetTenant(context.Context, string) (hierarchy.Versioned[core.Tenant], error)
	ListTenants(context.Context, hierarchy.PageRequest) (hierarchy.Page[core.Tenant], error)
}

type TenantMutator interface {
	CreateTenant(context.Context, hierarchy.CreateTenantInput, string) (etcd.IdempotencyResponse, error)
}

type TenantChanger interface {
	EditTenant(context.Context, string, hierarchy.EditTenantInput, string) (etcd.IdempotencyResponse, error)
	RenameTenant(context.Context, string, hierarchy.RenameTenantInput, string) (etcd.IdempotencyResponse, error)
}

type tenantListInput struct {
	Limit  int    `query:"limit" required:"false"`
	Cursor string `query:"cursor" required:"false"`
}

type tenantShowInput struct {
	ID string `path:"id"`
}

type tenantCreateInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

type tenantEditInput struct {
	ID             string `path:"id"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

type tenantRenameInput struct {
	ID             string `path:"id"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

type tenantOutput struct {
	Body apiTypes.Tenant
}

type tenantPageOutput struct {
	Body apiTypes.TenantPage
}

type tenantMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerTenants() {
	tenantSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.Tenant](),
		true,
		"Tenant",
	)
	registerTenantMutation[apiTypes.TenantCreate](
		s,
		huma.Operation{
			OperationID: "tenant.create", Method: http.MethodPost, Path: "/tenants",
			Summary: "Create a tenant", Tags: []string{"Tenant"}, DefaultStatus: http.StatusCreated,
			Middlewares: huma.Middlewares{s.rejectTenantMutationQuery},
		},
		tenantSchema,
		s.createTenant,
		"TenantCreate",
	)
	huma.Register(s.API, huma.Operation{
		OperationID: "tenant.list", Method: http.MethodGet, Path: "/tenants",
		Summary: "List tenants", Tags: []string{"Tenant"},
		Middlewares: huma.Middlewares{s.validateTenantListQuery},
	}, s.listTenants)
	huma.Register(s.API, huma.Operation{
		OperationID: "tenant.show", Method: http.MethodGet, Path: "/tenants/{id}",
		Summary: "Show a tenant", Tags: []string{"Tenant"},
		Middlewares: huma.Middlewares{s.rejectTenantMutationQuery},
	}, s.showTenant)
	registerTenantMutation[apiTypes.TenantEdit](
		s,
		huma.Operation{
			OperationID: "tenant.edit", Method: http.MethodPatch, Path: "/tenants/{id}",
			Summary: "Edit a tenant", Tags: []string{"Tenant"}, DefaultStatus: http.StatusOK,
			Middlewares: huma.Middlewares{s.rejectTenantMutationQuery},
		},
		tenantSchema,
		s.editTenant,
		"TenantEdit",
	)
	registerTenantMutation[apiTypes.TenantRename](
		s,
		huma.Operation{
			OperationID: "tenant.rename", Method: http.MethodPost, Path: "/tenants/{id}/rename",
			Summary: "Rename a tenant slug", Tags: []string{"Tenant"}, DefaultStatus: http.StatusOK,
			Middlewares: huma.Middlewares{s.rejectTenantMutationQuery},
		},
		tenantSchema,
		s.renameTenant,
		"TenantRename",
	)

	for _, pattern := range []string{
		"POST /api/v1/tenants",
		"PATCH /api/v1/tenants/{id}",
		"POST /api/v1/tenants/{id}/rename",
	} {
		s.setRoutePolicy(pattern, routePolicy{body: jsonBody})
	}
}

func registerTenantMutation[InputBody any, Input any](
	s *Server,
	operation huma.Operation,
	tenantSchema *huma.Schema,
	handler func(context.Context, *Input) (*tenantMutationOutput, error),
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
				"application/json": {Schema: tenantSchema},
			},
		},
	}
	huma.Register(s.API, operation, handler)
}

func (s *Server) validateTenantListQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	query := requestURL.Query()
	for key, values := range query {
		if key != "limit" && key != "cursor" {
			s.writeTenantHumaProblem(ctx, "Tenant pagination query is invalid")
			return
		}
		if len(values) != 1 {
			s.writeTenantHumaProblem(ctx, "Tenant pagination query is duplicated")
			return
		}
	}
	if raw := query.Get("limit"); raw != "" {
		if _, err := strconv.Atoi(raw); err != nil {
			s.writeTenantHumaProblem(ctx, "Tenant pagination limit is invalid")
			return
		}
	}
	next(ctx)
}

func (s *Server) rejectTenantMutationQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	if len(requestURL.Query()) != 0 {
		s.writeTenantHumaProblem(ctx, "Tenant request query is invalid")
		return
	}
	next(ctx)
}

func (s *Server) writeTenantHumaProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Tenant request problem", slog.Any("error", err))
	}
}

func (s *Server) createTenant(ctx context.Context, request *tenantCreateInput) (*tenantMutationOutput, error) {
	if s.tenantMutations == nil {
		return nil, errs.New(errs.KindInternal, "Tenant mutator is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeTenantCreate(request.RawBody)
	if err != nil {
		return nil, normalizeTenantError(err)
	}
	response, err := s.tenantMutations.CreateTenant(
		ctx,
		input,
		request.IdempotencyKey,
	)
	if err != nil {
		return nil, normalizeTenantError(err)
	}
	return s.tenantMutationResponse(response, "create"), nil
}

func (s *Server) editTenant(ctx context.Context, request *tenantEditInput) (*tenantMutationOutput, error) {
	if s.tenantChanges == nil {
		return nil, errs.New(errs.KindInternal, "Tenant changer is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeTenantEdit(request.RawBody)
	if err != nil {
		return nil, normalizeTenantError(err)
	}
	response, err := s.tenantChanges.EditTenant(
		ctx, request.ID, input, request.IdempotencyKey,
	)
	if err != nil {
		return nil, normalizeTenantError(err)
	}
	return s.tenantMutationResponse(response, "edit"), nil
}

func (s *Server) renameTenant(ctx context.Context, request *tenantRenameInput) (*tenantMutationOutput, error) {
	if s.tenantChanges == nil {
		return nil, errs.New(errs.KindInternal, "Tenant changer is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeTenantRename(request.RawBody)
	if err != nil {
		return nil, normalizeTenantError(err)
	}
	response, err := s.tenantChanges.RenameTenant(
		ctx, request.ID, input, request.IdempotencyKey,
	)
	if err != nil {
		return nil, normalizeTenantError(err)
	}
	return s.tenantMutationResponse(response, "rename"), nil
}

func (s *Server) tenantMutationResponse(
	response etcd.IdempotencyResponse,
	action string,
) *tenantMutationOutput {
	return &tenantMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
				s.Logger.Error(
					"controller: write Tenant mutation response",
					"action",
					action,
					slog.Any("error", err),
				)
			}
		},
	}
}

func decodeTenantEdit(body []byte) (hierarchy.EditTenantInput, error) {
	values, err := decodeTenantChangeBody(body, map[string]struct{}{"name": {}, "description": {}})
	if err != nil {
		return hierarchy.EditTenantInput{}, err
	}
	input := hierarchy.EditTenantInput{}
	if value, ok := values["name"]; ok {
		input.Name = &value
	}
	if value, ok := values["description"]; ok {
		input.Description = &value
	}
	if err := hierarchy.ValidateTenantEditInput(input); err != nil {
		return hierarchy.EditTenantInput{}, err
	}
	return input, nil
}

func decodeTenantRename(body []byte) (hierarchy.RenameTenantInput, error) {
	values, err := decodeTenantChangeBody(body, map[string]struct{}{"slug": {}})
	if err != nil {
		return hierarchy.RenameTenantInput{}, err
	}
	input := hierarchy.RenameTenantInput{Slug: values["slug"]}
	if err := hierarchy.ValidateTenantRenameInput(input); err != nil {
		return hierarchy.RenameTenantInput{}, err
	}
	return input, nil
}

func decodeTenantChangeBody(body []byte, allowed map[string]struct{}) (map[string]string, error) {
	if !utf8.Valid(body) {
		return nil, errs.New(errs.KindMalformedRequest, "Tenant mutation body is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return nil, tenantCreateJSONError(err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return nil, errs.New(errs.KindMalformedRequest, "Tenant mutation body must be an object")
	}
	values := make(map[string]string, len(allowed))
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, tenantCreateJSONError(err)
		}
		key, ok := token.(string)
		if !ok {
			return nil, errs.New(errs.KindMalformedRequest, "Tenant mutation member name is invalid")
		}
		if _, duplicate := values[key]; duplicate {
			return nil, errs.New(errs.KindMalformedRequest, "Tenant mutation body contains a duplicate member")
		}
		if _, ok := allowed[key]; !ok {
			return nil, errs.New(errs.KindMalformedRequest, "Tenant mutation body contains an unknown member")
		}
		var value string
		if err := decoder.Decode(&value); err != nil {
			var typeError *json.UnmarshalTypeError
			if errors.As(err, &typeError) {
				return nil, errs.New(errs.KindValidationFailed, "Tenant mutation members must be strings")
			}
			return nil, tenantCreateJSONError(err)
		}
		values[key] = value
	}
	closing, err := decoder.Token()
	if err != nil {
		return nil, tenantCreateJSONError(err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return nil, errs.New(errs.KindMalformedRequest, "Tenant mutation body is malformed")
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return nil, tenantCreateJSONError(err)
	}
	return values, nil
}

func decodeTenantCreate(body []byte) (hierarchy.CreateTenantInput, error) {
	if !utf8.Valid(body) {
		return hierarchy.CreateTenantInput{}, errs.New(
			errs.KindMalformedRequest,
			"Tenant creation body is not valid UTF-8",
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return hierarchy.CreateTenantInput{}, tenantCreateJSONError(err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return hierarchy.CreateTenantInput{}, errs.New(
			errs.KindMalformedRequest,
			"Tenant creation body must be an object",
		)
	}
	input := hierarchy.CreateTenantInput{}
	seen := make(map[string]struct{}, 3)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return hierarchy.CreateTenantInput{}, tenantCreateJSONError(err)
		}
		key, ok := token.(string)
		if !ok {
			return hierarchy.CreateTenantInput{}, errs.New(
				errs.KindMalformedRequest,
				"Tenant creation member name is invalid",
			)
		}
		if _, duplicate := seen[key]; duplicate {
			return hierarchy.CreateTenantInput{}, errs.New(
				errs.KindMalformedRequest,
				"Tenant creation body contains a duplicate member",
			)
		}
		seen[key] = struct{}{}
		if key != "slug" && key != "name" && key != "description" {
			return hierarchy.CreateTenantInput{}, errs.New(
				errs.KindMalformedRequest,
				"Tenant creation body contains an unknown member",
			)
		}
		var value string
		if err := decoder.Decode(&value); err != nil {
			var typeError *json.UnmarshalTypeError
			if errors.As(err, &typeError) {
				return hierarchy.CreateTenantInput{}, errs.New(
					errs.KindValidationFailed,
					"Tenant creation members must be strings",
				)
			}
			return hierarchy.CreateTenantInput{}, tenantCreateJSONError(err)
		}
		switch key {
		case "slug":
			input.Slug = value
		case "name":
			input.Name = &value
		case "description":
			input.Description = value
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return hierarchy.CreateTenantInput{}, tenantCreateJSONError(err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return hierarchy.CreateTenantInput{}, errs.New(
			errs.KindMalformedRequest,
			"Tenant creation body is malformed",
		)
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return hierarchy.CreateTenantInput{}, tenantCreateJSONError(err)
	}
	return input, nil
}

func tenantCreateJSONError(err error) error {
	return errs.Wrap(errs.KindMalformedRequest, err)
}

func (s *Server) listTenants(
	ctx context.Context,
	request *tenantListInput,
) (*tenantPageOutput, error) {
	if s.tenants == nil {
		return nil, errs.New(errs.KindInternal, "Tenant reader is not configured")
	}
	page, err := s.tenants.ListTenants(
		ctx,
		hierarchy.PageRequest{Limit: request.Limit, Cursor: request.Cursor},
	)
	if err != nil {
		return nil, normalizeTenantError(err)
	}
	response := apiTypes.TenantPage{
		Items: make([]apiTypes.Tenant, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, stored := range page.Items {
		response.Items[index] = tenantAPI(stored.Record)
	}
	return &tenantPageOutput{Body: response}, nil
}

func (s *Server) showTenant(ctx context.Context, request *tenantShowInput) (*tenantOutput, error) {
	if s.tenants == nil {
		return nil, errs.New(errs.KindInternal, "Tenant reader is not configured")
	}
	stored, err := s.tenants.GetTenant(ctx, request.ID)
	if err != nil {
		return nil, normalizeTenantError(err)
	}
	return &tenantOutput{Body: tenantAPI(stored.Record)}, nil
}

func tenantAPI(record core.Tenant) apiTypes.Tenant {
	return apiTypes.Tenant{
		ID: record.ID, Slug: record.Slug, Name: record.Name, Description: record.Description,
	}
}

func normalizeTenantError(err error) error {
	var domainError *errs.Error
	if errors.As(err, &domainError) {
		return domainError
	}
	return errs.Wrap(errs.KindInternal, err)
}
