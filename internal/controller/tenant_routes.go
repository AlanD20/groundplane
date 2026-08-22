package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/controller/hierarchy"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
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

func (s *Server) tenantCreate(w http.ResponseWriter, r *http.Request) {
	if s.tenantMutations == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Tenant mutator is not configured"))
		return
	}
	input, err := decodeTenantCreate(r)
	if err != nil {
		s.writeTenantProblem(w, err)
		return
	}
	response, err := s.tenantMutations.CreateTenant(
		r.Context(),
		input,
		r.Header.Get(idempotencyKeyHeader),
	)
	if err != nil {
		s.writeTenantProblem(w, err)
		return
	}
	w.Header().Set("Content-Type", response.ContentKind)
	w.WriteHeader(response.Status)
	if _, err := w.Write(response.Body); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Tenant creation response", slog.Any("error", err))
	}
}

func (s *Server) tenantEdit(w http.ResponseWriter, r *http.Request) {
	if s.tenantChanges == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Tenant changer is not configured"))
		return
	}
	input, err := decodeTenantEdit(r)
	if err != nil {
		s.writeTenantProblem(w, err)
		return
	}
	response, err := s.tenantChanges.EditTenant(
		r.Context(), r.PathValue("id"), input, r.Header.Get(idempotencyKeyHeader),
	)
	if err != nil {
		s.writeTenantProblem(w, err)
		return
	}
	s.writeTenantMutationResponse(w, response, "edit")
}

func (s *Server) tenantRename(w http.ResponseWriter, r *http.Request) {
	if s.tenantChanges == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Tenant changer is not configured"))
		return
	}
	input, err := decodeTenantRename(r)
	if err != nil {
		s.writeTenantProblem(w, err)
		return
	}
	response, err := s.tenantChanges.RenameTenant(
		r.Context(), r.PathValue("id"), input, r.Header.Get(idempotencyKeyHeader),
	)
	if err != nil {
		s.writeTenantProblem(w, err)
		return
	}
	s.writeTenantMutationResponse(w, response, "rename")
}

func (s *Server) writeTenantMutationResponse(w http.ResponseWriter, response etcd.IdempotencyResponse, action string) {
	w.Header().Set("Content-Type", response.ContentKind)
	w.WriteHeader(response.Status)
	if _, err := w.Write(response.Body); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Tenant mutation response", "action", action, slog.Any("error", err))
	}
}

func decodeTenantEdit(r *http.Request) (hierarchy.EditTenantInput, error) {
	values, err := decodeTenantChangeBody(r, map[string]struct{}{"name": {}, "description": {}})
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

func decodeTenantRename(r *http.Request) (hierarchy.RenameTenantInput, error) {
	values, err := decodeTenantChangeBody(r, map[string]struct{}{"slug": {}})
	if err != nil {
		return hierarchy.RenameTenantInput{}, err
	}
	input := hierarchy.RenameTenantInput{Slug: values["slug"]}
	if err := hierarchy.ValidateTenantRenameInput(input); err != nil {
		return hierarchy.RenameTenantInput{}, err
	}
	return input, nil
}

func decodeTenantChangeBody(r *http.Request, allowed map[string]struct{}) (map[string]string, error) {
	if len(r.URL.Query()) != 0 {
		return nil, errs.New(errs.KindMalformedRequest, "Tenant mutation query is invalid")
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, errs.Wrap(errs.KindMalformedRequest, err)
	}
	defer clear(body)
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

func decodeTenantCreate(r *http.Request) (hierarchy.CreateTenantInput, error) {
	if len(r.URL.Query()) != 0 {
		return hierarchy.CreateTenantInput{}, errs.New(
			errs.KindMalformedRequest,
			"Tenant creation query is invalid",
		)
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return hierarchy.CreateTenantInput{}, errs.Wrap(errs.KindMalformedRequest, err)
	}
	defer clear(body)
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

func (s *Server) tenantList(w http.ResponseWriter, r *http.Request) {
	if s.tenants == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Tenant reader is not configured"))
		return
	}
	request, err := tenantPageRequest(r)
	if err != nil {
		s.writeTenantProblem(w, err)
		return
	}
	page, err := s.tenants.ListTenants(r.Context(), request)
	if err != nil {
		s.writeTenantProblem(w, err)
		return
	}
	response := apiTypes.Page[apiTypes.Tenant]{
		Items: make([]apiTypes.Tenant, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, stored := range page.Items {
		response.Items[index] = tenantAPI(stored.Record)
	}
	s.writeTenantJSON(w, response)
}

func (s *Server) tenantShow(w http.ResponseWriter, r *http.Request) {
	if s.tenants == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Tenant reader is not configured"))
		return
	}
	if len(r.URL.Query()) != 0 {
		s.writeTenantProblem(w, errs.New(errs.KindMalformedRequest, "Tenant detail query is invalid"))
		return
	}
	stored, err := s.tenants.GetTenant(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeTenantProblem(w, err)
		return
	}
	s.writeTenantJSON(w, tenantAPI(stored.Record))
}

func tenantPageRequest(r *http.Request) (hierarchy.PageRequest, error) {
	query := r.URL.Query()
	for key, values := range query {
		if key != "limit" && key != "cursor" {
			return hierarchy.PageRequest{}, errs.New(errs.KindMalformedRequest, "Tenant pagination query is invalid")
		}
		if len(values) != 1 {
			return hierarchy.PageRequest{}, errs.New(errs.KindMalformedRequest, "Tenant pagination query is duplicated")
		}
	}
	request := hierarchy.PageRequest{Cursor: query.Get("cursor")}
	if raw := query.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil {
			return hierarchy.PageRequest{}, errs.New(errs.KindMalformedRequest, "Tenant pagination limit is invalid")
		}
		request.Limit = limit
	}
	return request, nil
}

func tenantAPI(record core.Tenant) apiTypes.Tenant {
	return apiTypes.Tenant{
		ID: record.ID, Slug: record.Slug, Name: record.Name, Description: record.Description,
	}
}

func (s *Server) writeTenantJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Tenant response", slog.Any("error", err))
	}
}

func (s *Server) writeTenantProblem(w http.ResponseWriter, err error) {
	var domainError *errs.Error
	if errors.As(err, &domainError) {
		s.writeProblem(w, domainError)
		return
	}
	s.writeProblem(w, errs.Wrap(errs.KindInternal, err))
}
