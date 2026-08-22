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

type ProjectReader interface {
	GetProject(context.Context, string) (hierarchy.Versioned[core.Project], error)
	ListAllProjects(context.Context, hierarchy.ProjectFilter, hierarchy.PageRequest) (hierarchy.Page[core.Project], error)
}

type ProjectMutator interface {
	CreateProject(context.Context, hierarchy.CreateProjectInput, string) (etcd.IdempotencyResponse, error)
}

func (s *Server) projectCreate(w http.ResponseWriter, r *http.Request) {
	if s.projectMutations == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Project mutator is not configured"))
		return
	}
	input, err := decodeProjectCreate(r)
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	response, err := s.projectMutations.CreateProject(
		r.Context(), input, r.Header.Get(idempotencyKeyHeader),
	)
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	w.Header().Set("Content-Type", response.ContentKind)
	w.WriteHeader(response.Status)
	if _, err := w.Write(response.Body); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Project creation response", slog.Any("error", err))
	}
}

func decodeProjectCreate(r *http.Request) (hierarchy.CreateProjectInput, error) {
	if len(r.URL.Query()) != 0 {
		return hierarchy.CreateProjectInput{}, errs.New(errs.KindMalformedRequest, "Project creation query is invalid")
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return hierarchy.CreateProjectInput{}, errs.Wrap(errs.KindMalformedRequest, err)
	}
	defer clear(body)
	if !utf8.Valid(body) {
		return hierarchy.CreateProjectInput{}, errs.New(
			errs.KindMalformedRequest, "Project creation body is not valid UTF-8",
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return hierarchy.CreateProjectInput{}, projectCreateJSONError(err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return hierarchy.CreateProjectInput{}, errs.New(errs.KindMalformedRequest, "Project creation body must be an object")
	}
	input := hierarchy.CreateProjectInput{}
	seen := make(map[string]struct{}, 4)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return hierarchy.CreateProjectInput{}, projectCreateJSONError(err)
		}
		key, ok := token.(string)
		if !ok {
			return hierarchy.CreateProjectInput{}, errs.New(errs.KindMalformedRequest, "Project creation member name is invalid")
		}
		if _, duplicate := seen[key]; duplicate {
			return hierarchy.CreateProjectInput{}, errs.New(
				errs.KindMalformedRequest, "Project creation body contains a duplicate member",
			)
		}
		seen[key] = struct{}{}
		if key != "tenant_id" && key != "slug" && key != "name" && key != "description" {
			return hierarchy.CreateProjectInput{}, errs.New(
				errs.KindMalformedRequest, "Project creation body contains an unknown member",
			)
		}
		var value string
		if err := decoder.Decode(&value); err != nil {
			var typeError *json.UnmarshalTypeError
			if errors.As(err, &typeError) {
				return hierarchy.CreateProjectInput{}, errs.New(
					errs.KindValidationFailed, "Project creation members must be strings",
				)
			}
			return hierarchy.CreateProjectInput{}, projectCreateJSONError(err)
		}
		switch key {
		case "tenant_id":
			input.TenantID = value
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
		return hierarchy.CreateProjectInput{}, projectCreateJSONError(err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return hierarchy.CreateProjectInput{}, errs.New(errs.KindMalformedRequest, "Project creation body is malformed")
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return hierarchy.CreateProjectInput{}, projectCreateJSONError(err)
	}
	return input, nil
}

func projectCreateJSONError(err error) error {
	return errs.Wrap(errs.KindMalformedRequest, err)
}

func (s *Server) projectList(w http.ResponseWriter, r *http.Request) {
	if s.projects == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Project reader is not configured"))
		return
	}
	filter, pageRequest, err := projectListRequest(r)
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	page, err := s.projects.ListAllProjects(r.Context(), filter, pageRequest)
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	response := apiTypes.Page[apiTypes.Project]{
		Items: make([]apiTypes.Project, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = projectAPI(item.Record)
	}
	s.writeProjectJSON(w, response)
}

func (s *Server) projectShow(w http.ResponseWriter, r *http.Request) {
	if s.projects == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Project reader is not configured"))
		return
	}
	if len(r.URL.Query()) != 0 {
		s.writeProjectProblem(w, errs.New(errs.KindMalformedRequest, "Project detail query is invalid"))
		return
	}
	stored, err := s.projects.GetProject(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	s.writeProjectJSON(w, projectAPI(stored.Record))
}

func projectListRequest(r *http.Request) (hierarchy.ProjectFilter, hierarchy.PageRequest, error) {
	query := r.URL.Query()
	for key, values := range query {
		if key != "kind" && key != "tenant" && key != "limit" && key != "cursor" {
			return hierarchy.ProjectFilter{}, hierarchy.PageRequest{}, errs.New(
				errs.KindMalformedRequest, "Project list query is invalid",
			)
		}
		if len(values) != 1 {
			return hierarchy.ProjectFilter{}, hierarchy.PageRequest{}, errs.New(
				errs.KindMalformedRequest, "Project list query is duplicated",
			)
		}
	}
	filter := hierarchy.ProjectFilter{
		TenantID: query.Get("tenant"), Kind: core.ProjectKind(query.Get("kind")),
	}
	request := hierarchy.PageRequest{Cursor: query.Get("cursor")}
	if raw := query.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil {
			return hierarchy.ProjectFilter{}, hierarchy.PageRequest{}, errs.New(
				errs.KindMalformedRequest, "Project pagination limit is invalid",
			)
		}
		request.Limit = limit
	}
	return filter, request, nil
}

func projectAPI(record core.Project) apiTypes.Project {
	return apiTypes.Project{
		ID: record.ID, TenantID: record.TenantID, Slug: record.Slug,
		Name: record.Name, Description: record.Description, Kind: string(record.Kind),
	}
}

func (s *Server) writeProjectJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Project response", slog.Any("error", err))
	}
}

func (s *Server) writeProjectProblem(w http.ResponseWriter, err error) {
	var domainError *errs.Error
	if errors.As(err, &domainError) {
		s.writeProblem(w, domainError)
		return
	}
	s.writeProblem(w, errs.Wrap(errs.KindInternal, err))
}
