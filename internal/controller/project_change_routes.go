package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/controller/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ProjectChanger interface {
	EditProject(context.Context, string, hierarchy.EditProjectInput, string) (etcd.IdempotencyResponse, error)
	RenameProject(context.Context, string, hierarchy.RenameProjectInput, string) (etcd.IdempotencyResponse, error)
}

func (s *Server) projectEdit(w http.ResponseWriter, r *http.Request) {
	if s.projectChanges == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Project changer is not configured"))
		return
	}
	input, err := decodeProjectEdit(r)
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	response, err := s.projectChanges.EditProject(
		r.Context(), r.PathValue("id"), input, r.Header.Get(idempotencyKeyHeader),
	)
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	s.writeProjectMutationResponse(w, response, "edit")
}

func (s *Server) projectRename(w http.ResponseWriter, r *http.Request) {
	if s.projectChanges == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Project changer is not configured"))
		return
	}
	input, err := decodeProjectRename(r)
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	response, err := s.projectChanges.RenameProject(
		r.Context(), r.PathValue("id"), input, r.Header.Get(idempotencyKeyHeader),
	)
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	s.writeProjectMutationResponse(w, response, "rename")
}

func (s *Server) writeProjectMutationResponse(
	w http.ResponseWriter,
	response etcd.IdempotencyResponse,
	action string,
) {
	w.Header().Set("Content-Type", response.ContentKind)
	w.WriteHeader(response.Status)
	if _, err := w.Write(response.Body); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Project mutation response", "action", action, slog.Any("error", err))
	}
}

func decodeProjectEdit(r *http.Request) (hierarchy.EditProjectInput, error) {
	values, err := decodeProjectChangeBody(r, map[string]struct{}{"name": {}})
	if err != nil {
		return hierarchy.EditProjectInput{}, err
	}
	input := hierarchy.EditProjectInput{}
	if value, ok := values["name"]; ok {
		input.Name = &value
	}
	if err := hierarchy.ValidateProjectEditInput(input); err != nil {
		return hierarchy.EditProjectInput{}, err
	}
	return input, nil
}

func decodeProjectRename(r *http.Request) (hierarchy.RenameProjectInput, error) {
	values, err := decodeProjectChangeBody(r, map[string]struct{}{"slug": {}})
	if err != nil {
		return hierarchy.RenameProjectInput{}, err
	}
	input := hierarchy.RenameProjectInput{Slug: values["slug"]}
	if err := hierarchy.ValidateProjectRenameInput(input); err != nil {
		return hierarchy.RenameProjectInput{}, err
	}
	return input, nil
}

func decodeProjectChangeBody(r *http.Request, allowed map[string]struct{}) (map[string]string, error) {
	if len(r.URL.Query()) != 0 {
		return nil, errs.New(errs.KindMalformedRequest, "Project mutation query is invalid")
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, errs.Wrap(errs.KindMalformedRequest, err)
	}
	defer clear(body)
	if !utf8.Valid(body) {
		return nil, errs.New(errs.KindMalformedRequest, "Project mutation body is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return nil, projectCreateJSONError(err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return nil, errs.New(errs.KindMalformedRequest, "Project mutation body must be an object")
	}
	values := make(map[string]string, len(allowed))
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, projectCreateJSONError(err)
		}
		key, ok := token.(string)
		if !ok {
			return nil, errs.New(errs.KindMalformedRequest, "Project mutation member name is invalid")
		}
		if _, duplicate := values[key]; duplicate {
			return nil, errs.New(errs.KindMalformedRequest, "Project mutation body contains a duplicate member")
		}
		if _, ok := allowed[key]; !ok {
			return nil, errs.New(errs.KindMalformedRequest, "Project mutation body contains an unknown member")
		}
		var value string
		if err := decoder.Decode(&value); err != nil {
			var typeError *json.UnmarshalTypeError
			if errors.As(err, &typeError) {
				return nil, errs.New(errs.KindValidationFailed, "Project mutation members must be strings")
			}
			return nil, projectCreateJSONError(err)
		}
		values[key] = value
	}
	closing, err := decoder.Token()
	if err != nil {
		return nil, projectCreateJSONError(err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return nil, errs.New(errs.KindMalformedRequest, "Project mutation body is malformed")
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return nil, projectCreateJSONError(err)
	}
	return values, nil
}
