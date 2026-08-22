package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/controller/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type ProjectChanger interface {
	EditProject(context.Context, string, hierarchy.EditProjectInput, string) (etcd.IdempotencyResponse, error)
	RenameProject(context.Context, string, hierarchy.RenameProjectInput, string) (etcd.IdempotencyResponse, error)
}

func (s *Server) editProject(ctx context.Context, request *projectEditInput) (*projectMutationOutput, error) {
	if s.projectChanges == nil {
		return nil, errs.New(errs.KindInternal, "Project changer is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeProjectEdit(request.RawBody)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response, err := s.projectChanges.EditProject(
		ctx, request.ID, input, request.IdempotencyKey,
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.projectMutationResponse(response, "edit"), nil
}

func (s *Server) renameProject(ctx context.Context, request *projectRenameInput) (*projectMutationOutput, error) {
	if s.projectChanges == nil {
		return nil, errs.New(errs.KindInternal, "Project changer is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeProjectRename(request.RawBody)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response, err := s.projectChanges.RenameProject(
		ctx, request.ID, input, request.IdempotencyKey,
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.projectMutationResponse(response, "rename"), nil
}

func (s *Server) projectMutationResponse(
	response etcd.IdempotencyResponse,
	action string,
) *projectMutationOutput {
	return &projectMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
				s.Logger.Error(
					"controller: write Project mutation response",
					"action",
					action,
					slog.Any("error", err),
				)
			}
		},
	}
}

func decodeProjectEdit(body []byte) (hierarchy.EditProjectInput, error) {
	values, err := decodeProjectChangeBody(body, map[string]struct{}{"name": {}})
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

func decodeProjectRename(body []byte) (hierarchy.RenameProjectInput, error) {
	values, err := decodeProjectChangeBody(body, map[string]struct{}{"slug": {}})
	if err != nil {
		return hierarchy.RenameProjectInput{}, err
	}
	input := hierarchy.RenameProjectInput{Slug: values["slug"]}
	if err := hierarchy.ValidateProjectRenameInput(input); err != nil {
		return hierarchy.RenameProjectInput{}, err
	}
	return input, nil
}

func decodeProjectChangeBody(body []byte, allowed map[string]struct{}) (map[string]string, error) {
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
