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

type EnvironmentChanger interface {
	RenameEnvironment(
		context.Context,
		string,
		hierarchy.RenameEnvironmentInput,
		string,
	) (etcd.IdempotencyResponse, error)
}

func (s *Server) environmentRename(w http.ResponseWriter, r *http.Request) {
	if s.environmentChanges == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Environment changer is not configured"))
		return
	}
	input, err := decodeEnvironmentRename(r)
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	response, err := s.environmentChanges.RenameEnvironment(
		r.Context(), r.PathValue("id"), input, r.Header.Get(idempotencyKeyHeader),
	)
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	w.Header().Set("Content-Type", response.ContentKind)
	w.WriteHeader(response.Status)
	if _, err := w.Write(response.Body); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Environment rename response", slog.Any("error", err))
	}
}

func decodeEnvironmentRename(r *http.Request) (hierarchy.RenameEnvironmentInput, error) {
	if len(r.URL.Query()) != 0 {
		return hierarchy.RenameEnvironmentInput{}, errs.New(
			errs.KindMalformedRequest,
			"Environment rename query is invalid",
		)
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return hierarchy.RenameEnvironmentInput{}, errs.Wrap(errs.KindMalformedRequest, err)
	}
	defer clear(body)
	if !utf8.Valid(body) {
		return hierarchy.RenameEnvironmentInput{}, errs.New(
			errs.KindMalformedRequest,
			"Environment rename body is not valid UTF-8",
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return hierarchy.RenameEnvironmentInput{}, projectCreateJSONError(err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return hierarchy.RenameEnvironmentInput{}, errs.New(
			errs.KindMalformedRequest,
			"Environment rename body must be an object",
		)
	}
	input := hierarchy.RenameEnvironmentInput{}
	seen := false
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return hierarchy.RenameEnvironmentInput{}, projectCreateJSONError(err)
		}
		member, ok := token.(string)
		if !ok {
			return hierarchy.RenameEnvironmentInput{}, errs.New(
				errs.KindMalformedRequest,
				"Environment rename member name is invalid",
			)
		}
		if member != "name" {
			return hierarchy.RenameEnvironmentInput{}, errs.New(
				errs.KindMalformedRequest,
				"Environment rename body contains an unknown member",
			)
		}
		if seen {
			return hierarchy.RenameEnvironmentInput{}, errs.New(
				errs.KindMalformedRequest,
				"Environment rename body contains a duplicate member",
			)
		}
		seen = true
		if err := decoder.Decode(&input.Name); err != nil {
			var typeError *json.UnmarshalTypeError
			if errors.As(err, &typeError) {
				return hierarchy.RenameEnvironmentInput{}, errs.New(
					errs.KindValidationFailed,
					"Environment name must be a string",
				)
			}
			return hierarchy.RenameEnvironmentInput{}, projectCreateJSONError(err)
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return hierarchy.RenameEnvironmentInput{}, projectCreateJSONError(err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return hierarchy.RenameEnvironmentInput{}, errs.New(
			errs.KindMalformedRequest,
			"Environment rename body is malformed",
		)
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return hierarchy.RenameEnvironmentInput{}, projectCreateJSONError(err)
	}
	if err := hierarchy.ValidateEnvironmentRenameInput(input); err != nil {
		return hierarchy.RenameEnvironmentInput{}, err
	}
	return input, nil
}
