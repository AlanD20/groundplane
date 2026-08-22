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

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EnvironmentReader interface {
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	ListEnvironments(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.EnvironmentRecord], error)
}

type EnvironmentMutator interface {
	CreateEnvironment(
		context.Context,
		hierarchy.CreateEnvironmentInput,
		string,
	) (etcd.IdempotencyResponse, error)
}

func (s *Server) environmentCreate(w http.ResponseWriter, r *http.Request) {
	if s.environmentMutations == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Environment mutator is not configured"))
		return
	}
	input, err := decodeEnvironmentCreate(r)
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	response, err := s.environmentMutations.CreateEnvironment(
		r.Context(), input, r.Header.Get(idempotencyKeyHeader),
	)
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	w.Header().Set("Content-Type", response.ContentKind)
	w.WriteHeader(response.Status)
	if _, err := w.Write(response.Body); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Environment creation response", slog.Any("error", err))
	}
}

func decodeEnvironmentCreate(r *http.Request) (hierarchy.CreateEnvironmentInput, error) {
	if len(r.URL.Query()) != 0 {
		return hierarchy.CreateEnvironmentInput{}, errs.New(
			errs.KindMalformedRequest,
			"Environment creation query is invalid",
		)
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return hierarchy.CreateEnvironmentInput{}, errs.Wrap(errs.KindMalformedRequest, err)
	}
	defer clear(body)
	if !utf8.Valid(body) {
		return hierarchy.CreateEnvironmentInput{}, errs.New(
			errs.KindMalformedRequest,
			"Environment creation body is not valid UTF-8",
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return hierarchy.CreateEnvironmentInput{}, projectCreateJSONError(err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return hierarchy.CreateEnvironmentInput{}, errs.New(
			errs.KindMalformedRequest,
			"Environment creation body must be an object",
		)
	}
	input := hierarchy.CreateEnvironmentInput{}
	seen := make(map[string]struct{}, 2)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return hierarchy.CreateEnvironmentInput{}, projectCreateJSONError(err)
		}
		member, ok := token.(string)
		if !ok {
			return hierarchy.CreateEnvironmentInput{}, errs.New(
				errs.KindMalformedRequest,
				"Environment creation member name is invalid",
			)
		}
		if _, duplicate := seen[member]; duplicate {
			return hierarchy.CreateEnvironmentInput{}, errs.New(
				errs.KindMalformedRequest,
				"Environment creation body contains a duplicate member",
			)
		}
		seen[member] = struct{}{}
		if member != "project_id" && member != "name" {
			return hierarchy.CreateEnvironmentInput{}, errs.New(
				errs.KindMalformedRequest,
				"Environment creation body contains an unknown member",
			)
		}
		var value string
		if err := decoder.Decode(&value); err != nil {
			var typeError *json.UnmarshalTypeError
			if errors.As(err, &typeError) {
				return hierarchy.CreateEnvironmentInput{}, errs.New(
					errs.KindValidationFailed,
					"Environment creation members must be strings",
				)
			}
			return hierarchy.CreateEnvironmentInput{}, projectCreateJSONError(err)
		}
		if member == "project_id" {
			input.ProjectID = value
		} else {
			input.Name = value
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return hierarchy.CreateEnvironmentInput{}, projectCreateJSONError(err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return hierarchy.CreateEnvironmentInput{}, errs.New(
			errs.KindMalformedRequest,
			"Environment creation body is malformed",
		)
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return hierarchy.CreateEnvironmentInput{}, projectCreateJSONError(err)
	}
	if err := hierarchy.ValidateEnvironmentCreateInput(input); err != nil {
		return hierarchy.CreateEnvironmentInput{}, err
	}
	return input, nil
}

func (s *Server) environmentList(w http.ResponseWriter, r *http.Request) {
	if s.environments == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Environment reader is not configured"))
		return
	}
	projectID, request, err := environmentListRequest(r)
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	page, err := s.environments.ListEnvironments(r.Context(), projectID, request)
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	response := apiTypes.Page[apiTypes.Environment]{
		Items: make([]apiTypes.Environment, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = environmentResponse(item.Record)
	}
	s.writeEnvironmentJSON(w, response)
}

func (s *Server) environmentShow(w http.ResponseWriter, r *http.Request) {
	if s.environments == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Environment reader is not configured"))
		return
	}
	if len(r.URL.Query()) != 0 {
		s.writeProjectProblem(w, errs.New(errs.KindMalformedRequest, "Environment detail query is invalid"))
		return
	}
	stored, err := s.environments.GetEnvironment(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	s.writeEnvironmentJSON(w, environmentResponse(stored.Record))
}

func environmentListRequest(r *http.Request) (string, etcd.PageRequest, error) {
	query := r.URL.Query()
	for key, values := range query {
		if key != "project" && key != "limit" && key != "cursor" {
			return "", etcd.PageRequest{}, errs.New(errs.KindMalformedRequest, "Environment list query is invalid")
		}
		if len(values) != 1 {
			return "", etcd.PageRequest{}, errs.New(
				errs.KindMalformedRequest,
				"Environment list query contains duplicate values",
			)
		}
	}
	projectID := query.Get("project")
	if err := ids.Validate(ids.KindProject, projectID); err != nil {
		return "", etcd.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Environment list requires a stable project id",
		)
	}
	request := etcd.PageRequest{Cursor: query.Get("cursor")}
	if raw := query.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			return "", etcd.PageRequest{}, errs.New(
				errs.KindValidationFailed,
				"Environment list limit must be a positive integer",
			)
		}
		request.Limit = limit
	}
	return projectID, request, nil
}

func environmentResponse(record etcd.EnvironmentRecord) apiTypes.Environment {
	var createTaskID *string
	if record.ProvisioningState != etcd.EnvironmentProvisioningReady {
		value := record.CreateTaskID
		createTaskID = &value
	}
	return apiTypes.Environment{
		ID: record.ID, ProjectID: record.ProjectID, Name: record.Name, VolumeDir: record.VolumeDir,
		ProvisioningState: apiTypes.EnvironmentProvisioningState(record.ProvisioningState),
		CreateTaskID:      createTaskID,
	}
}

func (s *Server) writeEnvironmentJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Environment response", slog.Any("error", err))
	}
}
