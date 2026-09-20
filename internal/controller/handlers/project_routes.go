package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strconv"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/controller/hierarchy"
	"github.com/AlanD20/groundplane/internal/core"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type ProjectReader interface {
	GetProject(context.Context, string) (hierarchy.Versioned[core.Project], error)
	ListAllProjects(
		context.Context,
		hierarchy.ProjectFilter,
		hierarchy.PageRequest,
	) (hierarchy.Page[core.Project], error)
}

type ProjectMutator interface {
	CreateProject(context.Context, hierarchy.CreateProjectInput, string) (idempotencyrecord.IdempotencyResponse, error)
}

type projectListInput struct {
	Kind   string `query:"kind" required:"false"`
	Tenant string `query:"tenant" required:"false"`
	Limit  int    `query:"limit" required:"false"`
	Cursor string `query:"cursor" required:"false"`
}

type projectShowInput struct {
	ID string `path:"id"`
}

type projectCreateInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

type projectEditInput struct {
	ID             string `path:"id"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

type projectRenameInput struct {
	ID             string `path:"id"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

type projectOutput struct {
	Body apiTypes.Project
}

type projectPageOutput struct {
	Body apiTypes.ProjectPage
}

type projectMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerProjects() {
	projectSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.Project](),
		true,
		"Project",
	)
	registerProjectMutation[apiTypes.ProjectCreate](
		s,
		huma.Operation{
			OperationID: "project.create", Method: http.MethodPost, Path: "/projects",
			Summary: "Create a tenant project", Tags: []string{"Project"}, DefaultStatus: http.StatusCreated,
			Middlewares: huma.Middlewares{s.rejectProjectQuery},
		},
		projectSchema,
		s.createProject,
		"ProjectCreate",
	)
	huma.Register(s.API, huma.Operation{
		OperationID: "project.list", Method: http.MethodGet, Path: "/projects",
		Summary: "List projects", Tags: []string{"Project"},
		Middlewares: huma.Middlewares{s.validateProjectListQuery},
	}, s.listProjects)
	huma.Register(s.API, huma.Operation{
		OperationID: "project.show", Method: http.MethodGet, Path: "/projects/{id}",
		Summary: "Show a project", Tags: []string{"Project"},
		Middlewares: huma.Middlewares{s.rejectProjectQuery},
	}, s.showProject)
	registerProjectMutation[apiTypes.ProjectEdit](
		s,
		huma.Operation{
			OperationID: "project.edit", Method: http.MethodPatch, Path: "/projects/{id}",
			Summary: "Edit a project", Tags: []string{"Project"}, DefaultStatus: http.StatusOK,
			Middlewares: huma.Middlewares{s.rejectProjectQuery},
		},
		projectSchema,
		s.editProject,
		"ProjectEdit",
	)
	registerProjectMutation[apiTypes.ProjectRename](
		s,
		huma.Operation{
			OperationID: "project.rename", Method: http.MethodPost, Path: "/projects/{id}/rename",
			Summary: "Rename a project slug", Tags: []string{"Project"}, DefaultStatus: http.StatusOK,
			Middlewares: huma.Middlewares{s.rejectProjectQuery},
		},
		projectSchema,
		s.renameProject,
		"ProjectRename",
	)

	for _, pattern := range []string{
		"POST /api/v1/projects",
		"PATCH /api/v1/projects/{id}",
		"POST /api/v1/projects/{id}/rename",
	} {
		s.setRoutePolicy(pattern, routePolicy{body: jsonBody})
	}
}

func registerProjectMutation[InputBody any, Input any](
	s *Server,
	operation huma.Operation,
	projectSchema *huma.Schema,
	handler func(context.Context, *Input) (*projectMutationOutput, error),
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
				"application/json": {Schema: projectSchema},
			},
		},
	}
	huma.Register(s.API, operation, handler)
}

func (s *Server) validateProjectListQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	query := requestURL.Query()
	for key, values := range query {
		if key != "kind" && key != "tenant" && key != "limit" && key != "cursor" {
			s.writeProjectHumaProblem(ctx, "Project list query is invalid")
			return
		}
		if len(values) != 1 {
			s.writeProjectHumaProblem(ctx, "Project list query is duplicated")
			return
		}
	}
	if raw := query.Get("limit"); raw != "" {
		if _, err := strconv.Atoi(raw); err != nil {
			s.writeProjectHumaProblem(ctx, "Project pagination limit is invalid")
			return
		}
	}
	next(ctx)
}

func (s *Server) rejectProjectQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	if len(requestURL.Query()) != 0 {
		s.writeProjectHumaProblem(ctx, "Project request query is invalid")
		return
	}
	next(ctx)
}

func (s *Server) writeProjectHumaProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Project request problem", slog.Any("error", err))
	}
}

func (s *Server) createProject(ctx context.Context, request *projectCreateInput) (*projectMutationOutput, error) {
	if s.projectMutations == nil {
		return nil, errs.New(errs.KindInternal, "Project mutator is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeProjectCreate(request.RawBody)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response, err := s.projectMutations.CreateProject(
		ctx, input, request.IdempotencyKey,
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.projectMutationResponse(response, "create"), nil
}

func decodeProjectCreate(body []byte) (hierarchy.CreateProjectInput, error) {
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
		return hierarchy.CreateProjectInput{}, errs.New(
			errs.KindMalformedRequest,
			"Project creation body must be an object",
		)
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
			return hierarchy.CreateProjectInput{}, errs.New(
				errs.KindMalformedRequest,
				"Project creation member name is invalid",
			)
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

func (s *Server) listProjects(
	ctx context.Context,
	request *projectListInput,
) (*projectPageOutput, error) {
	if s.projects == nil {
		return nil, errs.New(errs.KindInternal, "Project reader is not configured")
	}
	page, err := s.projects.ListAllProjects(
		ctx,
		hierarchy.ProjectFilter{TenantID: request.Tenant, Kind: core.ProjectKind(request.Kind)},
		hierarchy.PageRequest{Limit: request.Limit, Cursor: request.Cursor},
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response := apiTypes.ProjectPage{
		Items: make([]apiTypes.Project, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = projectAPI(item.Record)
	}
	return &projectPageOutput{Body: response}, nil
}

func (s *Server) showProject(ctx context.Context, request *projectShowInput) (*projectOutput, error) {
	if s.projects == nil {
		return nil, errs.New(errs.KindInternal, "Project reader is not configured")
	}
	stored, err := s.projects.GetProject(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &projectOutput{Body: projectAPI(stored.Record)}, nil
}

func projectAPI(record core.Project) apiTypes.Project {
	return apiTypes.Project{
		ID: record.ID, TenantID: record.TenantID, Slug: record.Slug,
		Name: record.Name, Description: record.Description, Kind: string(record.Kind),
		DeletionTaskID: record.DeletionTaskID,
	}
}

func normalizeProjectError(err error) error {
	var domainError *errs.Error
	if errors.As(err, &domainError) {
		return domainError
	}
	return errs.Wrap(errs.KindInternal, err)
}

func (s *Server) writeProjectProblem(w http.ResponseWriter, err error) {
	normalized := normalizeProjectError(err)
	var domainError *errs.Error
	if !errors.As(normalized, &domainError) {
		domainError = errs.New(errs.KindInternal, "Hierarchy request failed")
	}
	s.writeProblem(w, domainError)
}
