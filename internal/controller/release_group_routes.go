package controller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdreleasegroup "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroup"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type ReleaseGroupReader interface {
	Get(context.Context, string) (etcdreleasegroup.Versioned, error)
	List(context.Context, string, etcdreleasegroup.PageRequest) (etcdreleasegroup.Page, error)
}

type ReleaseGroupMutator interface {
	AddReleaseGroup(context.Context, apiTypes.ReleaseGroupAddRequest, string) (etcd.IdempotencyResponse, error)
	EditReleaseGroup(context.Context, string, apiTypes.ReleaseGroupEditRequest, string) (etcd.IdempotencyResponse, error)
	RemoveReleaseGroup(context.Context, string, string) (etcd.IdempotencyResponse, error)
}

type ReleaseOperator interface {
	DeployService(context.Context, string, domain.ServiceDeployInput, string) (etcd.IdempotencyResponse, error)
	RollbackService(context.Context, string, domain.ServiceRollbackInput, string) (etcd.IdempotencyResponse, error)
	DeployReleaseGroup(context.Context, string, domain.GroupDeployInput, string) (etcd.IdempotencyResponse, error)
	RollbackReleaseGroup(context.Context, string, domain.GroupRollbackInput, string) (etcd.IdempotencyResponse, error)
	PreviewReleaseGroupRollback(context.Context, string, domain.GroupRollbackPreviewInput) (domain.GroupRollbackPreview, error)
}

type releaseGroupListInput struct {
	EnvironmentID string `query:"environment_id" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Limit         int    `query:"limit" required:"false" minimum:"1" maximum:"200"`
	Cursor        string `query:"cursor" required:"false"`
}

type releaseGroupShowInput struct {
	ID string `path:"id" pattern:"^rg_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type releaseGroupAddInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.ReleaseGroupAddRequest
}

type releaseGroupEditInput struct {
	ID             string `path:"id" pattern:"^rg_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.ReleaseGroupEditRequest
}

type releaseGroupRemoveInput struct {
	ID             string `path:"id" pattern:"^rg_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

type releaseGroupDeployInput struct {
	ID             string `path:"id" pattern:"^rg_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.ReleaseGroupDeployRequest
}

type releaseGroupRollbackInput struct {
	ID             string `path:"id" pattern:"^rg_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           *apiTypes.ReleaseGroupRollbackRequest
}

type releaseGroupRollbackPreviewInput struct {
	ID  string `path:"id" pattern:"^rg_[0-9A-HJKMNP-TV-Z]{26}$"`
	Tag string `query:"tag" required:"false"`
}

type releaseGroupOutput struct{ Body apiTypes.ReleaseGroup }
type releaseGroupRollbackPreviewOutput struct {
	Body apiTypes.ReleaseGroupRollbackPreview
}
type releaseGroupPageOutput struct {
	Body apiTypes.Page[apiTypes.ReleaseGroup]
}
type releaseGroupMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerReleaseGroups() {
	registry := s.API.OpenAPI().Components.Schemas
	mutationSchema := openAPISchema[apiTypes.ReleaseGroupMutationAccepted](registry, "ReleaseGroupMutationAccepted")
	groupSchema := openAPISchema[apiTypes.ReleaseGroup](registry, "ReleaseGroup")
	taskSchema := openAPISchema[apiTypes.ReleaseGroupTaskAccepted](registry, "ReleaseGroupTaskAccepted")
	huma.Register(s.API, huma.Operation{
		OperationID: "release-group.rollback-preview", Method: http.MethodGet, Path: "/release-groups/{id}/rollback-preview",
		Summary: "Preview a release group rollback", Tags: []string{"Release Group"}, Middlewares: huma.Middlewares{s.rejectInvalidRollbackPreviewTag},
	}, s.previewReleaseGroupRollback)
	huma.Register(s.API, huma.Operation{
		OperationID: "release-group.list", Method: http.MethodGet, Path: "/release-groups",
		Summary: "List release groups", Tags: []string{"Release Group"},
	}, s.listReleaseGroups)
	huma.Register(s.API, huma.Operation{
		OperationID: "release-group.show", Method: http.MethodGet, Path: "/release-groups/{id}",
		Summary: "Show a release group", Tags: []string{"Release Group"},
	}, s.showReleaseGroup)
	huma.Register(s.API, huma.Operation{
		OperationID: "release-group.add", Method: http.MethodPost, Path: "/release-groups",
		Summary: "Add a release group", Tags: []string{"Release Group"}, DefaultStatus: http.StatusCreated,
		Responses: map[string]*huma.Response{"201": {Description: http.StatusText(http.StatusCreated), Content: map[string]*huma.MediaType{
			"application/json": {Schema: groupSchema},
		}}},
	}, s.addReleaseGroup)
	huma.Register(s.API, huma.Operation{
		OperationID: "release-group.edit", Method: http.MethodPatch, Path: "/release-groups/{id}",
		Summary: "Edit a release group", Tags: []string{"Release Group"}, DefaultStatus: http.StatusOK,
		Responses: map[string]*huma.Response{"200": {Description: http.StatusText(http.StatusOK), Content: map[string]*huma.MediaType{
			"application/json": {Schema: groupSchema},
		}}},
	}, s.editReleaseGroup)
	huma.Register(s.API, huma.Operation{
		OperationID: "release-group.remove", Method: http.MethodDelete, Path: "/release-groups/{id}",
		Summary: "Remove a release group", Tags: []string{"Release Group"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectReleaseGroupRemoveBody, s.rejectReleaseGroupRemoveQuery},
		Responses: map[string]*huma.Response{"202": {
			Description: http.StatusText(http.StatusAccepted),
			Content:     map[string]*huma.MediaType{"application/json": {Schema: mutationSchema}},
		}},
	}, s.removeReleaseGroup)
	huma.Register(s.API, huma.Operation{
		OperationID: "release-group.deploy", Method: http.MethodPost, Path: "/release-groups/{id}/deploy",
		Summary: "Deploy a release group", Tags: []string{"Release Group"}, DefaultStatus: http.StatusAccepted,
		Responses: map[string]*huma.Response{"202": {Description: http.StatusText(http.StatusAccepted), Content: map[string]*huma.MediaType{
			"application/json": {Schema: taskSchema},
		}}},
	}, s.deployReleaseGroup)
	huma.Register(s.API, huma.Operation{
		OperationID: "release-group.rollback", Method: http.MethodPost, Path: "/release-groups/{id}/rollback",
		Summary: "Roll back a release group", Tags: []string{"Release Group"}, DefaultStatus: http.StatusAccepted,
		Responses: map[string]*huma.Response{"202": {Description: http.StatusText(http.StatusAccepted), Content: map[string]*huma.MediaType{
			"application/json": {Schema: taskSchema},
		}}},
	}, s.rollbackReleaseGroup)
	s.setRoutePolicy("POST /api/v1/release-groups", routePolicy{body: jsonBody})
	s.setRoutePolicy("PATCH /api/v1/release-groups/{id}", routePolicy{body: jsonBody})
	s.setRoutePolicy("POST /api/v1/release-groups/{id}/deploy", routePolicy{body: jsonBody})
	s.setRoutePolicy("POST /api/v1/release-groups/{id}/rollback", routePolicy{body: jsonBody})
}

func (s *Server) deployReleaseGroup(ctx context.Context, input *releaseGroupDeployInput) (*releaseGroupMutationOutput, error) {
	if s.releaseOperations == nil {
		return nil, errs.New(errs.KindInternal, "release operator is not configured")
	}
	response, err := s.releaseOperations.DeployReleaseGroup(ctx, input.ID, domain.GroupDeployInput{Tag: input.Body.Tag}, input.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.releaseGroupMutationResponse(response), nil
}

func (s *Server) rollbackReleaseGroup(ctx context.Context, input *releaseGroupRollbackInput) (*releaseGroupMutationOutput, error) {
	if s.releaseOperations == nil {
		return nil, errs.New(errs.KindInternal, "release operator is not configured")
	}
	request := apiTypes.ReleaseGroupRollbackRequest{}
	if input.Body != nil {
		request = *input.Body
	}
	var revision *int64
	if request.PreviewRevision != nil {
		parsed, parseErr := strconv.ParseInt(*request.PreviewRevision, 10, 64)
		if parseErr != nil {
			return nil, errs.New(errs.KindValidationFailed, "rollback preview revision is invalid")
		}
		revision = &parsed
	}
	operation, err := domain.NewGroupRollbackInput(request.Tag, revision)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response, err := s.releaseOperations.RollbackReleaseGroup(ctx, input.ID, operation, input.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.releaseGroupMutationResponse(response), nil
}

func (s *Server) previewReleaseGroupRollback(ctx context.Context, input *releaseGroupRollbackPreviewInput) (*releaseGroupRollbackPreviewOutput, error) {
	if s.releaseOperations == nil {
		return nil, errs.New(errs.KindInternal, "release operator is not configured")
	}
	var tag *string
	if input.Tag != "" {
		tag = &input.Tag
	}
	request, err := domain.NewGroupRollbackPreviewInput(tag)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	preview, err := s.releaseOperations.PreviewReleaseGroupRollback(ctx, input.ID, request)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	sources := make([]apiTypes.ReleaseGroupRollbackSource, len(preview.Sources))
	for index, source := range preview.Sources {
		sources[index] = apiTypes.ReleaseGroupRollbackSource{ServiceID: source.ServiceID, ReleaseID: source.ReleaseID, Tag: source.Tag}
	}
	return &releaseGroupRollbackPreviewOutput{Body: apiTypes.ReleaseGroupRollbackPreview{ReleaseGroupID: preview.GroupID, Revision: strconv.FormatInt(preview.Revision, 10), Sources: sources}}, nil
}

func (s *Server) rejectInvalidRollbackPreviewTag(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	values, present := requestURL.Query()["tag"]
	if present && (len(values) != 1 || values[0] == "" || values[0] != strings.TrimSpace(values[0])) {
		s.writeReleaseGroupProblem(ctx, "rollback tag is invalid")
		return
	}
	next(ctx)
}

func (s *Server) listReleaseGroups(ctx context.Context, input *releaseGroupListInput) (*releaseGroupPageOutput, error) {
	if s.releaseGroups == nil {
		return nil, errs.New(errs.KindInternal, "release group reader is not configured")
	}
	limit := input.Limit
	if limit == 0 {
		limit = 50
	}
	page, err := s.releaseGroups.List(ctx, input.EnvironmentID, etcdreleasegroup.PageRequest{Limit: limit, Cursor: input.Cursor})
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	result := apiTypes.Page[apiTypes.ReleaseGroup]{
		Items: make([]apiTypes.ReleaseGroup, len(page.Items)), NextCursor: page.NextCursor, Revision: page.Revision,
	}
	for index, item := range page.Items {
		result.Items[index] = releaseGroupResponse(item)
	}
	return &releaseGroupPageOutput{Body: result}, nil
}

func (s *Server) showReleaseGroup(ctx context.Context, input *releaseGroupShowInput) (*releaseGroupOutput, error) {
	if s.releaseGroups == nil {
		return nil, errs.New(errs.KindInternal, "release group reader is not configured")
	}
	group, err := s.releaseGroups.Get(ctx, input.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &releaseGroupOutput{Body: releaseGroupResponse(group)}, nil
}

func (s *Server) addReleaseGroup(ctx context.Context, input *releaseGroupAddInput) (*releaseGroupMutationOutput, error) {
	if s.releaseGroupMutations == nil {
		return nil, errs.New(errs.KindInternal, "release group mutator is not configured")
	}
	response, err := s.releaseGroupMutations.AddReleaseGroup(ctx, input.Body, input.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.releaseGroupMutationResponse(response), nil
}

func (s *Server) editReleaseGroup(ctx context.Context, input *releaseGroupEditInput) (*releaseGroupMutationOutput, error) {
	if s.releaseGroupMutations == nil {
		return nil, errs.New(errs.KindInternal, "release group mutator is not configured")
	}
	response, err := s.releaseGroupMutations.EditReleaseGroup(ctx, input.ID, input.Body, input.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.releaseGroupMutationResponse(response), nil
}

func (s *Server) removeReleaseGroup(ctx context.Context, input *releaseGroupRemoveInput) (*releaseGroupMutationOutput, error) {
	if s.releaseGroupMutations == nil {
		return nil, errs.New(errs.KindInternal, "release group mutator is not configured")
	}
	response, err := s.releaseGroupMutations.RemoveReleaseGroup(ctx, input.ID, input.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.releaseGroupMutationResponse(response), nil
}

func (s *Server) rejectReleaseGroupRemoveBody(ctx huma.Context, next func(huma.Context)) {
	var probe [1]byte
	count, err := ctx.BodyReader().Read(probe[:])
	if count != 0 || (err != nil && !errors.Is(err, io.EOF)) {
		s.writeReleaseGroupProblem(ctx, "release group removal body is not allowed")
		return
	}
	next(ctx)
}

func (s *Server) rejectReleaseGroupRemoveQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	if len(requestURL.Query()) != 0 {
		s.writeReleaseGroupProblem(ctx, "release group removal query is invalid")
		return
	}
	next(ctx)
}

func (s *Server) writeReleaseGroupProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write release group request problem", slog.Any("error", err))
	}
}

func (s *Server) releaseGroupMutationResponse(response etcd.IdempotencyResponse) *releaseGroupMutationOutput {
	return &releaseGroupMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
				s.Logger.Error("controller: write release group mutation response", slog.Any("error", err))
			}
		},
	}
}

func releaseGroupResponse(versioned etcdreleasegroup.Versioned) apiTypes.ReleaseGroup {
	group := versioned.Group
	return apiTypes.ReleaseGroup{
		ID: group.ID, EnvironmentID: group.EnvironmentID, Name: group.Name,
		ServiceIDs: append([]string(nil), group.ServiceIDs...), Order: append([]string(nil), group.Order...),
		Tag: group.DefaultTag, OnFailure: apiTypes.OnFailure(group.OnFailure),
	}
}
