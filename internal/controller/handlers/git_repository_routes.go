package handlers

import (
	"context"
	"net/http"

	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/danielgtaylor/huma/v2"
)

type gitRepositoryOperator interface {
	GitRepositoryRunnerDefault(
		context.Context,
		apiTypes.GitHubConnectionScope,
		string,
		int64,
	) (apiTypes.GitRepositoryRunnerDefaultState, error)
	PutGitRepositoryRunnerDefault(
		context.Context,
		apiTypes.GitHubConnectionScope,
		string,
		int64,
		int64,
		apiTypes.GitRepositoryRunnerDefaultWrite,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
	DeleteGitRepositoryRunnerDefault(
		context.Context,
		apiTypes.GitHubConnectionScope,
		string,
		int64,
		int64,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
	ListGitHubWorkflows(
		context.Context,
		apiTypes.GitHubConnectionScope,
		string,
		int64,
		keyvalue.PageRequest,
	) (apiTypes.Page[apiTypes.GitHubWorkflow], error)
}

type GitRepositoryPath struct {
	GithubConnectionPath
	RepositoryID int64 `path:"repository_id" minimum:"1"`
}

type GitRepositoryMutationInput struct {
	GitRepositoryPath
	GitRevisionHeader
	GitWriteHeaders
}

type gitRepositoryRunnerDefaultInput struct {
	GitRepositoryMutationInput
	Body apiTypes.GitRepositoryRunnerDefaultWrite
}

type gitRepositoryRunnerDefaultOutput struct {
	ETag string `header:"ETag"`
	Body apiTypes.GitRepositoryRunnerDefaultState
}

type gitWorkflowListInput struct {
	GitRepositoryPath
	Limit  int    `query:"limit" minimum:"1" maximum:"100"`
	Cursor string `query:"cursor" maxLength:"4096"`
}

func (s *Server) registerGitRepositories(routes gitDeliveryRoutes) {
	const repository = "/github-connections/{id}/repositories/{repository_id}"
	const defaults = repository + "/runner-default"
	huma.Register(s.API, routes.operation("github-repository.runner-default-show", http.MethodGet, defaults,
		"Show the preferred Runner selector saved at this repository scope"), routes.runnerDefault)
	huma.Register(s.API, gitMutation[apiTypes.GitRepositoryRunnerDefaultState](s,
		routes.operation(
			"github-repository.runner-default-put",
			http.MethodPut,
			defaults,
			"Save the preferred Runner selector for this repository scope",
		), http.StatusOK, "GitRepositoryRunnerDefaultState"), routes.putRunnerDefault)
	remove := routes.operation("github-repository.runner-default-remove", http.MethodDelete, defaults,
		"Remove the preferred Runner selector at this repository scope")
	remove.DefaultStatus = http.StatusNoContent
	remove.Middlewares = append(remove.Middlewares, s.rejectTaskMutationBody)
	remove.Responses = map[string]*huma.Response{"204": {Description: "Repository Runner default removed"}}
	huma.Register(s.API, remove, routes.deleteRunnerDefault)
	huma.Register(s.API, routes.operation("github-repository.workflows", http.MethodGet, repository+"/workflows",
		"List GitHub Actions workflows visible in this repository"), routes.workflows)
	s.setRoutePolicy("PUT /api/v1"+defaults, routePolicy{body: jsonBody})
}

func (routes gitDeliveryRoutes) runnerDefault(
	ctx context.Context,
	input *GitRepositoryPath,
) (*gitRepositoryRunnerDefaultOutput, error) {
	scope, err := input.scope()
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	result, err := routes.operator.GitRepositoryRunnerDefault(ctx, scope, input.ConnectionID, input.RepositoryID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &gitRepositoryRunnerDefaultOutput{ETag: gitETag(result.Revision), Body: result}, nil
}

func (routes gitDeliveryRoutes) putRunnerDefault(
	ctx context.Context,
	input *gitRepositoryRunnerDefaultInput,
) (*serviceMutationOutput, error) {
	scope, err := input.scope()
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	revision, err := gitWriteRevision(input.IfMatch, true)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return routes.mutation(
		routes.operator.PutGitRepositoryRunnerDefault(
			ctx,
			scope,
			input.ConnectionID,
			input.RepositoryID,
			revision,
			input.Body,
			input.Key,
		),
	)
}

func (routes gitDeliveryRoutes) deleteRunnerDefault(
	ctx context.Context,
	input *GitRepositoryMutationInput,
) (*serviceMutationOutput, error) {
	scope, err := input.scope()
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	revision, err := gitWriteRevision(input.IfMatch, false)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return routes.mutation(
		routes.operator.DeleteGitRepositoryRunnerDefault(
			ctx,
			scope,
			input.ConnectionID,
			input.RepositoryID,
			revision,
			input.Key,
		),
	)
}

func (routes gitDeliveryRoutes) workflows(
	ctx context.Context,
	input *gitWorkflowListInput,
) (*gitListOutput[apiTypes.GitHubWorkflow], error) {
	scope, err := input.scope()
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return gitPageOutput(routes.operator.ListGitHubWorkflows(ctx, scope, input.ConnectionID, input.RepositoryID,
		keyvalue.PageRequest{Limit: input.Limit, Cursor: input.Cursor}))
}
