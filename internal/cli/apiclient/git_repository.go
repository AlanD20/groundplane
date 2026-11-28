package apiclient

import (
	"context"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (c *Client) ShowGitRepositoryRunnerDefault(
	ctx context.Context, scope apiTypes.GitHubConnectionScope, connection string, repository int64,
) (apiTypes.GitRepositoryRunnerDefaultState, string, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.GitRepositoryRunnerDefaultState{}, "", err
	}
	path := gitRepositoryPath(connection, repository) + "/runner-default"
	response, err := client.GithubRepositoryRunnerDefaultShowWithResponse(
		ctx,
		connection,
		repository,
		&generated.GithubRepositoryRunnerDefaultShowParams{
			ScopeKind: generated.GithubRepositoryRunnerDefaultShowParamsScopeKind(scope.Kind),
			ScopeId:   scope.ID,
		},
	)
	if err != nil {
		return apiTypes.GitRepositoryRunnerDefaultState{}, "", generatedCallError(ctx, http.MethodGet, path, err)
	}
	state, err := decodeGitResponse[apiTypes.GitRepositoryRunnerDefaultState](
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	)
	if err != nil {
		return apiTypes.GitRepositoryRunnerDefaultState{}, "", err
	}
	etag := response.HTTPResponse.Header.Get("ETag")
	if etag == "" {
		return apiTypes.GitRepositoryRunnerDefaultState{}, "", errs.New(
			errs.KindInternal,
			"repository Runner default response is missing its revision ETag",
		)
	}
	return state, etag, nil
}

func (c *Client) PutGitRepositoryRunnerDefault(
	ctx context.Context, scope apiTypes.GitHubConnectionScope, connection string, repository int64,
	input apiTypes.GitRepositoryRunnerDefaultWrite, ifMatch, key string,
) (apiTypes.GitRepositoryRunnerDefaultState, error) {
	key, err := gitIdempotencyKey(key)
	if err != nil {
		return apiTypes.GitRepositoryRunnerDefaultState{}, err
	}
	if ifMatch == "" {
		_, ifMatch, err = c.ShowGitRepositoryRunnerDefault(ctx, scope, connection, repository)
		if err != nil {
			return apiTypes.GitRepositoryRunnerDefaultState{}, err
		}
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.GitRepositoryRunnerDefaultState{}, err
	}
	path := gitRepositoryPath(connection, repository) + "/runner-default"
	body := generated.GithubRepositoryRunnerDefaultPutJSONRequestBody{
		Label: input.Label, Group: optionalServiceString(input.Group),
	}
	if input.GroupID != 0 {
		body.GroupId = &input.GroupID
	}
	response, err := client.GithubRepositoryRunnerDefaultPutWithResponse(
		ctx,
		connection,
		repository,
		&generated.GithubRepositoryRunnerDefaultPutParams{
			ScopeKind: generated.GithubRepositoryRunnerDefaultPutParamsScopeKind(scope.Kind),
			ScopeId:   scope.ID,
			IfMatch:   ifMatch, IdempotencyKey: key,
		},
		body,
	)
	if err != nil {
		return apiTypes.GitRepositoryRunnerDefaultState{}, generatedCallError(ctx, http.MethodPut, path, err)
	}
	return decodeGitResponse[apiTypes.GitRepositoryRunnerDefaultState](
		http.MethodPut,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	)
}

func (c *Client) RemoveGitRepositoryRunnerDefault(
	ctx context.Context, scope apiTypes.GitHubConnectionScope, connection string, repository int64, ifMatch, key string,
) error {
	key, err := gitIdempotencyKey(key)
	if err != nil {
		return err
	}
	if ifMatch == "" {
		var current apiTypes.GitRepositoryRunnerDefaultState
		current, ifMatch, err = c.ShowGitRepositoryRunnerDefault(ctx, scope, connection, repository)
		if err != nil {
			return err
		}
		if current.Default == nil {
			return errs.New(errs.KindRequestNotFound, "no repository Runner default is configured in this scope")
		}
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return err
	}
	path := gitRepositoryPath(connection, repository) + "/runner-default"
	response, err := client.GithubRepositoryRunnerDefaultRemoveWithResponse(
		ctx,
		connection,
		repository,
		&generated.GithubRepositoryRunnerDefaultRemoveParams{
			ScopeKind: generated.GithubRepositoryRunnerDefaultRemoveParamsScopeKind(scope.Kind),
			ScopeId:   scope.ID,
			IfMatch:   ifMatch, IdempotencyKey: key,
		},
	)
	if err != nil {
		return generatedCallError(ctx, http.MethodDelete, path, err)
	}
	return generatedResponseError(http.MethodDelete, path, response.HTTPResponse, response.Body, http.StatusNoContent)
}

func (c *Client) ListGitHubWorkflows(
	ctx context.Context,
	scope apiTypes.GitHubConnectionScope,
	connection string,
	repository int64,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.GitHubWorkflow], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.GitHubWorkflow]{}, err
	}
	path := gitRepositoryPath(connection, repository) + "/workflows"
	response, err := client.GithubRepositoryWorkflowsWithResponse(
		ctx,
		connection,
		repository,
		&generated.GithubRepositoryWorkflowsParams{
			ScopeKind: generated.GithubRepositoryWorkflowsParamsScopeKind(scope.Kind),
			ScopeId:   scope.ID,
			Limit:     gitPageLimit(limit), Cursor: optionalServiceString(cursor),
		},
	)
	if err != nil {
		return apiTypes.Page[apiTypes.GitHubWorkflow]{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	return decodeGitResponse[apiTypes.Page[apiTypes.GitHubWorkflow]](
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	)
}

func gitRepositoryPath(connection string, repository int64) string {
	return "/api/v1/github-connections/" + connection + "/repositories/" + strconv.FormatInt(repository, 10)
}
