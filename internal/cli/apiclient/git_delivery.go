package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (c *Client) ShowGitDeploymentBinding(
	ctx context.Context,
	service string,
) (apiTypes.GitDeploymentBindingState, string, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.GitDeploymentBindingState{}, "", err
	}
	path := "/api/v1/services/" + service + "/git-deployment"
	response, err := client.GitDeploymentBindingShowWithResponse(ctx, service)
	if err != nil {
		return apiTypes.GitDeploymentBindingState{}, "", generatedCallError(ctx, http.MethodGet, path, err)
	}
	value, err := decodeGitResponse[apiTypes.GitDeploymentBindingState](
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	)
	if err != nil {
		return apiTypes.GitDeploymentBindingState{}, "", err
	}
	etag := response.HTTPResponse.Header.Get("ETag")
	if etag == "" {
		return apiTypes.GitDeploymentBindingState{}, "", errs.New(
			errs.KindInternal,
			"Git delivery response is missing its revision ETag",
		)
	}
	return value, etag, nil
}

func (c *Client) BindGitDeployment(
	ctx context.Context,
	service string,
	input apiTypes.GitDeploymentBindingWrite,
	ifMatch, key string,
) (apiTypes.GitDeploymentBindingState, error) {
	key, err := gitIdempotencyKey(key)
	if err != nil {
		return apiTypes.GitDeploymentBindingState{}, err
	}
	if ifMatch == "" {
		_, etag, err := c.ShowGitDeploymentBinding(ctx, service)
		if err != nil {
			return apiTypes.GitDeploymentBindingState{}, err
		}
		ifMatch = etag
	}
	body, err := gitRequestBody(input)
	if err != nil {
		return apiTypes.GitDeploymentBindingState{}, err
	}
	defer clear(body)
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.GitDeploymentBindingState{}, err
	}
	path := "/api/v1/services/" + service + "/git-deployment"
	response, err := client.GitDeploymentBindWithBodyWithResponse(
		ctx,
		service,
		&generated.GitDeploymentBindParams{IfMatch: ifMatch, IdempotencyKey: key},
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		return apiTypes.GitDeploymentBindingState{}, generatedCallError(ctx, http.MethodPut, path, err)
	}
	return decodeGitResponse[apiTypes.GitDeploymentBindingState](
		http.MethodPut,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	)
}

func (c *Client) ListGitDeployments(
	ctx context.Context,
	service string,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.GitDeployment], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.GitDeployment]{}, err
	}
	path := "/api/v1/services/" + service + "/git-deployments"
	response, err := client.GitDeploymentListWithResponse(ctx, service, &generated.GitDeploymentListParams{
		Limit: gitPageLimit(limit), Cursor: optionalServiceString(cursor),
	})
	if err != nil {
		return apiTypes.Page[apiTypes.GitDeployment]{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	return decodeGitResponse[apiTypes.Page[apiTypes.GitDeployment]](
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	)
}

func (c *Client) StartGitDeployment(
	ctx context.Context,
	service string,
	input apiTypes.GitDeploymentStartRequest,
	key string,
) (apiTypes.GitDeployment, error) {
	key, err := gitIdempotencyKey(key)
	if err != nil {
		return apiTypes.GitDeployment{}, err
	}
	body, err := gitRequestBody(input)
	if err != nil {
		return apiTypes.GitDeployment{}, err
	}
	defer clear(body)
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.GitDeployment{}, err
	}
	path := "/api/v1/services/" + service + "/git-deployments"
	response, err := client.GitDeploymentStartWithBodyWithResponse(
		ctx,
		service,
		&generated.GitDeploymentStartParams{IdempotencyKey: key},
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		return apiTypes.GitDeployment{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	return decodeGitResponse[apiTypes.GitDeployment](
		http.MethodPost,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusAccepted,
	)
}

func (c *Client) ShowGitDeployment(ctx context.Context, service, deployment string) (apiTypes.GitDeployment, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.GitDeployment{}, err
	}
	path := "/api/v1/services/" + service + "/git-deployments/" + deployment
	response, err := client.GitDeploymentShowWithResponse(ctx, service, deployment)
	if err != nil {
		return apiTypes.GitDeployment{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	return decodeGitResponse[apiTypes.GitDeployment](
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	)
}

func (c *Client) ApproveGitDeployment(
	ctx context.Context,
	service, deployment, key string,
) (apiTypes.GitDeployment, error) {
	key, err := gitIdempotencyKey(key)
	if err != nil {
		return apiTypes.GitDeployment{}, err
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.GitDeployment{}, err
	}
	path := "/api/v1/services/" + service + "/git-deployments/" + deployment + "/approve"
	response, err := client.GitDeploymentApproveWithResponse(
		ctx,
		service,
		deployment,
		&generated.GitDeploymentApproveParams{IdempotencyKey: key},
	)
	if err != nil {
		return apiTypes.GitDeployment{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	return decodeGitResponse[apiTypes.GitDeployment](
		http.MethodPost,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusAccepted,
	)
}

func (c *Client) ListGitRunnerSelectors(
	ctx context.Context,
	service, connection string,
	repository int64,
) (apiTypes.Page[apiTypes.GitRunnerSelector], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.GitRunnerSelector]{}, err
	}
	path := "/api/v1/services/" + service + "/git-runner-selectors"
	response, err := client.GitDeploymentRunnerSelectorsWithResponse(
		ctx,
		service,
		&generated.GitDeploymentRunnerSelectorsParams{
			ConnectionId: connection, RepositoryId: repository,
		},
	)
	if err != nil {
		return apiTypes.Page[apiTypes.GitRunnerSelector]{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	return decodeGitResponse[apiTypes.Page[apiTypes.GitRunnerSelector]](
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	)
}

func (c *Client) ListGitHubConnections(
	ctx context.Context,
	scope apiTypes.GitHubConnectionScope,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.GitHubConnection], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.GitHubConnection]{}, err
	}
	const path = "/api/v1/github-connections"
	response, err := client.GithubConnectionListWithResponse(ctx, &generated.GithubConnectionListParams{
		ScopeKind: generated.GithubConnectionListParamsScopeKind(scope.Kind), ScopeId: scope.ID,
		Limit: gitPageLimit(limit), Cursor: optionalServiceString(cursor),
	})
	if err != nil {
		return apiTypes.Page[apiTypes.GitHubConnection]{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	return decodeGitResponse[apiTypes.Page[apiTypes.GitHubConnection]](
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	)
}

func (c *Client) CreateGitHubConnection(
	ctx context.Context,
	input apiTypes.GitHubConnectionCreateRequest,
	key string,
) (apiTypes.GitHubConnection, error) {
	key, err := gitIdempotencyKey(key)
	if err != nil {
		return apiTypes.GitHubConnection{}, err
	}
	body, err := gitRequestBody(input)
	if err != nil {
		return apiTypes.GitHubConnection{}, err
	}
	defer clear(body)
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.GitHubConnection{}, err
	}
	const path = "/api/v1/github-connections"
	response, err := client.GithubConnectionCreateWithBodyWithResponse(
		ctx,
		&generated.GithubConnectionCreateParams{IdempotencyKey: key},
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		return apiTypes.GitHubConnection{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	return decodeGitResponse[apiTypes.GitHubConnection](
		http.MethodPost,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusCreated,
	)
}

func (c *Client) ShowGitHubConnection(
	ctx context.Context,
	scope apiTypes.GitHubConnectionScope,
	id string,
) (apiTypes.GitHubConnection, string, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.GitHubConnection{}, "", err
	}
	path := "/api/v1/github-connections/" + id
	response, err := client.GithubConnectionShowWithResponse(ctx, id, &generated.GithubConnectionShowParams{
		ScopeKind: generated.GithubConnectionShowParamsScopeKind(scope.Kind), ScopeId: scope.ID,
	})
	if err != nil {
		return apiTypes.GitHubConnection{}, "", generatedCallError(ctx, http.MethodGet, path, err)
	}
	value, err := decodeGitResponse[apiTypes.GitHubConnection](
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	)
	if err != nil {
		return apiTypes.GitHubConnection{}, "", err
	}
	etag := response.HTTPResponse.Header.Get("ETag")
	if etag == "" {
		return apiTypes.GitHubConnection{}, "", errs.New(
			errs.KindInternal,
			"Git delivery response is missing its revision ETag",
		)
	}
	return value, etag, nil
}

func (c *Client) RevokeGitHubConnection(
	ctx context.Context,
	scope apiTypes.GitHubConnectionScope,
	id, ifMatch, key string,
) error {
	key, err := gitIdempotencyKey(key)
	if err != nil {
		return err
	}
	if ifMatch == "" {
		_, etag, err := c.ShowGitHubConnection(ctx, scope, id)
		if err != nil {
			return err
		}
		ifMatch = etag
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return err
	}
	path := "/api/v1/github-connections/" + id
	response, err := client.GithubConnectionRevokeWithResponse(ctx, id, &generated.GithubConnectionRevokeParams{
		ScopeKind: generated.GithubConnectionRevokeParamsScopeKind(scope.Kind), ScopeId: scope.ID,
		IfMatch: ifMatch, IdempotencyKey: key,
	})
	if err != nil {
		return generatedCallError(ctx, http.MethodDelete, path, err)
	}
	return generatedResponseError(http.MethodDelete, path, response.HTTPResponse, response.Body, http.StatusNoContent)
}

func (c *Client) RotateGitHubConnection(
	ctx context.Context,
	scope apiTypes.GitHubConnectionScope,
	id string,
	input apiTypes.GitHubConnectionCredentialsWrite,
	ifMatch, key string,
) (apiTypes.GitHubConnection, error) {
	key, err := gitIdempotencyKey(key)
	if err != nil {
		return apiTypes.GitHubConnection{}, err
	}
	if ifMatch == "" {
		_, etag, err := c.ShowGitHubConnection(ctx, scope, id)
		if err != nil {
			return apiTypes.GitHubConnection{}, err
		}
		ifMatch = etag
	}
	body, err := gitRequestBody(input)
	if err != nil {
		return apiTypes.GitHubConnection{}, err
	}
	defer clear(body)
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.GitHubConnection{}, err
	}
	path := "/api/v1/github-connections/" + id + "/credentials"
	response, err := client.GithubConnectionRotateWithBodyWithResponse(ctx, id, &generated.GithubConnectionRotateParams{
		ScopeKind: generated.GithubConnectionRotateParamsScopeKind(scope.Kind), ScopeId: scope.ID,
		IfMatch: ifMatch, IdempotencyKey: key,
	}, "application/json", bytes.NewReader(body))
	if err != nil {
		return apiTypes.GitHubConnection{}, generatedCallError(ctx, http.MethodPut, path, err)
	}
	return decodeGitResponse[apiTypes.GitHubConnection](
		http.MethodPut,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	)
}

func (c *Client) ListGitHubRepositories(
	ctx context.Context,
	scope apiTypes.GitHubConnectionScope,
	id string,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.GitHubRepository], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.GitHubRepository]{}, err
	}
	path := "/api/v1/github-connections/" + id + "/repositories"
	response, err := client.GithubConnectionRepositoriesWithResponse(
		ctx,
		id,
		&generated.GithubConnectionRepositoriesParams{
			ScopeKind: generated.GithubConnectionRepositoriesParamsScopeKind(scope.Kind), ScopeId: scope.ID,
			Limit: gitPageLimit(limit), Cursor: optionalServiceString(cursor),
		},
	)
	if err != nil {
		return apiTypes.Page[apiTypes.GitHubRepository]{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	return decodeGitResponse[apiTypes.Page[apiTypes.GitHubRepository]](
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	)
}

func gitIdempotencyKey(key string) (string, error) {
	if key == "" {
		key = ids.NewULID()
	}
	if !idempotencyKeyPattern.MatchString(key) {
		return "", errs.New(errs.KindValidationFailed, "Git delivery idempotency key is invalid")
	}
	return key, nil
}

func gitRequestBody(input any) ([]byte, error) {
	body, err := json.Marshal(input)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return body, nil
}

func gitPageLimit(limit int) *int64 {
	if limit == 0 {
		return nil
	}
	value := int64(limit)
	return &value
}

func decodeGitResponse[T any](method, path string, response *http.Response, body []byte, status int) (T, error) {
	var value T
	if err := generatedResponseError(method, path, response, body, status); err != nil {
		return value, err
	}
	if err := decodeSingleJSON(method, path, bytes.NewReader(body), &value); err != nil {
		return value, err
	}
	return value, nil
}
