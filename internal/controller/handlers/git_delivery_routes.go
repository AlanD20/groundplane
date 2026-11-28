package handlers

import (
	"context"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

// GitDeliveryOperator resolves authoritative ownership and handles durable
// replay. These routes only accept Service IDs, never a client-supplied
// deployment destination or hierarchy.
type GitDeliveryOperator interface {
	gitRepositoryOperator
	GitDeploymentBinding(context.Context, string) (apiTypes.GitDeploymentBindingState, error)
	PutGitDeploymentBinding(
		context.Context,
		string,
		int64,
		apiTypes.GitDeploymentBindingWrite,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
	ListGitDeployments(context.Context, string, keyvalue.PageRequest) (apiTypes.Page[apiTypes.GitDeployment], error)
	StartGitDeployment(
		context.Context,
		string,
		apiTypes.GitDeploymentStartRequest,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
	GitDeployment(context.Context, string, string) (apiTypes.GitDeployment, error)
	ApproveGitDeployment(context.Context, string, string, string) (idempotencyrecord.IdempotencyResponse, error)
	ListGitRunnerSelectors(context.Context, string, string, int64) (apiTypes.Page[apiTypes.GitRunnerSelector], error)

	ListGitHubConnections(
		context.Context,
		apiTypes.GitHubConnectionScope,
		keyvalue.PageRequest,
	) (apiTypes.Page[apiTypes.GitHubConnection], error)
	CreateGitHubConnection(
		context.Context,
		apiTypes.GitHubConnectionCreateRequest,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
	GitHubConnection(context.Context, apiTypes.GitHubConnectionScope, string) (apiTypes.GitHubConnection, int64, error)
	RevokeGitHubConnection(
		context.Context,
		apiTypes.GitHubConnectionScope,
		string,
		int64,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
	RotateGitHubConnection(
		context.Context,
		apiTypes.GitHubConnectionScope,
		string,
		int64,
		apiTypes.GitHubConnectionCredentialsWrite,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
	ListGitHubRepositories(
		context.Context,
		apiTypes.GitHubConnectionScope,
		string,
		keyvalue.PageRequest,
	) (apiTypes.Page[apiTypes.GitHubRepository], error)
}

type GitServicePath struct {
	ID string `path:"id" pattern:"^svc_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type GitDeploymentPath struct {
	GitServicePath
	Deployment string `path:"deployment" pattern:"^op_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type GitWriteHeaders struct {
	Key string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

type GitRevisionHeader struct {
	IfMatch string `header:"If-Match" required:"true"`
}

type GitPageQuery struct {
	Limit  int    `query:"limit" minimum:"1" maximum:"200"`
	Cursor string `query:"cursor" maxLength:"4096"`
}

func (page GitPageQuery) request() keyvalue.PageRequest {
	return keyvalue.PageRequest{Limit: page.Limit, Cursor: page.Cursor}
}

type gitBindingInput struct {
	GitServicePath
	GitRevisionHeader
	GitWriteHeaders
	Body apiTypes.GitDeploymentBindingWrite
}

type gitBindingOutput struct {
	ETag string `header:"ETag"`
	Body apiTypes.GitDeploymentBindingState
}

type gitDeploymentListInput struct {
	GitServicePath
	Limit  int    `query:"limit" minimum:"1" maximum:"64"`
	Cursor string `query:"cursor" maxLength:"4096"`
}

type gitDeploymentStartInput struct {
	GitServicePath
	GitWriteHeaders
	Body *apiTypes.GitDeploymentStartRequest `required:"false"`
}

type gitDeploymentApproveInput struct {
	GitDeploymentPath
	GitWriteHeaders
}

type gitRunnerSelectorsInput struct {
	GitServicePath
	ConnectionID string `query:"connection_id" required:"true" pattern:"^ghc_[0-9A-HJKMNP-TV-Z]{26}$"`
	RepositoryID int64  `query:"repository_id" required:"true" minimum:"1"`
}

type gitListOutput[T any] struct{ Body apiTypes.Page[T] }
type gitDeploymentOutput struct{ Body apiTypes.GitDeployment }

type GithubScopeQuery struct {
	Kind string `query:"scope_kind" required:"true" enum:"tenant,project,environment"`
	ID   string `query:"scope_id" required:"true" pattern:"^(tnt|prj|env)_[0-9A-HJKMNP-TV-Z]{26}$"`
}

func (scope GithubScopeQuery) scope() (apiTypes.GitHubConnectionScope, error) {
	value := apiTypes.GitHubConnectionScope{Kind: scope.Kind, ID: scope.ID}
	return value, validateGitHubScope(value)
}

type GithubConnectionPath struct {
	ConnectionID string `path:"id" pattern:"^ghc_[0-9A-HJKMNP-TV-Z]{26}$"`
	GithubScopeQuery
}

type githubConnectionListInput struct {
	GithubScopeQuery
	GitPageQuery
}

type githubConnectionCreateInput struct {
	GitWriteHeaders
	RawBody []byte
}

type GithubConnectionRevokeInput struct {
	GithubConnectionPath
	GitRevisionHeader
	GitWriteHeaders
}

type githubConnectionRotateInput struct {
	GithubConnectionRevokeInput
	RawBody []byte
}

type githubRepositoryListInput struct {
	GithubConnectionPath
	Limit  int    `query:"limit" minimum:"1" maximum:"100"`
	Cursor string `query:"cursor" maxLength:"4096"`
}

type githubConnectionOutput struct {
	ETag string `header:"ETag"`
	Body apiTypes.GitHubConnection
}

func (routes gitDeliveryRoutes) operation(id, method, path, summary string) huma.Operation {
	return huma.Operation{
		OperationID: id, Method: method, Path: path, Summary: summary, Tags: []string{"Git Delivery"},
		Errors: []int{http.StatusServiceUnavailable}, Middlewares: huma.Middlewares{routes.requireOperator},
	}
}

func (routes gitDeliveryRoutes) requireOperator(ctx huma.Context, next func(huma.Context)) {
	if routes.operator == nil {
		_ = huma.WriteErr(routes.server.API, ctx, http.StatusServiceUnavailable, "Git delivery is unavailable")
		return
	}
	next(ctx)
}

func gitMutation[T any](s *Server, operation huma.Operation, status int, schemaName string) huma.Operation {
	operation.DefaultStatus = status
	schema := openAPISchema[T](s.API.OpenAPI().Components.Schemas, schemaName)
	operation.Responses = map[string]*huma.Response{
		strconv.Itoa(status): {
			Description: http.StatusText(status),
			Content:     map[string]*huma.MediaType{"application/json": {Schema: schema}},
		},
	}
	return operation
}

func gitCredentialMutation[T any](s *Server, operation huma.Operation, schemaName string) huma.Operation {
	operation.SkipValidateBody = true
	operation.MaxBodyBytes = 64 * 1024
	operation.RequestBody = &huma.RequestBody{
		Required: true,
		Content: map[string]*huma.MediaType{
			"application/json": {Schema: openAPISchema[T](s.API.OpenAPI().Components.Schemas, schemaName)},
		},
	}
	return operation
}

func (s *Server) registerGitDelivery(operator GitDeliveryOperator) {
	routes := gitDeliveryRoutes{server: s, operator: operator}
	const (
		binding     = "/services/{id}/git-deployment"
		deployments = "/services/{id}/git-deployments"
		deployment  = deployments + "/{deployment}"
		connections = "/github-connections"
		connection  = connections + "/{id}"
	)
	huma.Register(
		s.API,
		routes.operation(
			"git-deployment.binding-show",
			http.MethodGet,
			binding,
			"Show a Service's GitHub deployment binding",
		),
		routes.binding,
	)
	huma.Register(s.API, gitMutation[apiTypes.GitDeploymentBindingState](s,
		routes.operation("git-deployment.bind", http.MethodPut, binding, "Save a Service's GitHub deployment binding"),
		http.StatusOK, "GitDeploymentBindingState"), routes.bind)
	huma.Register(
		s.API,
		routes.operation("git-deployment.list", http.MethodGet, deployments, "List a Service's GitHub deployments"),
		routes.list,
	)
	huma.Register(s.API, gitMutation[apiTypes.GitDeployment](
		s,
		routes.operation(
			"git-deployment.start",
			http.MethodPost,
			deployments,
			"Start a GitHub deployment for a Service",
		),
		http.StatusAccepted,
		"GitDeployment",
	), routes.start)
	huma.Register(
		s.API,
		routes.operation("git-deployment.show", http.MethodGet, deployment, "Show a Service's GitHub deployment"),
		routes.show,
	)
	approve := gitMutation[apiTypes.GitDeployment](
		s,
		routes.operation(
			"git-deployment.approve",
			http.MethodPost,
			deployment+"/approve",
			"Approve a verified GitHub build for deployment",
		),
		http.StatusAccepted,
		"GitDeployment",
	)
	approve.Middlewares = append(approve.Middlewares, s.rejectTaskMutationBody, s.rejectTaskMutationQuery)
	huma.Register(s.API, approve, routes.approve)
	huma.Register(s.API, routes.operation(
		"git-deployment.runner-selectors",
		http.MethodGet,
		"/services/{id}/git-runner-selectors",
		"List Runner selectors available to this Service and repository",
	), routes.runnerSelectors)

	huma.Register(
		s.API,
		routes.operation(
			"github-connection.list",
			http.MethodGet,
			connections,
			"List GitHub App connections in a scope",
		),
		routes.connectionList,
	)
	create := gitMutation[apiTypes.GitHubConnection](
		s,
		routes.operation(
			"github-connection.create",
			http.MethodPost,
			connections,
			"Register credentials for an operator-owned GitHub App installation",
		),
		http.StatusCreated,
		"GitHubConnection",
	)
	create = gitCredentialMutation[apiTypes.GitHubConnectionCreateRequest](s, create, "GitHubConnectionCreateRequest")
	huma.Register(s.API, create, routes.connectionCreate)
	delete(create.RequestBody.Content, "application/octet-stream")
	huma.Register(
		s.API,
		routes.operation("github-connection.show", http.MethodGet, connection, "Show GitHub App connection metadata"),
		routes.connectionShow,
	)
	revoke := routes.operation(
		"github-connection.revoke",
		http.MethodDelete,
		connection,
		"Revoke a GitHub App connection",
	)
	revoke.DefaultStatus = http.StatusNoContent
	revoke.Middlewares = append(revoke.Middlewares, s.rejectTaskMutationBody)
	revoke.Responses = map[string]*huma.Response{"204": {Description: "GitHub App connection revoked"}}
	huma.Register(s.API, revoke, routes.connectionRevoke)
	rotate := gitMutation[apiTypes.GitHubConnection](
		s,
		routes.operation(
			"github-connection.rotate",
			http.MethodPut,
			connection+"/credentials",
			"Replace GitHub App connection credentials",
		),
		http.StatusOK,
		"GitHubConnection",
	)
	rotate = gitCredentialMutation[apiTypes.GitHubConnectionCredentialsWrite](
		s,
		rotate,
		"GitHubConnectionCredentialsWrite",
	)
	huma.Register(s.API, rotate, routes.connectionRotate)
	delete(rotate.RequestBody.Content, "application/octet-stream")
	huma.Register(s.API, routes.operation("github-connection.repositories", http.MethodGet,
		connection+"/repositories", "List repositories available to the GitHub App installation"), routes.repositories)

	s.registerGitRepositories(routes)
	s.setRoutePolicy("PUT /api/v1"+binding, routePolicy{body: jsonBody})
	s.setRoutePolicy("POST /api/v1"+deployments, routePolicy{body: jsonBody})
	s.setRoutePolicy("POST /api/v1"+connections, routePolicy{body: jsonBody})
	s.setRoutePolicy("PUT /api/v1"+connection+"/credentials", routePolicy{body: jsonBody})
}

type gitDeliveryRoutes struct {
	server   *Server
	operator GitDeliveryOperator
}

func (routes gitDeliveryRoutes) binding(ctx context.Context, input *GitServicePath) (*gitBindingOutput, error) {
	result, err := routes.operator.GitDeploymentBinding(ctx, input.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &gitBindingOutput{ETag: gitETag(result.Revision), Body: result}, nil
}

func (routes gitDeliveryRoutes) bind(ctx context.Context, input *gitBindingInput) (*serviceMutationOutput, error) {
	revision, err := gitWriteRevision(input.IfMatch, true)
	if err != nil {
		return nil, err
	}
	return routes.mutation(routes.operator.PutGitDeploymentBinding(ctx, input.ID, revision, input.Body, input.Key))
}

func (routes gitDeliveryRoutes) list(
	ctx context.Context,
	input *gitDeploymentListInput,
) (*gitListOutput[apiTypes.GitDeployment], error) {
	return gitPageOutput(routes.operator.ListGitDeployments(ctx, input.ID,
		keyvalue.PageRequest{Limit: input.Limit, Cursor: input.Cursor}))
}

func (routes gitDeliveryRoutes) start(
	ctx context.Context,
	input *gitDeploymentStartInput,
) (*serviceMutationOutput, error) {
	var body apiTypes.GitDeploymentStartRequest
	if input.Body != nil {
		body = *input.Body
	}
	return routes.mutation(routes.operator.StartGitDeployment(ctx, input.ID, body, input.Key))
}

func (routes gitDeliveryRoutes) show(ctx context.Context, input *GitDeploymentPath) (*gitDeploymentOutput, error) {
	result, err := routes.operator.GitDeployment(ctx, input.ID, input.Deployment)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &gitDeploymentOutput{Body: result}, nil
}

func (routes gitDeliveryRoutes) approve(
	ctx context.Context,
	input *gitDeploymentApproveInput,
) (*serviceMutationOutput, error) {
	return routes.mutation(routes.operator.ApproveGitDeployment(ctx, input.ID, input.Deployment, input.Key))
}

func (routes gitDeliveryRoutes) runnerSelectors(
	ctx context.Context,
	input *gitRunnerSelectorsInput,
) (*gitListOutput[apiTypes.GitRunnerSelector], error) {
	return gitPageOutput(routes.operator.ListGitRunnerSelectors(ctx, input.ID, input.ConnectionID, input.RepositoryID))
}

func (routes gitDeliveryRoutes) connectionList(
	ctx context.Context,
	input *githubConnectionListInput,
) (*gitListOutput[apiTypes.GitHubConnection], error) {
	scope, err := input.scope()
	if err != nil {
		return nil, err
	}
	return gitPageOutput(routes.operator.ListGitHubConnections(ctx, scope, input.request()))
}

func (routes gitDeliveryRoutes) connectionCreate(
	ctx context.Context,
	input *githubConnectionCreateInput,
) (*serviceMutationOutput, error) {
	defer clear(input.RawBody)
	body, err := decodeGitCredentials[apiTypes.GitHubConnectionCreateRequest](input.RawBody)
	if err != nil {
		return nil, err
	}
	if err := validateGitHubScope(body.Scope); err != nil {
		return nil, err
	}
	if body.AppID <= 0 || body.InstallationID <= 0 || len(body.Name) == 0 || len(body.Name) > 100 {
		return nil, errs.New(
			errs.KindValidationFailed,
			"GitHub App connection name or installation identity is invalid",
		)
	}
	if err := validateGitHubCredentialFields(body.GitHubConnectionCredentialsWrite); err != nil {
		return nil, err
	}
	return routes.mutation(routes.operator.CreateGitHubConnection(ctx, body, input.Key))
}

func (routes gitDeliveryRoutes) connectionShow(
	ctx context.Context,
	input *GithubConnectionPath,
) (*githubConnectionOutput, error) {
	scope, err := input.scope()
	if err != nil {
		return nil, err
	}
	result, revision, err := routes.operator.GitHubConnection(ctx, scope, input.ConnectionID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &githubConnectionOutput{ETag: gitETag(revision), Body: result}, nil
}

func (routes gitDeliveryRoutes) connectionRevoke(
	ctx context.Context,
	input *GithubConnectionRevokeInput,
) (*serviceMutationOutput, error) {
	scope, err := input.scope()
	if err != nil {
		return nil, err
	}
	revision, err := gitWriteRevision(input.IfMatch, false)
	if err != nil {
		return nil, err
	}
	return routes.mutation(routes.operator.RevokeGitHubConnection(ctx, scope, input.ConnectionID, revision, input.Key))
}

func (routes gitDeliveryRoutes) connectionRotate(
	ctx context.Context,
	input *githubConnectionRotateInput,
) (*serviceMutationOutput, error) {
	defer clear(input.RawBody)
	scope, err := input.scope()
	if err != nil {
		return nil, err
	}
	revision, err := gitWriteRevision(input.IfMatch, false)
	if err != nil {
		return nil, err
	}
	body, err := decodeGitCredentials[apiTypes.GitHubConnectionCredentialsWrite](input.RawBody)
	if err != nil {
		return nil, err
	}
	if err := validateGitHubCredentialFields(body); err != nil {
		return nil, err
	}
	return routes.mutation(
		routes.operator.RotateGitHubConnection(ctx, scope, input.ConnectionID, revision, body, input.Key),
	)
}

func (routes gitDeliveryRoutes) repositories(
	ctx context.Context,
	input *githubRepositoryListInput,
) (*gitListOutput[apiTypes.GitHubRepository], error) {
	scope, err := input.scope()
	if err != nil {
		return nil, err
	}
	return gitPageOutput(routes.operator.ListGitHubRepositories(ctx, scope, input.ConnectionID,
		keyvalue.PageRequest{Limit: input.Limit, Cursor: input.Cursor}))
}

func (routes gitDeliveryRoutes) mutation(
	response idempotencyrecord.IdempotencyResponse,
	err error,
) (*serviceMutationOutput, error) {
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return routes.server.serviceMutationResponse(response), nil
}

func gitPageOutput[T any](page apiTypes.Page[T], err error) (*gitListOutput[T], error) {
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	if page.Items == nil {
		page.Items = []T{}
	}
	return &gitListOutput[T]{Body: page}, nil
}

func validateGitHubScope(scope apiTypes.GitHubConnectionScope) error {
	var kind ids.Kind
	switch scope.Kind {
	case "tenant":
		kind = ids.KindTenant
	case "project":
		kind = ids.KindProject
	case "environment":
		kind = ids.KindEnvironment
	}
	if kind == "" || ids.Validate(kind, scope.ID) != nil {
		return errs.New(errs.KindValidationFailed, "GitHub App connection scope requires a matching stable ID")
	}
	return nil
}

func gitETag(revision int64) string { return strconv.Quote(strconv.FormatInt(revision, 10)) }

func gitWriteRevision(header string, allowInitial bool) (int64, error) {
	value, err := strconv.Unquote(header)
	revision, parseErr := strconv.ParseInt(value, 10, 64)
	if err != nil || parseErr != nil || header != gitETag(revision) || revision < 0 ||
		(!allowInitial && revision == 0) {
		return 0, errs.New(errs.KindValidationFailed, "If-Match requires the quoted resource revision")
	}
	return revision, nil
}
