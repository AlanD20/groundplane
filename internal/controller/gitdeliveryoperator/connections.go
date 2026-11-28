package gitdeliveryoperator

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/githubconnection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/githubapp"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const connectionsRoute = "/github-connections"
const connectionRoute = connectionsRoute + "/{id}"

func (operator *Operator) ListGitHubConnections(
	ctx context.Context,
	scope api.GitHubConnectionScope,
	page keyvalue.PageRequest,
) (api.Page[api.GitHubConnection], error) {
	owner := connectionScope(scope)
	if _, _, err := operator.resolveOwner(ctx, owner); err != nil {
		return api.Page[api.GitHubConnection]{}, err
	}
	found, err := operator.connections.List(ctx, owner, page)
	if err != nil {
		return api.Page[api.GitHubConnection]{}, err
	}
	result := api.Page[api.GitHubConnection]{
		Items: make(
			[]api.GitHubConnection,
			0,
			len(found.Items),
		), NextCursor: found.NextCursor, Revision: found.Revision,
	}
	for _, connection := range found.Items {
		result.Items = append(result.Items, publicConnection(connection.Record))
	}
	return result, nil
}

func (operator *Operator) GitHubConnection(
	ctx context.Context,
	scope api.GitHubConnectionScope,
	id string,
) (api.GitHubConnection, int64, error) {
	owner := connectionScope(scope)
	if _, _, err := operator.resolveOwner(ctx, owner); err != nil {
		return api.GitHubConnection{}, 0, err
	}
	connection, err := operator.connections.Get(ctx, owner, id)
	if err != nil {
		return api.GitHubConnection{}, 0, err
	}
	return publicConnection(connection.Record), connection.Revision, nil
}

func (operator *Operator) CreateGitHubConnection(
	ctx context.Context,
	input api.GitHubConnectionCreateRequest,
	key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	scope := connectionScope(input.Scope)
	if _, _, err := operator.resolveOwner(ctx, scope); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return operator.mutate(ctx, mutation{
		scope: input.Scope.Kind, scopeID: input.Scope.ID, method: http.MethodPost, route: connectionsRoute,
		key: key, body: encodeMutationBody(input), status: http.StatusCreated,
	}, func(publish publish) (int64, error) {
		credentials := githubCredentials(input.GitHubConnectionCredentialsWrite)
		defer clear(credentials.PrivateKeyPEM)
		defer clear(credentials.WebhookSecret)
		if err := verifyInstallation(ctx, input.AppID, input.InstallationID, credentials.PrivateKeyPEM); err != nil {
			return 0, err
		}
		_, err := operator.connections.Create(ctx, githubconnection.CreateInput{
			Scope: scope, Name: input.Name, AppID: input.AppID, InstallationID: input.InstallationID,
			PrivateKeyPEM: credentials.PrivateKeyPEM, WebhookSecret: credentials.WebhookSecret, CreatedAt: time.Now().UTC(),
		}, nil, func(connection githubconnection.Connection) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
			return publish(publicConnection(connection))
		})
		return 0, err
	}, nil)
}

func (operator *Operator) RotateGitHubConnection(
	ctx context.Context,
	scope api.GitHubConnectionScope,
	id string,
	revision int64,
	input api.GitHubConnectionCredentialsWrite,
	key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	return operator.changeGitHubConnection(ctx, scope, id, revision, key, &input)
}

func (operator *Operator) RevokeGitHubConnection(
	ctx context.Context,
	scope api.GitHubConnectionScope,
	id string,
	revision int64,
	key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	return operator.changeGitHubConnection(ctx, scope, id, revision, key, nil)
}

func (operator *Operator) changeGitHubConnection(
	ctx context.Context,
	scope api.GitHubConnectionScope,
	id string,
	revision int64,
	key string,
	input *api.GitHubConnectionCredentialsWrite,
) (idempotencyrecord.IdempotencyResponse, error) {
	owner := connectionScope(scope)
	if _, _, err := operator.resolveOwner(ctx, owner); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	request := mutation{
		scope: scope.Kind, scopeID: scope.ID, route: connectionRoute, method: http.MethodDelete, key: key,
		path: []requestidempotency.PathBinding{pathBinding("id", id)}, expected: revision, status: http.StatusNoContent,
	}
	if input != nil {
		request.route += "/credentials"
		request.method, request.body, request.status = http.MethodPut, encodeMutationBody(*input), http.StatusOK
	}
	return operator.mutate(ctx, request, func(publish publish) (int64, error) {
		connection, err := operator.connections.Get(ctx, owner, id)
		if err != nil {
			return 0, err
		}
		if revision != connection.Revision {
			return 0, errs.New(errs.KindStateConflict, "GitHub connection revision changed")
		}
		publication := func(connection githubconnection.Connection) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
			return publish(publicConnection(connection))
		}
		if input == nil {
			_, err = operator.connections.Revoke(
				ctx,
				owner,
				id,
				revision,
				connection.Record.Generation,
				time.Now().UTC(),
				publication,
			)
			return 0, err
		}
		credentials := githubCredentials(*input)
		defer clear(credentials.PrivateKeyPEM)
		defer clear(credentials.WebhookSecret)
		if err := verifyInstallation(ctx, connection.Record.AppID, connection.Record.InstallationID, credentials.PrivateKeyPEM); err != nil {
			return 0, err
		}
		_, err = operator.connections.Rotate(
			ctx,
			owner,
			id,
			revision,
			connection.Record.Generation,
			credentials,
			time.Now().UTC(),
			publication,
		)
		return 0, err
	}, nil)
}

func (operator *Operator) ListGitHubRepositories(
	ctx context.Context,
	scope api.GitHubConnectionScope,
	connectionID string,
	request keyvalue.PageRequest,
) (api.Page[api.GitHubRepository], error) {
	owner := connectionScope(scope)
	if _, _, err := operator.resolveOwner(ctx, owner); err != nil {
		return api.Page[api.GitHubRepository]{}, err
	}
	connection, err := operator.connections.Get(ctx, owner, connectionID)
	if err != nil {
		return api.Page[api.GitHubRepository]{}, err
	}
	identity := connectionID + "." + strconv.FormatUint(connection.Record.Generation, 10)
	prefix, page, limit, err := githubCatalogPage(identity, request)
	if err != nil {
		return api.Page[api.GitHubRepository]{}, err
	}
	result := api.Page[api.GitHubRepository]{Items: []api.GitHubRepository{}}
	err = operator.connections.WithCredentials(ctx, owner, connectionID, connection.Record.Generation,
		func(record githubconnection.Connection, credentials githubconnection.Credentials) error {
			client, err := githubapp.New(record.AppID, credentials.PrivateKeyPEM)
			if err != nil {
				return err
			}
			repositories, hasMore, err := client.Repositories(ctx, record.InstallationID, page, limit)
			if err != nil {
				return err
			}
			for _, repository := range repositories {
				result.Items = append(result.Items, api.GitHubRepository{
					ID: repository.ID, FullName: repository.Owner + "/" + repository.Name,
					DefaultBranch: repository.DefaultBranch, Private: repository.Private,
				})
			}
			if hasMore {
				result.NextCursor = prefix + strconv.Itoa(page+1)
			}
			return nil
		})
	return result, err
}

func githubCredentials(input api.GitHubConnectionCredentialsWrite) githubconnection.Credentials {
	return githubconnection.Credentials{
		PrivateKeyPEM: []byte(input.PrivateKeyPEM),
		WebhookSecret: []byte(input.WebhookSecret),
	}
}

func verifyInstallation(ctx context.Context, appID, installationID int64, privateKey []byte) error {
	client, err := githubapp.New(appID, privateKey)
	if err != nil {
		return err
	}
	_, _, err = client.Repositories(ctx, installationID, 1, 1)
	return err
}

func publicConnection(connection githubconnection.Connection) api.GitHubConnection {
	return api.GitHubConnection{
		ID: connection.ID, Scope: publicScope(connection.Scope), Name: connection.Name,
		AppID: connection.AppID, InstallationID: connection.InstallationID, Generation: int64(connection.Generation),
		CreatedAt: connection.CreatedAt, UpdatedAt: connection.UpdatedAt, RevokedAt: connection.RevokedAt,
	}
}

func githubCatalogPage(identity string, request keyvalue.PageRequest) (string, int, int, error) {
	limit := request.Limit
	if limit == 0 {
		limit = 50
	}
	if limit < 1 || limit > 100 || request.Revision != 0 {
		return "", 0, 0, errs.New(errs.KindValidationFailed, "invalid GitHub catalog page")
	}
	prefix := identity + "." + strconv.Itoa(limit) + "."
	page := 1
	if request.Cursor != "" {
		after, ok := strings.CutPrefix(request.Cursor, prefix)
		if !ok {
			return "", 0, 0, errs.New(
				errs.KindValidationFailed,
				"GitHub catalog cursor is outside this collection or page size",
			)
		}
		parsed, err := strconv.Atoi(after)
		if err != nil || parsed < 2 {
			return "", 0, 0, errs.New(errs.KindValidationFailed, "invalid GitHub catalog cursor")
		}
		page = parsed
	}
	return prefix, page, limit, nil
}
