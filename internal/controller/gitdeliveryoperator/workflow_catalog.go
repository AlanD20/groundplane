package gitdeliveryoperator

import (
	"context"
	"strconv"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/githubapp"
	api "github.com/AlanD20/groundplane/pkg/api"
)

func (operator *Operator) ListGitHubWorkflows(
	ctx context.Context,
	scope api.GitHubConnectionScope,
	connectionID string,
	repositoryID int64,
	request keyvalue.PageRequest,
) (api.Page[api.GitHubWorkflow], error) {
	binding, _, _, err := operator.repositoryTarget(ctx, scope, connectionID, repositoryID)
	if err != nil {
		return api.Page[api.GitHubWorkflow]{}, err
	}
	identity := "workflows." + connectionID + "." + strconv.FormatInt(
		binding.ConnectionGeneration,
		10,
	) + "." + strconv.FormatInt(
		repositoryID,
		10,
	)
	cursorPrefix, page, limit, err := githubCatalogPage(identity, request)
	if err != nil {
		return api.Page[api.GitHubWorkflow]{}, err
	}
	result := api.Page[api.GitHubWorkflow]{Items: []api.GitHubWorkflow{}}
	err = operator.withRepository(ctx, binding, func(client *githubapp.Client, scope githubapp.Scope) error {
		found, more, err := client.Workflows(ctx, scope, page, limit)
		if err != nil {
			return err
		}
		for _, workflow := range found {
			result.Items = append(result.Items, api.GitHubWorkflow{
				ID: workflow.ID, Name: workflow.Name, Path: workflow.Path, State: workflow.State,
			})
		}
		if more {
			result.NextCursor = cursorPrefix + strconv.Itoa(page+1)
		}
		return nil
	})
	return result, err
}
