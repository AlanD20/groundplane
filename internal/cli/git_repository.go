package cli

import (
	"strconv"

	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newGitHubRepositoryRunnerDefaultCmd(scopeKind *string) *cobra.Command {
	return &cobra.Command{
		Use: "runner-default <connection> <repository-id>", Short: "Show the repository's saved Runner selector", Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			scope, connection, repository, err := resolveGitHubRepository(cmd, *scopeKind, args)
			if err != nil {
				return err
			}
			state, _, err := fromContext(
				cmd,
			).Client.ShowGitRepositoryRunnerDefault(
				cmd.Context(),
				scope,
				connection,
				repository,
			)
			if err != nil {
				return err
			}
			return renderGitRepositoryRunnerDefault(cmd, state)
		},
	}
}

func newGitHubRepositorySetRunnerDefaultCmd(scopeKind *string) *cobra.Command {
	var input api.GitRepositoryRunnerDefaultWrite
	var ifMatch, key string
	command := &cobra.Command{
		Use: "set-runner-default <connection> <repository-id>", Short: "Save a Runner selector for the repository", Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			scope, connection, repository, err := resolveGitHubRepository(cmd, *scopeKind, args)
			if err != nil {
				return err
			}
			state, err := fromContext(
				cmd,
			).Client.PutGitRepositoryRunnerDefault(
				cmd.Context(),
				scope,
				connection,
				repository,
				input,
				ifMatch,
				key,
			)
			if err != nil {
				return err
			}
			return renderGitRepositoryRunnerDefault(cmd, state)
		},
	}
	command.Flags().StringVar(&input.Label, "runner-label", "", "GitHub runner label")
	command.Flags().StringVar(&input.Group, "runner-group", "", "GitHub runner group name")
	command.Flags().Int64Var(&input.GroupID, "runner-group-id", 0, "GitHub runner group ID")
	_ = command.MarkFlagRequired("runner-label")
	gitMutationFlags(command, &ifMatch, &key)
	return command
}

func newGitHubRepositoryRemoveRunnerDefaultCmd(scopeKind *string) *cobra.Command {
	var ifMatch, key string
	command := &cobra.Command{
		Use: "remove-runner-default <connection> <repository-id>", Short: "Remove the repository's saved Runner selector", Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			scope, connection, repository, err := resolveGitHubRepository(cmd, *scopeKind, args)
			if err != nil {
				return err
			}
			err = fromContext(
				cmd,
			).Client.RemoveGitRepositoryRunnerDefault(
				cmd.Context(),
				scope,
				connection,
				repository,
				ifMatch,
				key,
			)
			if err != nil {
				return err
			}
			fields := map[string]any{"connection_id": connection, "repository_id": repository, "status": "removed"}
			return renderGitItem(cmd, fields, fields)
		},
	}
	gitMutationFlags(command, &ifMatch, &key)
	return command
}

func newGitHubRepositoryWorkflowsCmd(scopeKind *string) *cobra.Command {
	var limit int
	var cursor string
	command := &cobra.Command{
		Use: "workflows <connection> <repository-id>", Short: "List GitHub Actions workflows in the repository", Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			scope, connection, repository, err := resolveGitHubRepository(cmd, *scopeKind, args)
			if err != nil {
				return err
			}
			page, err := fromContext(
				cmd,
			).Client.ListGitHubWorkflows(
				cmd.Context(),
				scope,
				connection,
				repository,
				limit,
				cursor,
			)
			if err != nil {
				return err
			}
			return renderGitPage(cmd, page, func(workflow api.GitHubWorkflow) map[string]any {
				return map[string]any{
					"id":    workflow.ID,
					"name":  workflow.Name,
					"path":  workflow.Path,
					"state": workflow.State,
				}
			}, "id", "name", "path", "state")
		},
	}
	gitPageFlags(command, &limit, &cursor)
	return command
}

func resolveGitHubRepository(
	cmd *cobra.Command,
	scopeKind string,
	args []string,
) (api.GitHubConnectionScope, string, int64, error) {
	repository, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || repository <= 0 {
		return api.GitHubConnectionScope{}, "", 0, errs.New(
			errs.KindValidationFailed,
			"repository ID must be a positive GitHub repository ID",
		)
	}
	scope, err := resolveGitHubConnectionScope(cmd, scopeKind)
	if err != nil {
		return scope, "", 0, err
	}
	if fromContext(cmd).Scope.AsID {
		return scope, args[0], repository, nil
	}
	// Keep the selected repository scope while resolving inherited connections
	// from the nearest owning scope by name.
	owner := scope
	for {
		connection, err := findGitHubConnection(cmd, owner, args[0])
		if err != nil || connection != "" {
			return scope, connection, repository, err
		}
		if owner.Kind == "tenant" {
			return scope, "", 0, errs.Newf(
				errs.KindRequestNotFound,
				"GitHub connection %q was not found in the selected scope or its parents",
				args[0],
			)
		}
		parent := "tenant"
		if owner.Kind == "environment" {
			parent = "project"
		}
		owner, err = resolveGitHubConnectionScope(cmd, parent)
		if err != nil {
			return scope, "", 0, err
		}
	}
}

func renderGitRepositoryRunnerDefault(cmd *cobra.Command, state api.GitRepositoryRunnerDefaultState) error {
	fields := map[string]any{"revision": state.Revision, "configured": state.Default != nil}
	if selector := state.Default; selector != nil {
		fields["connection_id"] = selector.ConnectionID
		fields["repository_id"] = selector.RepositoryID
		fields["runner_label"] = selector.Label
		fields["runner_group"] = selector.Group
		fields["runner_group_id"] = selector.GroupID
	}
	return renderGitItem(cmd, state, fields)
}
