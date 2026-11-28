package cli

import (
	"fmt"
	"strings"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newGitDeploymentCmd() *cobra.Command {
	command := &cobra.Command{Use: "git-deployment", Short: "Configure and run Git deployments for a service"}
	command.AddCommand(
		newGitDeploymentBindingCmd(),
		newGitDeploymentBindCmd(),
		newGitDeploymentListCmd(),
		newGitDeploymentShowCmd(),
		newGitDeploymentRunCmd(),
		newGitDeploymentApproveCmd(),
		newGitRunnerSelectorsCmd(),
	)
	return command
}

func newGitDeploymentBindingCmd() *cobra.Command {
	return &cobra.Command{
		Use: "binding <service>", Short: "Show the service's Git deployment configuration", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			service, err := resolveServiceTarget(cmd, args[0])
			if err != nil {
				return err
			}
			state, _, err := fromContext(cmd).Client.ShowGitDeploymentBinding(cmd.Context(), service)
			if err != nil {
				return err
			}
			return renderGitDeploymentBinding(cmd, state)
		},
	}
}

func newGitDeploymentBindCmd() *cobra.Command {
	var input apiTypes.GitDeploymentBindingWrite
	var ifMatch, key string
	command := &cobra.Command{
		Use: "bind <service>", Short: "Set the repository, workflow, and runner for a service", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if input.InheritRunnerDefault &&
				(input.RunnerLabel != "" || input.RunnerGroup != "" || input.RunnerGroupID != 0) {
				return errs.New(
					errs.KindValidationFailed,
					"choose either --inherit-runner-default or explicit Runner settings",
				)
			}
			if !input.InheritRunnerDefault && strings.TrimSpace(input.RunnerLabel) == "" {
				return errs.New(errs.KindValidationFailed, "provide --runner-label or --inherit-runner-default")
			}
			service, err := resolveServiceTarget(cmd, args[0])
			if err != nil {
				return err
			}
			state, err := fromContext(cmd).Client.BindGitDeployment(cmd.Context(), service, input, ifMatch, key)
			if err != nil {
				return err
			}
			return renderGitDeploymentBinding(cmd, state)
		},
	}
	flags := command.Flags()
	flags.StringVar(&input.ConnectionID, "connection-id", "", "GitHub connection ID")
	flags.Int64Var(&input.RepositoryID, "repository-id", 0, "GitHub repository ID")
	flags.Int64Var(&input.WorkflowID, "workflow-id", 0, "GitHub Actions workflow ID")
	flags.StringVar(&input.WorkflowRef, "workflow-ref", "", "Git ref containing the reviewed workflow")
	flags.StringVar(&input.WorkflowCommit, "workflow-commit", "", "expected commit containing the reviewed workflow")
	flags.StringVar(&input.Branch, "branch", "", "application branch to deploy")
	flags.StringVar(&input.Dockerfile, "dockerfile", "Dockerfile", "repository-relative Dockerfile path")
	flags.StringVar(&input.BuildContext, "build-context", ".", "repository-relative build context")
	flags.BoolVar(
		&input.InheritRunnerDefault,
		"inherit-runner-default",
		false,
		"use the repository's saved Runner selector",
	)
	flags.StringVar(&input.RunnerLabel, "runner-label", "", "explicit GitHub runner label")
	flags.StringVar(&input.RunnerGroup, "runner-group", "", "GitHub runner group name")
	flags.Int64Var(&input.RunnerGroupID, "runner-group-id", 0, "GitHub runner group ID")
	flags.BoolVar(&input.Automatic, "automatic", false, "start deployments on pushes to the configured branch")
	flags.BoolVar(&input.RequireApproval, "require-approval", true, "require approval before deploying a built image")
	for _, name := range []string{"connection-id", "repository-id", "workflow-id", "workflow-ref", "branch"} {
		_ = command.MarkFlagRequired(name)
	}
	gitMutationFlags(command, &ifMatch, &key)
	return command
}

func newGitDeploymentListCmd() *cobra.Command {
	var limit int
	var cursor string
	command := &cobra.Command{
		Use: "list <service>", Short: "List the service's Git deployment runs", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			service, err := resolveServiceTarget(cmd, args[0])
			if err != nil {
				return err
			}
			page, err := fromContext(cmd).Client.ListGitDeployments(cmd.Context(), service, limit, cursor)
			if err != nil {
				return err
			}
			return renderGitPage(cmd, page, gitDeploymentFields, "id", "phase", "commit", "created_at", "error_code")
		},
	}
	gitPageFlags(command, &limit, &cursor)
	return command
}

func newGitDeploymentShowCmd() *cobra.Command {
	return &cobra.Command{
		Use: "show <service> <deployment-id>", Short: "Show a Git deployment run and its linked tasks", Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			service, err := resolveServiceTarget(cmd, args[0])
			if err != nil {
				return err
			}
			deployment, err := fromContext(cmd).Client.ShowGitDeployment(cmd.Context(), service, args[1])
			if err != nil {
				return err
			}
			return renderGitItem(cmd, deployment, gitDeploymentFields(deployment))
		},
	}
}

func newGitDeploymentRunCmd() *cobra.Command {
	var input apiTypes.GitDeploymentStartRequest
	var key string
	command := &cobra.Command{
		Use: "run <service>", Short: "Start a Git deployment run", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			service, err := resolveServiceTarget(cmd, args[0])
			if err != nil {
				return err
			}
			deployment, err := fromContext(cmd).Client.StartGitDeployment(cmd.Context(), service, input, key)
			if err != nil {
				return err
			}
			return renderGitItem(cmd, deployment, gitDeploymentFields(deployment))
		},
	}
	command.Flags().
		StringVar(&input.ExpectedCommit, "expected-commit", "", "expected commit at the configured branch head")
	gitMutationFlags(command, nil, &key)
	return command
}

func newGitDeploymentApproveCmd() *cobra.Command {
	var key string
	command := &cobra.Command{
		Use: "approve <service> <deployment-id>", Short: "Approve a built image for deployment", Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			service, err := resolveServiceTarget(cmd, args[0])
			if err != nil {
				return err
			}
			deployment, err := fromContext(cmd).Client.ApproveGitDeployment(cmd.Context(), service, args[1], key)
			if err != nil {
				return err
			}
			return renderGitItem(cmd, deployment, gitDeploymentFields(deployment))
		},
	}
	gitMutationFlags(command, nil, &key)
	return command
}

func newGitRunnerSelectorsCmd() *cobra.Command {
	var connection string
	var repository int64
	command := &cobra.Command{
		Use: "runner-selectors <service>", Short: "List runners eligible for the service and repository", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			service, err := resolveServiceTarget(cmd, args[0])
			if err != nil {
				return err
			}
			page, err := fromContext(cmd).Client.ListGitRunnerSelectors(cmd.Context(), service, connection, repository)
			if err != nil {
				return err
			}
			return renderGitPage(cmd, page, func(selector apiTypes.GitRunnerSelector) map[string]any {
				return map[string]any{
					"label": selector.Label, "group": selector.Group, "group_id": selector.GroupID,
					"matching": selector.Matching, "available": selector.Available,
				}
			}, "label", "group", "group_id", "matching", "available")
		},
	}
	command.Flags().StringVar(&connection, "connection-id", "", "GitHub connection ID")
	command.Flags().Int64Var(&repository, "repository-id", 0, "GitHub repository ID")
	_ = command.MarkFlagRequired("connection-id")
	_ = command.MarkFlagRequired("repository-id")
	return command
}

func renderGitDeploymentBinding(cmd *cobra.Command, state apiTypes.GitDeploymentBindingState) error {
	fields := map[string]any{"revision": state.Revision, "configured": state.Binding != nil}
	if binding := state.Binding; binding != nil {
		fields["connection_id"] = binding.ConnectionID
		fields["repository_id"] = binding.RepositoryID
		fields["workflow_id"] = binding.WorkflowID
		fields["workflow_ref"] = binding.WorkflowRef
		fields["workflow_commit"] = binding.WorkflowCommit
		fields["branch"] = binding.Branch
		fields["dockerfile"] = binding.Dockerfile
		fields["build_context"] = binding.BuildContext
		fields["inherit_runner_default"] = binding.InheritRunnerDefault
		fields["runner_label"] = binding.RunnerLabel
		fields["runner_group"] = binding.RunnerGroup
		fields["runner_group_id"] = binding.RunnerGroupID
		fields["automatic"] = binding.Automatic
		fields["require_approval"] = binding.RequireApproval
	}
	return renderGitItem(cmd, state, fields)
}

func gitDeploymentFields(deployment apiTypes.GitDeployment) map[string]any {
	return map[string]any{
		"id": deployment.ID, "phase": deployment.Phase, "commit": deployment.Commit,
		"github_url": deployment.GitHubURL, "image_digest": deployment.ImageDigest,
		"fetch_task_id": deployment.FetchTaskID, "deploy_task_id": deployment.DeployTaskID,
		"release_id": deployment.ReleaseID, "error_code": deployment.ErrorCode,
		"created_at": deployment.CreatedAt, "updated_at": deployment.UpdatedAt,
	}
}

func gitPageFlags(command *cobra.Command, limit *int, cursor *string) {
	command.Flags().IntVar(limit, "limit", 0, "maximum number of results")
	command.Flags().StringVar(cursor, "cursor", "", "cursor from the previous results page")
}

func gitMutationFlags(command *cobra.Command, ifMatch, key *string) {
	if ifMatch != nil {
		command.Flags().StringVar(ifMatch, "if-match", "", "expected quoted ETag (defaults to the current revision)")
	}
	command.Flags().StringVar(key, "idempotency-key", "", "reuse the same key to retry this request")
}

func renderGitItem(cmd *cobra.Command, item any, fields map[string]any) error {
	headers, values := fieldsOfVia(fields)
	return fromContext(cmd).Out.RenderOne(headers, values, item)
}

func renderGitPage[T any](
	cmd *cobra.Command,
	page apiTypes.Page[T],
	fields func(T) map[string]any,
	columns ...string,
) error {
	headers := make([]string, len(columns))
	for index, key := range columns {
		headers[index] = strings.ToUpper(strings.ReplaceAll(key, "_", " "))
	}
	rows := make([][]string, len(page.Items))
	for index, item := range page.Items {
		row := make([]string, len(columns))
		values := fields(item)
		for column, key := range columns {
			if value, ok := values[key]; ok {
				row[column] = fmt.Sprint(value)
			}
		}
		rows[index] = row
	}
	return fromContext(cmd).Out.Render(headers, rows, page)
}
