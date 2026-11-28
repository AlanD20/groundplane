package cli

import (
	"strings"

	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newGitHubConnectionCmd() *cobra.Command {
	var scopeKind string
	command := &cobra.Command{Use: "github-connection", Short: "Manage scoped GitHub App connections"}
	command.PersistentFlags().
		StringVar(&scopeKind, "scope", "", "tenant, project, or environment (defaults to the most specific configured scope)")
	command.AddCommand(
		newGitHubConnectionAddCmd(&scopeKind),
		newGitHubConnectionListCmd(&scopeKind),
		newGitHubConnectionShowCmd(&scopeKind),
		newGitHubConnectionRevokeCmd(&scopeKind),
		newGitHubConnectionRotateCmd(&scopeKind),
		newGitHubConnectionRepositoriesCmd(&scopeKind),
		newGitHubRepositoryRunnerDefaultCmd(&scopeKind),
		newGitHubRepositorySetRunnerDefaultCmd(&scopeKind),
		newGitHubRepositoryRemoveRunnerDefaultCmd(&scopeKind),
		newGitHubRepositoryWorkflowsCmd(&scopeKind),
	)
	return command
}

func newGitHubConnectionAddCmd(scopeKind *string) *cobra.Command {
	var appID, installationID int64
	var credentialFiles githubCredentialFiles
	var key string
	command := &cobra.Command{
		Use: "add <name>", Short: "Connect an installed GitHub App to a Groundplane scope", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			scope, err := resolveGitHubConnectionScope(cmd, *scopeKind)
			if err != nil {
				return err
			}
			credentials, err := credentialFiles.read(cmd)
			if err != nil {
				return err
			}
			created, err := fromContext(
				cmd,
			).Client.CreateGitHubConnection(
				cmd.Context(),
				api.GitHubConnectionCreateRequest{
					Scope: scope, Name: args[0], AppID: appID, InstallationID: installationID,
					GitHubConnectionCredentialsWrite: credentials,
				},
				key,
			)
			if err != nil {
				return err
			}
			return renderGitItem(cmd, created, githubConnectionFields(created))
		},
	}
	command.Flags().Int64Var(&appID, "app-id", 0, "GitHub App ID")
	command.Flags().Int64Var(&installationID, "installation-id", 0, "GitHub App installation ID")
	_ = command.MarkFlagRequired("app-id")
	_ = command.MarkFlagRequired("installation-id")
	credentialFiles.addFlags(command)
	gitMutationFlags(command, nil, &key)
	return command
}

func newGitHubConnectionListCmd(scopeKind *string) *cobra.Command {
	var limit int
	var cursor string
	command := &cobra.Command{
		Use: "list", Short: "List GitHub connections in a scope", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			scope, err := resolveGitHubConnectionScope(cmd, *scopeKind)
			if err != nil {
				return err
			}
			page, err := fromContext(cmd).Client.ListGitHubConnections(cmd.Context(), scope, limit, cursor)
			if err != nil {
				return err
			}
			return renderGitPage(cmd, page, githubConnectionFields, "name", "id", "installation_id", "status")
		},
	}
	gitPageFlags(command, &limit, &cursor)
	return command
}

func newGitHubConnectionShowCmd(scopeKind *string) *cobra.Command {
	return &cobra.Command{
		Use: "show <connection>", Short: "Show GitHub connection status", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			scope, id, err := resolveGitHubConnection(cmd, *scopeKind, args[0])
			if err != nil {
				return err
			}
			connection, etag, err := fromContext(cmd).Client.ShowGitHubConnection(cmd.Context(), scope, id)
			if err != nil {
				return err
			}
			fields := githubConnectionFields(connection)
			fields["etag"] = etag
			item := struct {
				api.GitHubConnection `yaml:",inline"`
				ETag                 string `json:"etag" yaml:"etag"`
			}{GitHubConnection: connection, ETag: etag}
			return renderGitItem(cmd, item, fields)
		},
	}
}

func newGitHubConnectionRevokeCmd(scopeKind *string) *cobra.Command {
	var ifMatch, key string
	command := &cobra.Command{
		Use: "revoke <connection>", Short: "Revoke a Groundplane GitHub connection", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			scope, id, err := resolveGitHubConnection(cmd, *scopeKind, args[0])
			if err != nil {
				return err
			}
			err = fromContext(cmd).Client.RevokeGitHubConnection(cmd.Context(), scope, id, ifMatch, key)
			if err != nil {
				return err
			}
			return renderGitItem(
				cmd,
				map[string]string{"id": id, "status": "revoked"},
				map[string]any{"id": id, "status": "revoked"},
			)
		},
	}
	gitMutationFlags(command, &ifMatch, &key)
	return command
}

func newGitHubConnectionRotateCmd(scopeKind *string) *cobra.Command {
	var credentialFiles githubCredentialFiles
	var ifMatch, key string
	command := &cobra.Command{
		Use: "rotate <connection>", Short: "Replace the GitHub App credentials stored by Groundplane", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			scope, id, err := resolveGitHubConnection(cmd, *scopeKind, args[0])
			if err != nil {
				return err
			}
			credentials, err := credentialFiles.read(cmd)
			if err != nil {
				return err
			}
			connection, err := fromContext(
				cmd,
			).Client.RotateGitHubConnection(
				cmd.Context(),
				scope,
				id,
				credentials,
				ifMatch,
				key,
			)
			if err != nil {
				return err
			}
			return renderGitItem(cmd, connection, githubConnectionFields(connection))
		},
	}
	credentialFiles.addFlags(command)
	gitMutationFlags(command, &ifMatch, &key)
	return command
}

func newGitHubConnectionRepositoriesCmd(scopeKind *string) *cobra.Command {
	var limit int
	var cursor string
	command := &cobra.Command{
		Use: "repositories <connection>", Short: "List repositories available to the GitHub installation", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			scope, id, err := resolveGitHubConnection(cmd, *scopeKind, args[0])
			if err != nil {
				return err
			}
			page, err := fromContext(cmd).Client.ListGitHubRepositories(cmd.Context(), scope, id, limit, cursor)
			if err != nil {
				return err
			}
			return renderGitPage(cmd, page, func(repository api.GitHubRepository) map[string]any {
				return map[string]any{
					"id": repository.ID, "repository": repository.FullName,
					"default_branch": repository.DefaultBranch, "private": repository.Private,
				}
			}, "repository", "id", "default_branch", "private")
		},
	}
	gitPageFlags(command, &limit, &cursor)
	return command
}

func resolveGitHubConnectionScope(cmd *cobra.Command, kind string) (api.GitHubConnectionScope, error) {
	scope := fromContext(cmd).Scope
	if kind == "" {
		switch {
		case scope.Environment != "":
			kind = "environment"
		case scope.Project != "":
			kind = "project"
		default:
			kind = "tenant"
		}
	}
	var id string
	var err error
	switch kind {
	case "tenant":
		if scope.Tenant == "" {
			return api.GitHubConnectionScope{}, errs.New(errs.KindValidationFailed, "tenant scope requires --tenant")
		}
		id, err = resolveTenantTarget(cmd, scope.Tenant)
	case "project":
		if scope.Project == "" {
			return api.GitHubConnectionScope{}, errs.New(errs.KindValidationFailed, "project scope requires --project")
		}
		id, err = resolveProjectTarget(cmd, scope.Project)
	case "environment":
		if scope.Environment == "" {
			return api.GitHubConnectionScope{}, errs.New(errs.KindValidationFailed, "environment scope requires --env")
		}
		id, err = resolveEnvironmentTarget(cmd, scope.Environment)
	default:
		return api.GitHubConnectionScope{}, errs.New(
			errs.KindValidationFailed,
			"--scope must be tenant, project, or environment",
		)
	}
	return api.GitHubConnectionScope{Kind: kind, ID: id}, err
}

func resolveGitHubConnection(
	cmd *cobra.Command,
	kind string,
	argument string,
) (api.GitHubConnectionScope, string, error) {
	scope, err := resolveGitHubConnectionScope(cmd, kind)
	if err != nil {
		return scope, "", err
	}
	app := fromContext(cmd)
	if app.Scope.AsID {
		return scope, argument, nil
	}

	id, err := findGitHubConnection(cmd, scope, argument)
	if err != nil {
		return scope, "", err
	}
	if id == "" {
		return scope, "", errs.Newf(
			errs.KindRequestNotFound,
			"GitHub connection %q was not found in the selected %s",
			argument,
			scope.Kind,
		)
	}
	return scope, id, nil
}

func findGitHubConnection(cmd *cobra.Command, scope api.GitHubConnectionScope, argument string) (string, error) {
	app := fromContext(cmd)
	var found, cursor string
	for {
		page, err := app.Client.ListGitHubConnections(cmd.Context(), scope, 100, cursor)
		if err != nil {
			return "", err
		}
		for _, connection := range page.Items {
			if connection.Name != argument {
				continue
			}
			if found != "" {
				return "", errs.Newf(
					errs.KindValidationFailed,
					"GitHub connection name %q is ambiguous; use --id",
					argument,
				)
			}
			found = connection.ID
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	return found, nil
}

type githubCredentialFiles struct {
	privateKey    string
	webhookSecret string
}

func (files *githubCredentialFiles) addFlags(command *cobra.Command) {
	command.Flags().
		StringVar(&files.privateKey, "private-key-file", "", "GitHub App PEM private key file (use - for stdin)")
	command.Flags().
		StringVar(&files.webhookSecret, "webhook-secret-file", "", "GitHub App webhook secret file (use - for stdin)")
	_ = command.MarkFlagRequired("private-key-file")
}

func (files githubCredentialFiles) read(cmd *cobra.Command) (api.GitHubConnectionCredentialsWrite, error) {
	if files.privateKey == "-" && files.webhookSecret == "-" {
		return api.GitHubConnectionCredentialsWrite{}, errs.New(
			errs.KindValidationFailed,
			"only one credential can be read from stdin",
		)
	}
	key, err := readValueFile(files.privateKey, cmd.InOrStdin(), 16384, "GitHub App private key")
	if err != nil {
		return api.GitHubConnectionCredentialsWrite{}, err
	}
	credentials := api.GitHubConnectionCredentialsWrite{PrivateKeyPEM: key}
	if files.webhookSecret != "" {
		secret, err := readValueFile(files.webhookSecret, cmd.InOrStdin(), 258, "GitHub App webhook secret")
		if err != nil {
			return api.GitHubConnectionCredentialsWrite{}, err
		}
		credentials.WebhookSecret = strings.TrimSuffix(strings.TrimSuffix(secret, "\n"), "\r")
		if len(credentials.WebhookSecret) < 32 || len(credentials.WebhookSecret) > 256 {
			return api.GitHubConnectionCredentialsWrite{}, errs.New(
				errs.KindValidationFailed,
				"GitHub App webhook secret must be 32 to 256 bytes",
			)
		}
	}
	return credentials, nil
}

func githubConnectionFields(connection api.GitHubConnection) map[string]any {
	fields := map[string]any{
		"id": connection.ID, "name": connection.Name, "scope": connection.Scope.Kind, "scope_id": connection.Scope.ID,
		"app_id": connection.AppID, "installation_id": connection.InstallationID, "generation": connection.Generation,
		"created_at": connection.CreatedAt, "updated_at": connection.UpdatedAt, "status": "active",
	}
	if connection.RevokedAt != nil {
		fields["status"] = "revoked"
		fields["revoked_at"] = *connection.RevokedAt
	}
	return fields
}
