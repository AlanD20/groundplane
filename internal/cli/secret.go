package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// secret: list | add | show | remove. Scope = project (plus
// the platform default). See mvp.md, "Secrets" and "Secret kinds".
func newSecretCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "secret", Short: "Project-scoped secrets (env variable or file secret)"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List secrets",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			projectID, err := resolveSecretProjectScope(cmd)
			if err != nil {
				return err
			}
			page, err := fromContext(cmd).Client.ListSecrets(cmd.Context(), projectID, false, 0, "")
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(page.Items))
			for index, secret := range page.Items {
				items[index] = secretFields(secret)
			}
			headers, rows := tabulateVia(fromContext(cmd), items)
			return fromContext(cmd).Out.Render(headers, rows, page)
		},
	})

	var kind, ref string
	add := &cobra.Command{
		Use:   "add <name>",
		Short: "Add a secret",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runCreate(cmd, "/api/v1/secrets", map[string]string{
				"name": args[0], "kind": kind, "ref": ref, "project": app.Scope.Project,
			})
		},
	}
	add.Flags().StringVar(&kind, "kind", "env_var", "env_var | file")
	add.Flags().StringVar(&ref, "ref", "", "env file name or file path this secret is materialized to")
	cmd.AddCommand(add)

	cmd.AddCommand(&cobra.Command{
		Use:   "show <key>",
		Short: "Show a secret's metadata (masked)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveSecretTarget(cmd, args[0])
			if err != nil {
				return err
			}
			secret, err := fromContext(cmd).Client.ShowSecret(cmd.Context(), id)
			if err != nil {
				return err
			}
			return renderSecret(cmd, secret)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:     "remove <key>",
		Aliases: []string{"delete"},
		Short:   "Remove a secret",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveSecretTarget(cmd, args[0])
			if err != nil {
				return err
			}
			return runDestroy(cmd, "/api/v1/secrets/"+id)
		},
	})

	return cmd
}

func resolveSecretProjectScope(cmd *cobra.Command) (string, error) {
	app := fromContext(cmd)
	if app.Scope.Project == "" {
		return "", errs.New(errs.KindValidationFailed, "secret command requires --project")
	}
	return resolveProjectTarget(cmd, app.Scope.Project)
}

func resolveSecretTarget(cmd *cobra.Command, argument string) (string, error) {
	app := fromContext(cmd)
	if app.Scope.AsID {
		return target(app, argument), nil
	}
	projectID, err := resolveSecretProjectScope(cmd)
	if err != nil {
		return "", err
	}
	cursor := ""
	for {
		page, err := app.Client.ListSecrets(cmd.Context(), projectID, false, 200, cursor)
		if err != nil {
			return "", err
		}
		for _, secret := range page.Items {
			if secret.Key == argument {
				return target(app, secret.ID), nil
			}
		}
		if page.NextCursor == "" {
			return "", errs.Newf(errs.KindSecretNotFound, "secret key %q was not found", argument)
		}
		cursor = page.NextCursor
	}
}

func renderSecret(cmd *cobra.Command, secret apiTypes.Secret) error {
	fields := secretFields(secret)
	headers, values := fieldsOfVia(fields)
	return fromContext(cmd).Out.RenderOne(headers, values, secret)
}

func secretFields(secret apiTypes.Secret) map[string]any {
	return map[string]any{
		"id": secret.ID, "scope": secret.Scope, "project_id": secret.ProjectID,
		"key": secret.Key, "kind": secret.Kind, "ref": secret.Ref, "updated_at": secret.UpdatedAt,
	}
}
