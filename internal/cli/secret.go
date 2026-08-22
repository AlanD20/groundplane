package cli

import (
	"errors"
	"io"
	"os"
	"unicode/utf8"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// secret: list | add | show | remove. Scope = project (plus
// the platform default). See mvp.md, "Secrets" and "Secret kinds".
func newSecretCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "secret", Short: "Project or platform secrets (env variable or file secret)"}
	var platform bool
	cmd.PersistentFlags().BoolVar(&platform, "platform", false, "use platform Secret scope")

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List secrets",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			projectID, platformScope, err := resolveSecretScope(cmd, platform)
			if err != nil {
				return err
			}
			page, err := fromContext(cmd).Client.ListSecrets(cmd.Context(), projectID, platformScope, 0, "")
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

	var kind, filePath, valueFile string
	add := &cobra.Command{
		Use:   "add <name>",
		Short: "Add a secret",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			if valueFile == "" {
				return errs.New(errs.KindValidationFailed, "secret add requires --value-file")
			}
			projectID, platformScope, err := resolveSecretScope(cmd, platform)
			if err != nil {
				return err
			}
			value, err := readSecretValue(valueFile, cmd.InOrStdin())
			if err != nil {
				return err
			}
			secret, err := app.Client.CreateSecret(cmd.Context(), apiTypes.SecretCreateRequest{
				ProjectID: projectID, Platform: platformScope, Key: args[0],
				Kind: kind, Path: filePath, Value: value,
			})
			if err != nil {
				return err
			}
			return renderSecret(cmd, secret)
		},
	}
	add.Flags().StringVar(&kind, "kind", "env_var", "env_var | file")
	add.Flags().StringVar(&filePath, "path", "", "volume-relative path for a file Secret")
	add.Flags().StringVar(&valueFile, "value-file", "", "read the value from PATH, or - for stdin")
	cmd.AddCommand(add)

	cmd.AddCommand(&cobra.Command{
		Use:   "show <key>",
		Short: "Show a secret's metadata (masked)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveSecretTarget(cmd, args[0], platform)
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
			id, err := resolveSecretTarget(cmd, args[0], platform)
			if err != nil {
				return err
			}
			accepted, err := fromContext(cmd).Client.RemoveSecret(cmd.Context(), id)
			if err != nil {
				return err
			}
			return renderTaskAccepted(cmd, accepted)
		},
	})

	return cmd
}

func resolveSecretScope(cmd *cobra.Command, platform bool) (string, bool, error) {
	app := fromContext(cmd)
	if platform {
		if app.Scope.Project != "" {
			return "", false, errs.New(
				errs.KindValidationFailed,
				"secret command cannot combine --platform and --project",
			)
		}
		return "", true, nil
	}
	projectID, err := resolveSecretProjectScope(cmd)
	return projectID, false, err
}

func resolveSecretProjectScope(cmd *cobra.Command) (string, error) {
	app := fromContext(cmd)
	if app.Scope.Project == "" {
		return "", errs.New(errs.KindValidationFailed, "secret command requires --project")
	}
	return resolveProjectTarget(cmd, app.Scope.Project)
}

func resolveSecretTarget(cmd *cobra.Command, argument string, platform bool) (string, error) {
	app := fromContext(cmd)
	if app.Scope.AsID {
		return target(app, argument), nil
	}
	projectID, platformScope, err := resolveSecretScope(cmd, platform)
	if err != nil {
		return "", err
	}
	cursor := ""
	for {
		page, err := app.Client.ListSecrets(cmd.Context(), projectID, platformScope, 200, cursor)
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

func readSecretValue(path string, stdin io.Reader) (string, error) {
	if path == "-" {
		return readBoundedSecretValue(stdin)
	}
	file, err := os.Open(path)
	if err != nil {
		return "", errs.Wrap(errs.KindValidationFailed, err)
	}
	stat, statErr := file.Stat()
	if statErr != nil {
		return "", errs.Wrap(errs.KindInternal, errors.Join(statErr, file.Close()))
	}
	if !stat.Mode().IsRegular() {
		closeErr := file.Close()
		if closeErr != nil {
			return "", errs.Wrap(errs.KindInternal, closeErr)
		}
		return "", errs.New(errs.KindValidationFailed, "secret value file must be a regular file")
	}
	value, readErr := readBoundedSecretValue(file)
	closeErr := file.Close()
	if readErr != nil {
		if closeErr != nil {
			return "", errs.Wrap(errs.KindValidationFailed, errors.Join(readErr, closeErr))
		}
		return "", readErr
	}
	if closeErr != nil {
		return "", errs.Wrap(errs.KindInternal, closeErr)
	}
	return value, nil
}

func readBoundedSecretValue(reader io.Reader) (string, error) {
	if reader == nil {
		return "", errs.New(errs.KindValidationFailed, "secret value input is required")
	}
	value, err := io.ReadAll(io.LimitReader(reader, apiTypes.MaximumSecretValueBytes+1))
	if err != nil {
		clear(value)
		return "", errs.Wrap(errs.KindValidationFailed, err)
	}
	defer clear(value)
	if len(value) > apiTypes.MaximumSecretValueBytes {
		return "", errs.New(errs.KindValidationFailed, "secret value exceeds the 255 KiB limit")
	}
	if !utf8.Valid(value) {
		return "", errs.New(errs.KindValidationFailed, "secret value must be valid UTF-8")
	}
	return string(value), nil
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
