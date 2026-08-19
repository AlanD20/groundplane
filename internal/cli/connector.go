package cli

import "github.com/spf13/cobra"

// connector: list | add | show | remove. Environment-scoped OR the
// platform default — NEVER project-scoped (locked). See blueprint.md,
// "Envelope and placement": "There is no project-scoped connector. An
// environment resolves its connector by environment id, then the
// platform default."
func newConnectorCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "connector", Short: "Backup destinations + credentials"}

	var platform bool
	list := &cobra.Command{
		Use:   "list",
		Short: "List connectors",
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			q := scopeQuery(app, "environment")
			if platform {
				q["platform"] = "true"
			}
			return runList(cmd, "/api/v1/connectors", q)
		},
	}
	list.Flags().BoolVar(&platform, "platform", false, "list the platform-default connector instead of an environment's")
	cmd.AddCommand(list)

	var kind, accessKeyRef, secretKeyRef string
	var addPlatform bool
	add := &cobra.Command{
		Use:   "add <name>",
		Short: "Add a connector (scoped to -e/--env, or --platform for the platform default)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			body := map[string]interface{}{
				"name": args[0], "kind": kind,
				"credentials": map[string]string{
					"access_key_ref": accessKeyRef,
					"secret_key_ref": secretKeyRef,
				},
			}
			if addPlatform {
				body["platform"] = true
			} else {
				body["environment"] = app.Scope.Environment
			}
			return runCreate(cmd, "/api/v1/connectors", body)
		},
	}
	add.Flags().StringVar(&kind, "kind", "s3-compatible", "s3-compatible (R2 today; S3/MinIO/B2 later)")
	add.Flags().StringVar(&accessKeyRef, "access-key-ref", "", "secret-store env var name holding the access key (recommended over a direct value)")
	add.Flags().StringVar(&secretKeyRef, "secret-key-ref", "", "secret-store env var name holding the secret key")
	add.Flags().BoolVar(&addPlatform, "platform", false, "register as the platform-default connector instead of scoping to -e/--env")
	cmd.AddCommand(add)

	cmd.AddCommand(&cobra.Command{
		Use:   "show <id>",
		Short: "Show a connector",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/connectors/"+target(fromContext(cmd), args[0]))
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:     "remove <id>",
		Aliases: []string{"delete"},
		Short:   "Remove a connector (dispatches a task)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDestroy(cmd, "/api/v1/connectors/"+target(fromContext(cmd), args[0]))
		},
	})

	return cmd
}
