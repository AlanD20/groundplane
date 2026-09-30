package cli

import (
	"os"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newEtcdConfigCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "config", Short: "Saved etcd configuration and explicit activation"}
	cmd.AddCommand(
		&cobra.Command{
			Use:   "show",
			Short: "Show saved etcd YAML",
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				document, err := fromContext(cmd).Client.ShowEtcdConfig(cmd.Context())
				if err != nil {
					return err
				}
				return renderEtcdConfig(cmd, document)
			},
		},
	)
	var file string
	set := &cobra.Command{
		Use:   "set",
		Short: "Validate and save etcd YAML without restarting",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			app := fromContext(cmd)
			current, err := app.Client.ShowEtcdConfig(cmd.Context())
			if err != nil {
				return err
			}
			content, err := os.ReadFile(file)
			if err != nil {
				return errs.Wrap(errs.KindValidationFailed, err)
			}
			updated, err := app.Client.SetEtcdConfig(
				cmd.Context(),
				apiTypes.EtcdConfigReplacement{Content: string(content), ExpectedRevision: current.Revision},
			)
			if err != nil {
				return err
			}
			return renderEtcdConfig(cmd, updated)
		},
	}
	set.Flags().StringVar(&file, "file", "", "YAML file to save")
	_ = set.MarkFlagRequired("file")
	cmd.AddCommand(
		set,
		&cobra.Command{
			Use:   "apply",
			Short: "Restart only etcd to activate its saved configuration",
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				app := fromContext(cmd)
				current, err := app.Client.ShowEtcdConfig(cmd.Context())
				if err != nil {
					return err
				}
				accepted, err := app.Client.ApplyEtcdConfig(cmd.Context(), current.Revision)
				if err != nil {
					return err
				}
				return renderDispatchedTask(cmd, accepted)
			},
		},
	)
	return withExecutionClass(cmd, executionAPI)
}

func renderEtcdConfig(cmd *cobra.Command, document apiTypes.EtcdConfigDocument) error {
	app := fromContext(cmd)
	headers, rows := tabulateVia(
		app,
		[]map[string]any{
			{
				"path":           document.Path,
				"revision":       document.Revision,
				"apply_required": document.ApplyRequired,
				"content":        document.Content,
			},
		},
	)
	return app.Out.Render(headers, rows, document)
}
