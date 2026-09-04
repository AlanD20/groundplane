package cli

import (
	"fmt"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

type environmentBlueprintBundleFlags struct {
	directory     string
	root          string
	compose       []string
	interpolation []string
}

func newEnvironmentBlueprintCmd() *cobra.Command {
	blueprint := &cobra.Command{
		Use:   "blueprint",
		Short: "View, validate, and apply an Environment Blueprint",
	}
	show := &cobra.Command{
		Use:   "show <name>",
		Short: "Write the current canonical authoring Blueprint",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("output") {
				return errs.New(
					errs.KindValidationFailed,
					"environment blueprint show does not support global --output",
				)
			}
			id, err := resolveEnvironmentTarget(cmd, args[0])
			if err != nil {
				return err
			}
			document, err := fromContext(cmd).Client.ShowEnvironmentBlueprint(cmd.Context(), id)
			if err != nil {
				return err
			}
			_, err = fmt.Fprint(cmd.OutOrStdout(), document.Document)
			return err
		},
	}
	blueprint.AddCommand(show)

	validateFlags := &environmentBlueprintBundleFlags{}
	validate := &cobra.Command{
		Use:   "validate <name>",
		Short: "Validate a closed Blueprint bundle without changing state",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveEnvironmentTarget(cmd, args[0])
			if err != nil {
				return err
			}
			body, contentType, err := validateFlags.build()
			if err != nil {
				return err
			}
			app := fromContext(cmd)
			current, err := app.Client.ShowEnvironmentBlueprint(cmd.Context(), id)
			if err != nil {
				return err
			}
			validation, err := app.Client.ValidateEnvironmentBlueprint(
				cmd.Context(),
				id,
				current.Revision,
				body,
				contentType,
			)
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(validation.Changes))
			for index, change := range validation.Changes {
				items[index] = map[string]any{
					"resource": change.Resource,
					"key":      change.Key,
					"action":   change.Action,
				}
			}
			headers, rows := tabulateVia(app, items)
			return app.Out.Render(headers, rows, validation)
		},
	}
	validateFlags.bind(validate)
	blueprint.AddCommand(validate)

	applyFlags := &environmentBlueprintBundleFlags{}
	apply := &cobra.Command{
		Use:   "apply <name>",
		Short: "Apply a closed Blueprint bundle against the current revision",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveEnvironmentTarget(cmd, args[0])
			if err != nil {
				return err
			}
			body, contentType, err := applyFlags.build()
			if err != nil {
				return err
			}
			current, err := fromContext(cmd).Client.ShowEnvironmentBlueprint(cmd.Context(), id)
			if err != nil {
				return err
			}
			return runBlueprintApply(cmd, id, current.Revision, body, contentType)
		},
	}
	applyFlags.bind(apply)
	blueprint.AddCommand(apply)
	return blueprint
}

func (flags *environmentBlueprintBundleFlags) bind(command *cobra.Command) {
	command.Flags().StringVar(
		&flags.directory,
		"bundle-dir",
		"",
		"directory containing the closed Blueprint file bundle",
	)
	command.Flags().StringVar(&flags.root, "root", "", "relative path to the root Blueprint document")
	command.Flags().StringArrayVar(&flags.compose, "compose-file", nil, "additional Compose source in layer order")
	command.Flags().StringArrayVar(&flags.interpolation, "var", nil, "non-secret Compose interpolation KEY=VALUE")
	_ = command.MarkFlagRequired("bundle-dir")
	_ = command.MarkFlagRequired("root")
}

func (flags environmentBlueprintBundleFlags) build() ([]byte, string, error) {
	return buildBlueprintMultipart(flags.directory, flags.root, flags.compose, flags.interpolation)
}
