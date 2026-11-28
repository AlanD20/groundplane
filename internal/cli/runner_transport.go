package cli

import (
	"context"
	"os"

	"github.com/AlanD20/groundplane/internal/cli/apiclient"
	clicommon "github.com/AlanD20/groundplane/internal/cli/common"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func configureScopedRunner(cmd *cobra.Command, flags rootFlags) (bool, error) {
	endpoint, hasEndpoint := os.LookupEnv("GROUNDPLANE_RUNNER_ENDPOINT")
	tokenPath, hasTokenPath := os.LookupEnv("GROUNDPLANE_RUNNER_TOKEN_FILE")
	if !hasEndpoint && !hasTokenPath {
		return false, nil
	}

	if commandExecutionClass(cmd) == executionTool {
		return true, nil
	}

	if endpoint == "" || tokenPath == "" {
		return true, errs.New(
			errs.KindValidationFailed,
			"scoped Runner requires GROUNDPLANE_RUNNER_ENDPOINT and GROUNDPLANE_RUNNER_TOKEN_FILE",
		)
	}
	if cmd.Flags().Changed("host") || cmd.Flags().Changed("config") {
		return true, errs.New(
			errs.KindRequestForbidden,
			"scoped Runner commands use the managed Runner endpoint and credential",
		)
	}
	if err := validateScopedRunnerCommand(cmd, flags); err != nil {
		return true, err
	}

	format, err := clicommon.ParseFormat(flags.output)
	if err != nil {
		return true, err
	}
	client, err := apiclient.NewRunner(endpoint, tokenPath)
	if err != nil {
		return true, err
	}
	app := &App{
		Client: client,
		Out:    clicommon.NewWriter(format, flags.noColor, cmd.OutOrStdout()),
		Scope:  Scope{AsID: flags.asID},
	}
	cmd.SetContext(context.WithValue(cmd.Context(), appKey{}, app))
	return true, nil
}

func validateScopedRunnerCommand(cmd *cobra.Command, flags rootFlags) error {
	if cmd.Parent() != nil {
		switch cmd.Parent().Name() + " " + cmd.Name() {
		case "service show", "service deploy":
			if !flags.asID {
				return errs.New(
					errs.KindValidationFailed,
					"scoped Runner service commands require a stable Service ID with --id",
				)
			}
			return nil
		case "task show":
			if cmd.Flags().Changed("limit") || cmd.Flags().Changed("after") || cmd.Flags().Changed("revision") {
				return errs.New(errs.KindRequestForbidden, "scoped Runner task show exposes the Task status")
			}
			return nil
		case "image fetch":
			return nil
		}
	}
	return errs.New(errs.KindRequestForbidden, "this command is outside the scoped Runner API")
}
