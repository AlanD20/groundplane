package cli

import (
	"errors"
	"os"
	"strconv"

	"github.com/spf13/cobra"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// controller: serve | key show | etcd show. Local admin surface on the
// same host as the Controller — `serve` is the foreground entry point
// (cmd/controller/main.go is the production systemd-run path; this
// subcommand is for local debugging/dev), `key show` and `etcd show`
// read local files/state rather than calling the HTTP API. See mvp.md,
// "The Controller itself is the one systemd unit that never becomes a
// container."
func newControllerCmd(deps Dependencies) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "controller",
		Short: "Controller configuration and same-host administration",
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "serve",
		Short: "Run the Controller in the foreground (see cmd/controller for the systemd-managed entry point)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if deps.RunController == nil {
				return errs.New(errs.KindInternal, "controller runner is not configured")
			}
			return deps.RunController(cmd.Context())
		},
	})

	key := &cobra.Command{Use: "key", Short: "The root-only controller age key"}
	key.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "Show the controller age key's path and fingerprint (never its value)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if deps.InspectControllerKey == nil {
				return errs.New(errs.KindInternal, "controller key inspector is not configured")
			}
			result, err := deps.InspectControllerKey(cmd.Context())
			if err != nil {
				return err
			}
			return fromContext(cmd).Out.Render(
				[]string{"PATH", "FINGERPRINT"},
				[][]string{{result.Path, result.Fingerprint}},
				result,
			)
		},
	})
	cmd.AddCommand(key)

	etcd := &cobra.Command{Use: "etcd", Short: "The bootstrap etcd store"}
	etcd.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "Show etcd endpoint status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if deps.InspectControllerEtcd == nil {
				return errs.New(errs.KindInternal, "controller etcd inspector is not configured")
			}
			results, inspectErr := deps.InspectControllerEtcd(cmd.Context())
			if inspectErr != nil && !errors.Is(inspectErr, errs.New(errs.KindStorageUnavailable, "")) {
				return inspectErr
			}
			if results == nil {
				return inspectErr
			}
			rows := make([][]string, len(results))
			for index, result := range results {
				rows[index] = []string{
					result.Endpoint,
					strconv.FormatBool(result.Healthy),
					result.Version,
					result.MemberID,
					result.LeaderID,
					result.Revision,
					formatOptionalInt64(result.DBSizeBytes),
					formatOptionalInt64(result.LatencyMS),
					result.Error,
				}
			}
			renderErr := fromContext(cmd).Out.Render(
				[]string{
					"ENDPOINT",
					"HEALTH",
					"VERSION",
					"MEMBER",
					"LEADER",
					"REVISION",
					"DB_SIZE",
					"LATENCY",
					"ERROR",
				},
				rows,
				results,
			)
			if renderErr != nil {
				return renderErr
			}
			return inspectErr
		},
	})
	cmd.AddCommand(etcd)

	configCmd := &cobra.Command{Use: "config", Short: "The Controller startup configuration"}
	configCmd.AddCommand(&cobra.Command{
		Use: "show", Short: "Show the exact Controller startup configuration", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			document, err := fromContext(cmd).Client.ShowControllerConfig(cmd.Context())
			if err != nil {
				return err
			}
			return renderControllerConfig(cmd, document)
		},
	})
	var configFile string
	setConfig := &cobra.Command{
		Use: "set", Short: "Validate and replace the Controller startup configuration", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			current, err := app.Client.ShowControllerConfig(cmd.Context())
			if err != nil {
				return err
			}
			content, err := os.ReadFile(configFile)
			if err != nil {
				return errs.Wrap(errs.KindValidationFailed, err)
			}
			updated, err := app.Client.SetControllerConfig(cmd.Context(), apiTypes.ControllerConfigReplacement{
				Content: string(content), ExpectedRevision: current.Revision,
			})
			if err != nil {
				return err
			}
			return renderControllerConfig(cmd, updated)
		},
	}
	setConfig.Flags().StringVar(&configFile, "file", "", "YAML file to publish")
	_ = setConfig.MarkFlagRequired("file")
	configCmd.AddCommand(setConfig)
	cmd.AddCommand(withExecutionClass(configCmd, executionAPI), newControllerUpdateCmd())

	return cmd
}

func renderControllerConfig(cmd *cobra.Command, document apiTypes.ControllerConfigDocument) error {
	fields := map[string]any{
		"path": document.Path, "revision": document.Revision,
		"restart_required": document.RestartRequired, "content": document.Content,
	}
	app := fromContext(cmd)
	headers, rows := tabulateVia(app, []map[string]any{fields})
	return app.Out.Render(headers, rows, document)
}

func formatOptionalInt64(value *int64) string {
	if value == nil {
		return ""
	}
	return strconv.FormatInt(*value, 10)
}
