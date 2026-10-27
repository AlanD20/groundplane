package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
	"strings"
)

func newTaskAlertCmd() *cobra.Command {
	command := &cobra.Command{Use: "task-alert", Short: "Discord notifications for failed Tasks and operator attention"}
	var platform bool
	command.PersistentFlags().BoolVar(&platform, "platform", false, "Manage only Tasks without an Environment scope")
	scope := func(cmd *cobra.Command) (string, error) {
		environment := fromContext(cmd).Scope.Environment
		if platform {
			if environment != "" {
				return "", errs.New(errs.KindValidationFailed, "Use --platform or --env, never both")
			}
			return "platform", nil
		}
		if environment == "" {
			return "", errs.New(errs.KindValidationFailed, "Task alerts require --platform or --env")
		}
		return resolveEnvironmentTarget(cmd, environment)
	}
	command.AddCommand(
		&cobra.Command{
			Use:   "show",
			Short: "Show settings and last delivery, without the webhook credential",
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				owner, err := scope(cmd)
				if err != nil {
					return err
				}
				value, err := fromContext(cmd).Client.ShowTaskAlerts(cmd.Context(), owner)
				if err != nil {
					return err
				}
				return renderTaskAlerts(cmd, value)
			},
		},
	)
	var enabled, clearWebhook bool
	var webhookFile, consoleURL string
	set := &cobra.Command{
		Use:   "set",
		Short: "Save alert settings; omitted fields keep their current values",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			owner, err := scope(cmd)
			if err != nil {
				return err
			}
			app := fromContext(cmd)
			current, err := app.Client.ShowTaskAlerts(cmd.Context(), owner)
			if err != nil {
				return err
			}
			input := apiTypes.TaskAlertSettingsWrite{
				ExpectedRevision: current.Revision,
				Enabled:          current.Enabled,
				ConsoleURL:       current.ConsoleURL,
			}
			if cmd.Flags().Changed("enabled") {
				input.Enabled = enabled
			}
			if cmd.Flags().Changed("console-url") {
				input.ConsoleURL = consoleURL
			}
			if clearWebhook && webhookFile != "" {
				return errs.New(errs.KindValidationFailed, "Use a webhook file or --clear-webhook, never both")
			}
			if clearWebhook {
				empty := ""
				input.WebhookURL = &empty
			}
			if webhookFile != "" {
				value, err := readValueFile(webhookFile, cmd.InOrStdin(), 2048, "Discord webhook URL")
				if err != nil {
					return err
				}
				value = strings.TrimSpace(value)
				if value == "" {
					return errs.New(
						errs.KindValidationFailed,
						"Webhook file is empty; use --clear-webhook to remove it",
					)
				}
				input.WebhookURL = &value
			}
			result, err := app.Client.SetTaskAlerts(cmd.Context(), owner, input)
			if err != nil {
				return err
			}
			return renderTaskAlerts(cmd, result)
		},
	}
	set.Flags().BoolVar(&enabled, "enabled", false, "Enable or disable notifications")
	set.Flags().
		StringVar(&webhookFile, "webhook-url-file", "", "File containing a Discord webhook URL; - reads standard input")
	set.Flags().
		BoolVar(&clearWebhook, "clear-webhook", false, "Remove the saved webhook; disable alerts in the same command")
	set.Flags().StringVar(&consoleURL, "console-url", "", "Console HTTP(S) origin for Task links; empty removes links")
	command.AddCommand(
		set,
		&cobra.Command{Use: "test", Short: "Queue a test notification using the saved destination", Args: cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				owner, err := scope(cmd)
				if err != nil {
					return err
				}
				app := fromContext(cmd)
				current, err := app.Client.ShowTaskAlerts(cmd.Context(), owner)
				if err != nil {
					return err
				}
				result, err := app.Client.TestTaskAlerts(cmd.Context(), owner, current.Revision)
				if err != nil {
					return err
				}
				return app.Out.RenderOne(
					[]string{"queued", "notification_id"},
					[]string{"true", result.NotificationID},
					result,
				)
			}},
	)
	return withExecutionClass(command, executionAPI)
}
func renderTaskAlerts(cmd *cobra.Command, value apiTypes.TaskAlertSettings) error {
	fields := map[string]any{
		"revision":           value.Revision,
		"enabled":            value.Enabled,
		"webhook_configured": value.WebhookConfigured,
		"console_url":        value.ConsoleURL,
		"delivery_state":     value.DeliveryState,
		"last_task_id":       value.LastTaskID,
		"last_attempt_at":    value.LastAttemptAt,
		"last_error":         value.LastError,
	}
	headers, values := fieldsOfVia(fields)
	return fromContext(cmd).Out.RenderOne(headers, values, value)
}
