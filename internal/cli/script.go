package cli

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// script: list | show | add | edit | run | remove. Context edits replace the
// complete choice; run and removal use the existing Task-backed operations.
func newScriptCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "script", Short: "Per-environment scripts, optionally hooked into deploy/rollback"}

	cmd.AddCommand(&cobra.Command{
		Use: "list", Short: "List scripts", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			environmentID, err := resolveEnvironmentTarget(cmd, fromContext(cmd).Scope.Environment)
			if err != nil {
				return err
			}
			page, err := fromContext(cmd).Client.ListScripts(cmd.Context(), environmentID, 0, "")
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(page.Items))
			for index, script := range page.Items {
				items[index] = scriptFields(script)
			}
			headers, rows := tabulateVia(fromContext(cmd), items)
			return fromContext(cmd).Out.Render(headers, rows, page)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use: "show <slug>", Short: "Show a script", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			scriptID, err := resolveScriptTarget(cmd, args[0])
			if err != nil {
				return err
			}
			script, err := fromContext(cmd).Client.GetScript(cmd.Context(), scriptID)
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(scriptFields(script))
			return fromContext(cmd).Out.RenderOne(fields, values, script)
		},
	})

	var service, body, when, executionFile string
	var order uint16
	add := &cobra.Command{
		Use: "add <slug>", Short: "Add a script", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			environmentID, err := resolveEnvironmentTarget(cmd, app.Scope.Environment)
			if err != nil {
				return err
			}
			serviceID, err := resolveServiceTarget(cmd, service)
			if err != nil {
				return err
			}
			var execution *apiTypes.ScriptExecution
			if cmd.Flags().Changed("execution-file") {
				execution, err = loadScriptExecution(cmd, executionFile)
				if err != nil {
					return err
				}
			}
			script, err := app.Client.CreateScript(cmd.Context(), apiTypes.ScriptCreate{
				EnvironmentID: environmentID, Slug: args[0], ServiceID: serviceID, Body: body, When: when, Order: order,
				Execution: execution,
			})
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(scriptFields(script))
			return app.Out.RenderOne(fields, values, script)
		},
	}
	add.Flags().StringVar(&service, "service", "", "service the script runs against")
	add.Flags().StringVar(&body, "script", "", "one-line or multi-line script body")
	add.Flags().Uint16Var(&order, "order", 0, "hook order within this Service and phase (0-65535; then slug)")
	add.Flags().
		StringVar(&executionFile, "execution-file", "", "complete execution context YAML (64 KiB max; - reads stdin)")
	add.Flags().StringVar(
		&when, "when", "manual",
		"manual | pre-deploy | post-deploy | pre-rollback | post-rollback | on-failure",
	)
	_ = add.MarkFlagRequired("service")
	_ = add.MarkFlagRequired("script")
	cmd.AddCommand(add)

	var editSlug, editBody, editWhen, editExecutionFile string
	var inheritExecution bool
	var editOrder uint16
	edit := &cobra.Command{
		Use: "edit <slug>", Short: "Edit a script", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			input := apiTypes.ScriptEdit{}
			if cmd.Flags().Changed("slug") {
				input.Slug = &editSlug
			}
			if cmd.Flags().Changed("script") {
				input.Body = &editBody
			}
			if cmd.Flags().Changed("when") {
				input.When = &editWhen
			}
			if cmd.Flags().Changed("order") {
				input.Order = &editOrder
			}
			if cmd.Flags().Changed("execution-file") {
				execution, err := loadScriptExecution(cmd, editExecutionFile)
				if err != nil {
					return err
				}
				input.Execution = execution
			} else if inheritExecution {
				input.Execution = &apiTypes.ScriptExecution{Mode: "inherited"}
			}
			if input.Slug == nil && input.Body == nil && input.When == nil && input.Order == nil &&
				input.Execution == nil {
				return errs.New(errs.KindValidationFailed, "Script edit requires a changed field or execution context")
			}
			scriptID, err := resolveScriptTarget(cmd, args[0])
			if err != nil {
				return err
			}
			script, err := fromContext(cmd).Client.EditScript(cmd.Context(), scriptID, input)
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(scriptFields(script))
			return fromContext(cmd).Out.RenderOne(fields, values, script)
		},
	}
	edit.Flags().StringVar(&editSlug, "slug", "", "new Environment-unique Script slug")
	edit.Flags().StringVar(&editBody, "script", "", "new script body")
	edit.Flags().Uint16Var(&editOrder, "order", 0, "new hook order (0-65535; then slug)")
	edit.Flags().
		StringVar(&editExecutionFile, "execution-file", "", "replacement execution context YAML (64 KiB max; - reads stdin)")
	edit.Flags().
		BoolVar(&inheritExecution, "inherit-execution", false, "replace execution context with inherited Service context")
	edit.MarkFlagsMutuallyExclusive("execution-file", "inherit-execution")
	edit.Flags().StringVar(
		&editWhen, "when", "",
		"new hook: manual | pre-deploy | post-deploy | pre-rollback | post-rollback | on-failure",
	)
	cmd.AddCommand(edit)

	cmd.AddCommand(&cobra.Command{
		Use: "run <slug>", Short: "Run a script now", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			scriptID, err := resolveScriptTarget(cmd, args[0])
			if err != nil {
				return err
			}
			return runAction(cmd, "/api/v1/scripts/"+scriptID+"/run", nil)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use: "remove <slug>", Aliases: []string{"delete"},
		Short: "Remove a script (dispatches a task)", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			scriptID, err := resolveScriptTarget(cmd, args[0])
			if err != nil {
				return err
			}
			accepted, err := fromContext(cmd).Client.RemoveScript(cmd.Context(), scriptID)
			if err != nil {
				return err
			}
			return renderTaskAccepted(cmd, accepted)
		},
	})

	return cmd
}

func resolveScriptTarget(cmd *cobra.Command, value string) (string, error) {
	app := fromContext(cmd)
	requested := target(app, value)
	if ids.Validate(ids.KindScript, requested) == nil {
		return requested, nil
	}
	environmentID, err := resolveEnvironmentTarget(cmd, app.Scope.Environment)
	if err != nil {
		return "", err
	}
	for cursor := ""; ; {
		page, listErr := app.Client.ListScripts(cmd.Context(), environmentID, 100, cursor)
		if listErr != nil {
			return "", listErr
		}
		for _, script := range page.Items {
			if script.Slug == value {
				return script.ID, nil
			}
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	return "", errs.New(errs.KindScriptNotFound, "Script was not found")
}

func scriptFields(script apiTypes.Script) map[string]any {
	return map[string]any{
		"id": script.ID, "slug": script.Slug, "service": script.ServiceName,
		"service_id": script.ServiceID, "script": script.Body, "when": script.When, "order": script.Order,
		"execution": script.Execution,
		"origin":    script.Origin, "active_generation": script.ActiveGeneration,
	}
}
