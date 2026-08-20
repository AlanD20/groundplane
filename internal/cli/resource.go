// resource.go is the shared plumbing every noun file calls into: list /
// show / create / remove / action against the human API. Table/field
// rendering itself lives in internal/cli/common (the locked "common/ =
// error handler + output" split) — this file only shapes the HTTP calls
// and hands the result to that package.
package cli

import (
	"fmt"
	"net/url"

	"github.com/spf13/cobra"

	clicommon "github.com/AlanD20/groundplane/internal/cli/common"
)

func runList(cmd *cobra.Command, path string, query map[string]string) error {
	app := fromContext(cmd)
	var page struct {
		Items      []map[string]any `json:"items" yaml:"items"`
		NextCursor string           `json:"next_cursor,omitempty" yaml:"next_cursor,omitempty"`
	}
	request := app.Client.NewRequest("GET", path, query, nil)
	if err := app.Client.Do(cmd.Context(), request, &page); err != nil {
		return err
	}
	headers, rows := tabulateVia(app, page.Items)
	return app.Out.Render(headers, rows, page)
}

func runShow(cmd *cobra.Command, path string) error {
	app := fromContext(cmd)
	var item map[string]any
	request := app.Client.NewRequest("GET", path, nil, nil)
	if err := app.Client.Do(cmd.Context(), request, &item); err != nil {
		return err
	}
	fields, values := fieldsOfVia(item)
	return app.Out.RenderOne(fields, values, item)
}

// runCreate handles both `create` (scope-creating entities) and `add`
// (everything added inside an existing scope) — same HTTP shape, POST
// path with a body, render the created entity.
func runCreate(cmd *cobra.Command, path string, body any) error {
	app := fromContext(cmd)
	var item map[string]any
	request := app.Client.NewRequest("POST", path, nil, body)
	if err := app.Client.Do(cmd.Context(), request, &item); err != nil {
		return err
	}
	fields, values := fieldsOfVia(item)
	return app.Out.RenderOne(fields, values, item)
}

func runEdit(cmd *cobra.Command, path string, body any) error {
	app := fromContext(cmd)
	var item map[string]any
	request := app.Client.NewRequest("PUT", path, nil, body)
	if err := app.Client.Do(cmd.Context(), request, &item); err != nil {
		return err
	}
	fields, values := fieldsOfVia(item)
	return app.Out.RenderOne(fields, values, item)
}

// runPatch is a partial update — used where api-cli.md's resource map
// lists PATCH alongside PUT (tenant, project, release-group, zone,
// route, volume, entry, script). PUT stays the "replace the whole
// document" verb; PATCH is for single-field changes like `tenant edit`.
func runPatch(cmd *cobra.Command, path string, body any) error {
	app := fromContext(cmd)
	var item map[string]any
	request := app.Client.NewRequest("PATCH", path, nil, body)
	if err := app.Client.Do(cmd.Context(), request, &item); err != nil {
		return err
	}
	fields, values := fieldsOfVia(item)
	return app.Out.RenderOne(fields, values, item)
}

// changedStringFields builds a PATCH document from flags the operator
// explicitly supplied. Unset flags must not clear existing server fields.
func changedStringFields(cmd *cobra.Command, values map[string]string) map[string]string {
	fields := make(map[string]string, len(values))
	for name, value := range values {
		if cmd.Flags().Changed(name) {
			fields[name] = value
		}
	}
	return fields
}

func runRemove(cmd *cobra.Command, path string) error {
	app := fromContext(cmd)
	request := app.Client.NewRequest("DELETE", path, nil, nil)
	if err := app.Client.Do(cmd.Context(), request, nil); err != nil {
		return err
	}
	_, err := fmt.Fprintln(cmd.OutOrStdout(), "removed")
	return err
}

// runDestroy is DELETE for resources whose deletion is destructive and
// therefore a task — tenant, project, environment, zone, route, volume,
// entry, script, release-group, component, backing-service, connector,
// secret, and attach all fall under api-cli.md's resource-map rows
// marked "destructive DELETE …/{id} → 202 {task_id}". `runner` is the
// one CLI noun that stays synchronous (runRemove) — deregistration has
// no filesystem/Compose side effect to reconcile.
func runDestroy(cmd *cobra.Command, path string) error {
	return runActionMethod(cmd, "DELETE", path, nil)
}

// runAction is every Ops verb (deploy, rollback, backup, restore,
// attach, detach, run, abort, …): POST to an action sub-resource, which
// always returns a task id. See api-cli.md, "Actions are POST
// sub-resources on the entity, and always return a task."
func runAction(cmd *cobra.Command, path string, body any) error {
	return runActionMethod(cmd, "POST", path, body)
}

// runActionMethod is runAction for the cases that aren't a POST, e.g.
// DELETE /attaches/{id} also returns a task (detach deprovisions). See
// api-cli.md: "DELETE /attaches/{id} returns 202 {task_id}".
func runActionMethod(cmd *cobra.Command, method, path string, body any) error {
	app := fromContext(cmd)
	var res struct {
		TaskID string `json:"task_id"`
	}
	request := app.Client.NewRequest(method, path, nil, body)
	if err := app.Client.Do(cmd.Context(), request, &res); err != nil {
		return err
	}
	if res.TaskID == "" {
		return fmt.Errorf("cli: action response is missing task_id")
	}
	_, err := fmt.Fprintf(cmd.OutOrStdout(), "task %s dispatched — `groundplane task show %s` to follow\n", res.TaskID, res.TaskID)
	return err
}

func runDeleteAction(cmd *cobra.Command, path string) error {
	app := fromContext(cmd)
	var res struct {
		TaskID string `json:"task_id"`
	}
	request := app.Client.NewRequest("DELETE", path, nil, nil)
	if err := app.Client.Do(cmd.Context(), request, &res); err != nil {
		return err
	}
	if res.TaskID == "" {
		return fmt.Errorf("cli: action response is missing task_id")
	}
	_, err := fmt.Fprintf(cmd.OutOrStdout(), "task %s dispatched — `groundplane task show %s` to follow\n", res.TaskID, res.TaskID)
	return err
}

func runExportKey(cmd *cobra.Command, path string) error {
	app := fromContext(cmd)
	var res struct {
		Value string `json:"value"`
	}
	request := app.Client.NewRequest("POST", path, nil, nil)
	if err := app.Client.Do(cmd.Context(), request, &res); err != nil {
		return err
	}
	_, err := fmt.Fprintln(cmd.OutOrStdout(), res.Value)
	return err
}

// scopeQuery builds the ?tenant=&project=&environment= filter set from
// the resolved Scope — the CLI's flags become the API's query params,
// exactly like the Console sidebar's filters. Only non-empty, requested
// keys are included.
func scopeQuery(app *App, keys ...string) map[string]string {
	q := map[string]string{}
	for _, k := range keys {
		switch k {
		case "tenant":
			q["tenant"] = app.Scope.Tenant
		case "project":
			q["project"] = app.Scope.Project
		case "environment":
			q["environment"] = app.Scope.Environment
		}
	}
	return q
}

// target resolves a positional argument to its API path segment. With
// --id, the raw value is used as-is (it's already an id); otherwise it's
// passed through as a slug for the Controller to resolve within the
// active scope chain (tenant -> project -> environment) — never
// ambiguous, since slugs are scoped-unique.
func target(app *App, arg string) string {
	return url.PathEscape(arg)
}

func tabulateVia(app *App, items []map[string]any) ([]string, [][]string) {
	_ = app // reserved: a future --columns flag would read app.Scope/config here
	return clicommon.Tabulate(items)
}

func fieldsOfVia(item map[string]any) ([]string, []string) {
	return clicommon.FieldsOf(item)
}
