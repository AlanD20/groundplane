package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// project: list | show | create | edit | rename | delete — scoped to a
// tenant (-t/--tenant). See mvp.md, "Project" (tenant vs backing, both
// same hierarchy).
func newProjectCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "project", Short: "Projects — a tenant application or a backing service"}

	var kind string
	list := &cobra.Command{
		Use:   "list",
		Short: "List projects",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			tenantID := ""
			if app.Scope.Tenant != "" {
				resolved, err := resolveTenantTarget(cmd, app.Scope.Tenant)
				if err != nil {
					return err
				}
				tenantID = resolved
			}
			page, err := app.Client.ListProjects(cmd.Context(), tenantID, kind, 0, "")
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(page.Items))
			for index, project := range page.Items {
				items[index] = projectFields(project)
			}
			headers, rows := tabulateVia(app, items)
			return app.Out.Render(headers, rows, page)
		},
	}
	list.Flags().StringVar(&kind, "kind", "", "filter: tenant | backing")
	cmd.AddCommand(list)

	cmd.AddCommand(&cobra.Command{
		Use:   "show <slug>",
		Short: "Show a project",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveProjectTarget(cmd, args[0])
			if err != nil {
				return err
			}
			project, err := fromContext(cmd).Client.ShowProject(cmd.Context(), id)
			if err != nil {
				return err
			}
			return renderProject(cmd, project)
		},
	})

	var name, description string
	create := &cobra.Command{
		Use:   "create <slug>",
		Short: "Create a tenant project (use `backing-service create` for backing projects)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			tenantID, err := resolveTenantTarget(cmd, app.Scope.Tenant)
			if err != nil {
				return err
			}
			body := apiTypes.ProjectCreate{Slug: args[0], TenantID: tenantID}
			if name != "" {
				body.Name = &name
			}
			if description != "" {
				body.Description = &description
			}
			project, err := app.Client.CreateProject(cmd.Context(), body)
			if err != nil {
				return err
			}
			return renderProject(cmd, project)
		},
	}
	create.Flags().StringVar(&name, "name", "", "display name (defaults to the slug)")
	create.Flags().StringVar(&description, "description", "", "optional project summary")
	cmd.AddCommand(create)

	var editName string
	edit := &cobra.Command{
		Use:   "edit <slug>",
		Short: "Edit a project's fields (partial update)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveProjectTarget(cmd, args[0])
			if err != nil {
				return err
			}
			body := apiTypes.ProjectEdit{}
			if cmd.Flags().Changed("name") {
				body.Name = &editName
			}
			project, err := fromContext(cmd).Client.EditProject(cmd.Context(), id, body)
			if err != nil {
				return err
			}
			return renderProject(cmd, project)
		},
	}
	edit.Flags().StringVar(&editName, "name", "", "new display name")
	cmd.AddCommand(edit)

	var newSlug string
	rename := &cobra.Command{
		Use:   "rename <slug>",
		Short: "Rename a project's slug (id unchanged)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveProjectTarget(cmd, args[0])
			if err != nil {
				return err
			}
			project, err := fromContext(cmd).Client.RenameProject(cmd.Context(), id, newSlug)
			if err != nil {
				return err
			}
			return renderProject(cmd, project)
		},
	}
	rename.Flags().StringVar(&newSlug, "slug", "", "new slug (unique within the tenant)")
	_ = rename.MarkFlagRequired("slug")
	cmd.AddCommand(rename)

	cmd.AddCommand(&cobra.Command{
		Use:     "delete <slug>",
		Aliases: []string{"remove"},
		Short:   "Delete a project (destructive — dispatches a task)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveProjectTarget(cmd, args[0])
			if err != nil {
				return err
			}
			accepted, err := fromContext(cmd).Client.DeleteProject(cmd.Context(), id)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	})

	return cmd
}

func resolveProjectTarget(cmd *cobra.Command, argument string) (string, error) {
	app := fromContext(cmd)
	if app.Scope.AsID {
		return target(app, argument), nil
	}
	tenantID, err := resolveTenantTarget(cmd, app.Scope.Tenant)
	if err != nil {
		return "", err
	}
	cursor := ""
	for {
		page, err := app.Client.ListProjects(cmd.Context(), tenantID, "tenant", 200, cursor)
		if err != nil {
			return "", err
		}
		for _, project := range page.Items {
			if project.Slug == argument {
				return target(app, project.ID), nil
			}
		}
		if page.NextCursor == "" {
			return "", errs.Newf(errs.KindProjectNotFound, "project slug %q was not found", argument)
		}
		cursor = page.NextCursor
	}
}

func renderProject(cmd *cobra.Command, project apiTypes.Project) error {
	item := projectFields(project)
	fields, values := fieldsOfVia(item)
	return fromContext(cmd).Out.RenderOne(fields, values, project)
}

func projectFields(project apiTypes.Project) map[string]any {
	return map[string]any{
		"id": project.ID, "tenant_id": project.TenantID, "slug": project.Slug,
		"name": project.Name, "description": project.Description, "kind": project.Kind,
	}
}
