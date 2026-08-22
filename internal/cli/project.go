package cli

import (
	"net/http"

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
			q := map[string]string{}
			if app.Scope.Tenant != "" {
				tenantID, err := resolveTenantTarget(cmd, app.Scope.Tenant)
				if err != nil {
					return err
				}
				q["tenant"] = tenantID
			}
			if kind != "" {
				q["kind"] = kind
			}
			return runList(cmd, "/api/v1/projects", q)
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
			return runShow(cmd, "/api/v1/projects/"+id)
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
			body := map[string]string{"slug": args[0], "tenant_id": tenantID}
			if name != "" {
				body["name"] = name
			}
			if description != "" {
				body["description"] = description
			}
			return runCreate(cmd, "/api/v1/projects", body)
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
			return runPatch(
				cmd,
				"/api/v1/projects/"+id,
				changedStringFields(cmd, map[string]string{"name": editName}),
			)
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
			return runPostUpdate(
				cmd,
				"/api/v1/projects/"+id+"/rename",
				map[string]string{"slug": newSlug},
			)
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
			return runDestroy(cmd, "/api/v1/projects/"+id)
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
		var page apiTypes.Page[apiTypes.Project]
		request := app.Client.NewRequest(
			http.MethodGet,
			"/api/v1/projects",
			map[string]string{
				"tenant": tenantID, "kind": "tenant", "limit": "200", "cursor": cursor,
			},
			nil,
			http.StatusOK,
		)
		if err := app.Client.Do(cmd.Context(), request, &page); err != nil {
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
