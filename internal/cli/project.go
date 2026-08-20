package cli

import "github.com/spf13/cobra"

// project: list | show | create | edit | rename | delete — scoped to a
// tenant (-t/--tenant). See mvp.md, "Project" (tenant vs backing, both
// same hierarchy).
func newProjectCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "project", Short: "Projects — a tenant application or a backing service"}

	var kind string
	list := &cobra.Command{
		Use:   "list",
		Short: "List projects",
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			q := scopeQuery(app, "tenant")
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
			return runShow(cmd, "/api/v1/projects/"+target(fromContext(cmd), args[0]))
		},
	})

	var name string
	create := &cobra.Command{
		Use:   "create <slug>",
		Short: "Create a tenant project (use `backing-service create` for backing projects)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runCreate(cmd, "/api/v1/projects", map[string]string{
				"slug": args[0], "name": name, "kind": "tenant", "tenant": app.Scope.Tenant,
			})
		},
	}
	create.Flags().StringVar(&name, "name", "", "display name (defaults to the slug)")
	cmd.AddCommand(create)

	var editName string
	edit := &cobra.Command{
		Use:   "edit <slug>",
		Short: "Edit a project's fields (partial update)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPatch(cmd, "/api/v1/projects/"+target(fromContext(cmd), args[0]), changedStringFields(cmd, map[string]string{"name": editName}))
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
			return runPatch(cmd, "/api/v1/projects/"+target(fromContext(cmd), args[0]), map[string]string{"slug": newSlug})
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
			return runDestroy(cmd, "/api/v1/projects/"+target(fromContext(cmd), args[0]))
		},
	})

	return cmd
}
