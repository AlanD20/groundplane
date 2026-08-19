package cli

import "github.com/spf13/cobra"

// tenant: list | show <slug> | create | edit | rename | delete <slug>.
// See mvp.md, "Tenant" — a strict isolation boundary.
func newTenantCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "tenant", Short: "Tenants — strict isolation boundaries"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List tenants",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(cmd, "/api/v1/tenants", nil)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <slug>",
		Short: "Show a tenant",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/tenants/"+target(fromContext(cmd), args[0]))
		},
	})

	var name string
	create := &cobra.Command{
		Use:   "create <slug>",
		Short: "Create a tenant",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCreate(cmd, "/api/v1/tenants", map[string]string{"slug": args[0], "name": name})
		},
	}
	create.Flags().StringVar(&name, "name", "", "display name (defaults to the slug)")
	cmd.AddCommand(create)

	var editName string
	edit := &cobra.Command{
		Use:   "edit <slug>",
		Short: "Edit a tenant's fields (partial update)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPatch(cmd, "/api/v1/tenants/"+target(fromContext(cmd), args[0]), map[string]string{"name": editName})
		},
	}
	edit.Flags().StringVar(&editName, "name", "", "new display name")
	cmd.AddCommand(edit)

	var newSlug string
	rename := &cobra.Command{
		Use:   "rename <slug>",
		Short: "Rename a tenant's slug (id unchanged; a stale slug simply fails to resolve afterward)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPatch(cmd, "/api/v1/tenants/"+target(fromContext(cmd), args[0]), map[string]string{"slug": newSlug})
		},
	}
	rename.Flags().StringVar(&newSlug, "slug", "", "new slug (globally unique)")
	_ = rename.MarkFlagRequired("slug")
	cmd.AddCommand(rename)

	cmd.AddCommand(&cobra.Command{
		Use:     "delete <slug>",
		Aliases: []string{"remove"},
		Short:   "Delete a tenant (destructive — dispatches a task)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDestroy(cmd, "/api/v1/tenants/"+target(fromContext(cmd), args[0]))
		},
	})

	return cmd
}
