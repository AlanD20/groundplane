package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// tenant: list | show <slug> | create | edit | rename | delete <slug>.
// See mvp.md, "Tenant" — a strict isolation boundary.
func newTenantCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "tenant", Short: "Tenants — strict isolation boundaries"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List tenants",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			page, err := app.Client.ListTenants(cmd.Context(), 0, "")
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(page.Items))
			for index, tenant := range page.Items {
				items[index] = tenantFields(tenant)
			}
			headers, rows := tabulateVia(app, items)
			return app.Out.Render(headers, rows, page)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <slug>",
		Short: "Show a tenant",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveTenantTarget(cmd, args[0])
			if err != nil {
				return err
			}
			tenant, err := fromContext(cmd).Client.ShowTenant(cmd.Context(), id)
			if err != nil {
				return err
			}
			return renderTenant(cmd, tenant)
		},
	})

	var name, description string
	create := &cobra.Command{
		Use:   "create <slug>",
		Short: "Create a tenant",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			body := apiTypes.TenantCreate{Slug: args[0]}
			if name != "" {
				body.Name = &name
			}
			if description != "" {
				body.Description = &description
			}
			tenant, err := fromContext(cmd).Client.CreateTenant(cmd.Context(), body)
			if err != nil {
				return err
			}
			return renderTenant(cmd, tenant)
		},
	}
	create.Flags().StringVar(&name, "name", "", "display name (defaults to the slug)")
	create.Flags().StringVar(&description, "description", "", "optional tenant summary")
	cmd.AddCommand(create)

	var editName, editDescription string
	edit := &cobra.Command{
		Use:   "edit <slug>",
		Short: "Edit a tenant's fields (partial update)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveTenantTarget(cmd, args[0])
			if err != nil {
				return err
			}
			body := apiTypes.TenantEdit{}
			if cmd.Flags().Changed("name") {
				body.Name = &editName
			}
			if cmd.Flags().Changed("description") {
				body.Description = &editDescription
			}
			tenant, err := fromContext(cmd).Client.EditTenant(cmd.Context(), id, body)
			if err != nil {
				return err
			}
			return renderTenant(cmd, tenant)
		},
	}
	edit.Flags().StringVar(&editName, "name", "", "new display name")
	edit.Flags().StringVar(&editDescription, "description", "", "new tenant summary (empty clears it)")
	cmd.AddCommand(edit)

	var newSlug string
	rename := &cobra.Command{
		Use:   "rename <slug>",
		Short: "Rename a tenant's slug (id unchanged; a stale slug simply fails to resolve afterward)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveTenantTarget(cmd, args[0])
			if err != nil {
				return err
			}
			tenant, err := fromContext(cmd).Client.RenameTenant(cmd.Context(), id, newSlug)
			if err != nil {
				return err
			}
			return renderTenant(cmd, tenant)
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
			id, err := resolveTenantTarget(cmd, args[0])
			if err != nil {
				return err
			}
			accepted, err := fromContext(cmd).Client.DeleteTenant(cmd.Context(), id)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	})

	return cmd
}

func resolveTenantTarget(cmd *cobra.Command, argument string) (string, error) {
	app := fromContext(cmd)
	if app.Scope.AsID {
		return target(app, argument), nil
	}
	cursor := ""
	for {
		page, err := app.Client.ListTenants(cmd.Context(), 200, cursor)
		if err != nil {
			return "", err
		}
		for _, tenant := range page.Items {
			if tenant.Slug == argument {
				return target(app, tenant.ID), nil
			}
		}
		if page.NextCursor == "" {
			return "", errs.Newf(errs.KindTenantNotFound, "tenant slug %q was not found", argument)
		}
		cursor = page.NextCursor
	}
}

func renderTenant(cmd *cobra.Command, tenant apiTypes.Tenant) error {
	item := tenantFields(tenant)
	fields, values := fieldsOfVia(item)
	return fromContext(cmd).Out.RenderOne(fields, values, tenant)
}

func tenantFields(tenant apiTypes.Tenant) map[string]any {
	return map[string]any{
		"id": tenant.ID, "slug": tenant.Slug, "name": tenant.Name, "description": tenant.Description,
	}
}
