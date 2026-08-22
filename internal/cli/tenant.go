package cli

import (
	"net/http"

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
			return runList(cmd, "/api/v1/tenants", nil)
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
			return runShow(cmd, "/api/v1/tenants/"+id)
		},
	})

	var name, description string
	create := &cobra.Command{
		Use:   "create <slug>",
		Short: "Create a tenant",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			body := map[string]string{"slug": args[0]}
			if name != "" {
				body["name"] = name
			}
			if description != "" {
				body["description"] = description
			}
			return runCreate(cmd, "/api/v1/tenants", body)
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
			return runPatch(
				cmd,
				"/api/v1/tenants/"+id,
				changedStringFields(cmd, map[string]string{
					"name": editName, "description": editDescription,
				}),
			)
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
			return runPostUpdate(
				cmd,
				"/api/v1/tenants/"+id+"/rename",
				map[string]string{"slug": newSlug},
			)
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
			return runDestroy(cmd, "/api/v1/tenants/"+id)
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
		var page apiTypes.Page[apiTypes.Tenant]
		request := app.Client.NewRequest(
			http.MethodGet,
			"/api/v1/tenants",
			map[string]string{"limit": "200", "cursor": cursor},
			nil,
			http.StatusOK,
		)
		if err := app.Client.Do(cmd.Context(), request, &page); err != nil {
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
