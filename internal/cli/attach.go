package cli

import (
	"context"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// attach: list | rename | fact. Attach creation and detach remain Service actions
// because the consumer is exactly one Service; this noun owns the flat
// Environment-scoped collection and stable Attach identity operations.
func newAttachCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "attach", Short: "Attaches - per-service access to backing services"}
	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List attaches in the environment",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			environmentID, err := resolveEnvironmentTarget(cmd, fromContext(cmd).Scope.Environment)
			if err != nil {
				return err
			}
			page, err := fromContext(cmd).Client.ListAttaches(cmd.Context(), environmentID, 0, "")
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(page.Items))
			for index, attach := range page.Items {
				items[index] = attachFields(attach)
			}
			headers, rows := tabulateVia(fromContext(cmd), items)
			return fromContext(cmd).Out.Render(headers, rows, page)
		},
	})

	var name string
	rename := &cobra.Command{
		Use:   "rename <attach>",
		Short: "Rename an attach's Environment-scoped spec key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveAttachTarget(cmd, args[0])
			if err != nil {
				return err
			}
			attach, err := fromContext(cmd).Client.RenameAttach(cmd.Context(), id, name)
			if err != nil {
				return err
			}
			return renderAttach(cmd, attach)
		},
	}
	rename.Flags().StringVar(&name, "name", "", "new attach name (unique within the environment)")
	_ = rename.MarkFlagRequired("name")
	cmd.AddCommand(rename)

	var grant string
	fact := &cobra.Command{
		Use:   "fact <attach> <key>",
		Short: "Reveal one ready Attach fact",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveAttachTarget(cmd, args[0])
			if err != nil {
				return err
			}
			grantID := ""
			if grant != "" {
				grantID, err = resolveAttachTarget(cmd, grant)
				if err != nil {
					return err
				}
			}
			value, err := fromContext(cmd).Client.RevealAttachFact(cmd.Context(), id, args[1], grantID)
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(map[string]any{"value": value.Value})
			return fromContext(cmd).Out.RenderOne(fields, values, value)
		},
	}
	fact.Flags().StringVar(&grant, "grant", "", "select another Attach granted to the owning Attach")
	cmd.AddCommand(fact)
	return cmd
}

func resolveAttachTarget(cmd *cobra.Command, argument string) (string, error) {
	app := fromContext(cmd)
	if app.Scope.AsID {
		return target(app, argument), nil
	}
	environmentID, err := resolveEnvironmentTarget(cmd, app.Scope.Environment)
	if err != nil {
		return "", err
	}
	return resolveAttachName(cmd.Context(), app, environmentID, argument)
}

func resolveAttachName(ctx context.Context, app *App, environmentID, name string) (string, error) {
	cursor := ""
	for {
		page, err := app.Client.ListAttaches(ctx, environmentID, 200, cursor)
		if err != nil {
			return "", err
		}
		for _, attach := range page.Items {
			if attach.Name == name {
				return target(app, attach.ID), nil
			}
		}
		if page.NextCursor == "" {
			return "", errs.Newf(errs.KindAttachNotFound, "attach name %q was not found", name)
		}
		cursor = page.NextCursor
	}
}

func renderAttach(cmd *cobra.Command, attach apiTypes.Attach) error {
	fields, values := fieldsOfVia(attachFields(attach))
	return fromContext(cmd).Out.RenderOne(fields, values, attach)
}

func attachFields(attach apiTypes.Attach) map[string]any {
	return map[string]any{
		"id": attach.ID, "name": attach.Name, "service_id": attach.ServiceID, "credential": attach.Credential,
		"backing_service_id": attach.BackingServiceID, "backing_environment_id": attach.BackingEnvironmentID,
		"backing_project_id": attach.BackingProjectID, "backing_network_id": attach.BackingNetworkID,
		"grant_attach_ids": attach.GrantAttachIDs, "fact_sets": attach.FactSets,
		"status": attach.Status,
	}
}
