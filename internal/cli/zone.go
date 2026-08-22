package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// zone: list | show | add | remove. See mvp.md, "Network Zone" —
// Groundplane's domain term for a Compose network (blueprint.md).
func newZoneCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "zone", Short: "Network zones — named internal docker networks"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List zones",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			environmentID, err := resolveEnvironmentTarget(cmd, fromContext(cmd).Scope.Environment)
			if err != nil {
				return err
			}
			page, err := fromContext(cmd).Client.ListZones(cmd.Context(), environmentID, 0, "")
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(page.Items))
			for index, zone := range page.Items {
				items[index] = zoneFields(zone)
			}
			headers, rows := tabulateVia(fromContext(cmd), items)
			return fromContext(cmd).Out.Render(headers, rows, page)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <name>",
		Short: "Show a zone",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveZoneTarget(cmd, args[0])
			if err != nil {
				return err
			}
			zone, err := fromContext(cmd).Client.GetZone(cmd.Context(), id)
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(zoneFields(zone))
			return fromContext(cmd).Out.RenderOne(fields, values, zone)
		},
	})

	var subnet string
	var internal bool
	add := &cobra.Command{
		Use:   "add <name>",
		Short: "Add a zone",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			environmentID, err := resolveEnvironmentTarget(cmd, fromContext(cmd).Scope.Environment)
			if err != nil {
				return err
			}
			zone, err := fromContext(cmd).Client.CreateZone(cmd.Context(), apiTypes.ZoneCreate{
				EnvironmentID: environmentID, Name: args[0], Subnet: subnet, Internal: internal,
			})
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(zoneFields(zone))
			return fromContext(cmd).Out.RenderOne(fields, values, zone)
		},
	}
	add.Flags().StringVar(&subnet, "subnet", "", "IPv4 CIDR reserved inside the Environment pool")
	_ = add.MarkFlagRequired("subnet")
	add.Flags().BoolVar(&internal, "internal", false, "no route to the host's default gateway")
	cmd.AddCommand(add)

	cmd.AddCommand(&cobra.Command{
		Use:     "remove <name>",
		Aliases: []string{"delete"},
		Short:   "Remove a zone (strips it from every service's membership — see mvp.md's zone-removal warning; dispatches a task)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveZoneTarget(cmd, args[0])
			if err != nil {
				return err
			}
			return runDestroy(cmd, "/api/v1/zones/"+id)
		},
	})

	return cmd
}

func resolveZoneTarget(cmd *cobra.Command, argument string) (string, error) {
	app := fromContext(cmd)
	if app.Scope.AsID {
		return target(app, argument), nil
	}
	environmentID, err := resolveEnvironmentTarget(cmd, app.Scope.Environment)
	if err != nil {
		return "", err
	}
	cursor := ""
	for {
		page, err := app.Client.ListZones(cmd.Context(), environmentID, 200, cursor)
		if err != nil {
			return "", err
		}
		for _, zone := range page.Items {
			if zone.Name == argument {
				return target(app, zone.ID), nil
			}
		}
		if page.NextCursor == "" {
			return "", errs.Newf(errs.KindZoneNotFound, "zone name %q was not found", argument)
		}
		cursor = page.NextCursor
	}
}

func zoneFields(zone apiTypes.Zone) map[string]any {
	return map[string]any{
		"id": zone.ID, "environment_id": zone.EnvironmentID, "name": zone.Name,
		"subnet": zone.Subnet, "internal": zone.Internal,
		"owner_kind": zone.OwnerKind, "owner_id": zone.OwnerID,
	}
}
