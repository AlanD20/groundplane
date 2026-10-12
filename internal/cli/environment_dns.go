package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newEnvironmentDNSCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "dns", Short: "Environment DNS on the shared CoreDNS resolver"}
	cmd.AddCommand(&cobra.Command{Use: "list", Short: "Show names, targets and resolver activation", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			view, err := readEnvironmentDNS(cmd)
			if err != nil {
				return err
			}
			rows := make([][]string, 0, len(view.Records))
			for _, record := range view.Records {
				target := record.Address
				if record.Service != "" {
					target = record.Service + " / " + record.Zone
				}
				rows = append(rows, []string{record.Label, record.Hostname, target, record.State, record.TaskID})
			}
			for _, withdrawal := range view.Withdrawals {
				rows = append(rows, []string{"", withdrawal.Hostname, "withdrawal requested", view.State, view.TaskID})
			}
			return fromContext(cmd).Out.Render([]string{"LABEL", "HOSTNAME", "TARGET", "STATE", "TASK"}, rows, view)
		}})
	cmd.AddCommand(newEnvironmentDNSWriteCmd(false), newEnvironmentDNSWriteCmd(true))
	cmd.AddCommand(
		&cobra.Command{
			Use:   "remove <label>",
			Short: "Withdraw a DNS record (use --id for a stable record id)",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				view, err := readEnvironmentDNS(cmd)
				if err != nil {
					return err
				}
				record, err := environmentDNSTarget(view, args[0], fromContext(cmd).Scope.AsID)
				if err != nil {
					return err
				}
				err = fromContext(
					cmd,
				).Client.RemoveEnvironmentDNS(
					cmd.Context(),
					view.EnvironmentID,
					record.ID,
					view.Revision,
				)
				if err != nil {
					return err
				}
				return renderDNSSaved(
					cmd,
					apiTypes.EnvironmentDNSSaved{EnvironmentID: view.EnvironmentID, RecordID: record.ID},
				)
			},
		},
	)
	return cmd
}

func newEnvironmentDNSWriteCmd(edit bool) *cobra.Command {
	var label, scope, address, service, zone string
	use, short := "create <label>", "Create an IPv4 or same-Environment Service DNS record"
	if edit {
		use, short = "edit <label>", "Edit DNS naming or target (use --id for a stable record id)"
	}
	cmd := &cobra.Command{Use: use, Short: short, Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			view, err := readEnvironmentDNS(cmd)
			if err != nil {
				return err
			}
			input := apiTypes.EnvironmentDNSWrite{Label: args[0], Address: address, Service: service, Zone: zone}
			recordID := ""
			if edit {
				if !cmd.Flags().Changed("label") && !cmd.Flags().Changed("scope") && !cmd.Flags().Changed("address") &&
					!cmd.Flags().Changed("service") &&
					!cmd.Flags().Changed("zone") {
					return errs.New(errs.KindValidationFailed, "DNS edit requires a label, scope or target change")
				}
				record, err := environmentDNSTarget(view, args[0], fromContext(cmd).Scope.AsID)
				if err != nil {
					return err
				}
				recordID = record.ID
				input = apiTypes.EnvironmentDNSWrite{
					Label:   record.Label,
					Address: record.Address,
					Service: record.Service,
					Zone:    record.Zone,
				}
				if cmd.Flags().Changed("label") {
					input.Label = label
				}
				if cmd.Flags().Changed("address") {
					input.Address, input.Service, input.Zone = address, "", ""
				}
				if cmd.Flags().Changed("service") {
					input.Service, input.Address = service, ""
				}
				if cmd.Flags().Changed("zone") {
					input.Zone = zone
				}
			}
			if cmd.Flags().Changed("scope") {
				input.Scope = &scope
			}
			var result apiTypes.EnvironmentDNSSaved
			if edit {
				result, err = fromContext(
					cmd,
				).Client.EditEnvironmentDNS(
					cmd.Context(),
					view.EnvironmentID,
					recordID,
					view.Revision,
					input,
				)
			} else {
				result, err = fromContext(cmd).Client.CreateEnvironmentDNS(cmd.Context(), view.EnvironmentID, view.Revision, input)
			}
			if err != nil {
				return err
			}
			return renderDNSSaved(cmd, result)
		}}
	if edit {
		cmd.Flags().StringVar(&label, "label", "", "replacement record label")
	}
	cmd.Flags().
		StringVar(&scope, "scope", "", "environment | project | tenant; edit preserves the captured namespace when omitted")
	cmd.Flags().StringVar(&address, "address", "", "explicit IPv4 target")
	cmd.Flags().StringVar(&service, "service", "", "Service name in this Environment; may wait for deployment")
	cmd.Flags().StringVar(&zone, "zone", "", "declared Zone name for a Service target")
	cmd.MarkFlagsMutuallyExclusive("address", "service")
	cmd.MarkFlagsMutuallyExclusive("address", "zone")
	return cmd
}

func readEnvironmentDNS(cmd *cobra.Command) (apiTypes.EnvironmentDNS, error) {
	app := fromContext(cmd)
	id, err := resolveEnvironmentTarget(cmd, app.Scope.Environment)
	if err != nil {
		return apiTypes.EnvironmentDNS{}, err
	}
	return app.Client.GetEnvironmentDNS(cmd.Context(), id)
}

func environmentDNSTarget(
	view apiTypes.EnvironmentDNS,
	value string,
	asID bool,
) (apiTypes.EnvironmentDNSRecord, error) {
	for _, record := range view.Records {
		if asID && record.ID == value || !asID && record.Label == value {
			return record, nil
		}
	}
	return apiTypes.EnvironmentDNSRecord{}, errs.New(
		errs.KindValidationFailed,
		"DNS record was not found in the selected Environment",
	)
}

func renderDNSSaved(cmd *cobra.Command, result apiTypes.EnvironmentDNSSaved) error {
	return fromContext(cmd).Out.RenderOne([]string{"record_id", "status"},
		[]string{result.RecordID, "saved; use environment dns list to check resolver activation"}, result)
}
