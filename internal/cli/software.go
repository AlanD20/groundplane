package cli

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
	"strconv"
	"strings"
)

func newSoftwareCmd() *cobra.Command {
	command := &cobra.Command{Use: "software", Short: "Prepare and apply Controller and Agent software"}
	var selection, source, ref, prepareKey string
	prepare := &cobra.Command{
		Use:   "prepare",
		Short: "Build a source ref or retain published artifacts without activation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := softwareSelection(selection); err != nil {
				return err
			}
			if source != "source_ref" && source != "release" {
				return errs.New(errs.KindValidationFailed, "source must be source_ref or release")
			}
			accepted, err := fromContext(cmd).Client.PrepareSoftware(cmd.Context(), apiTypes.SoftwarePreparationRequest{
				Selection: apiTypes.SoftwareSelection(
					selection,
				), SourceKind: apiTypes.SoftwareSourceKind(source), Ref: ref,
			}, prepareKey)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	prepare.Flags().StringVar(&selection, "component", "", "controller, agent or both")
	prepare.Flags().StringVar(&source, "source", "", "source_ref or release")
	prepare.Flags().StringVar(&ref, "ref", "", "branch/tag/commit, or component release tag")
	prepare.Flags().
		StringVar(&prepareKey, "idempotency-key", "", "reuse to resolve uncertain acceptance of this exact preparation")
	for _, flag := range []string{"component", "source", "ref"} {
		_ = prepare.MarkFlagRequired(flag)
	}
	var applyKey string
	apply := &cobra.Command{
		Use:   "apply <preparation-task-id>",
		Short: "Apply previously verified outputs; never rebuild",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			accepted, err := fromContext(cmd).Client.ApplySoftware(cmd.Context(), args[0], applyKey)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	apply.Flags().
		StringVar(&applyKey, "idempotency-key", "", "reuse to resolve uncertain acceptance of this exact activation")
	var limit int
	var cursor string
	list := &cobra.Command{Use: "ls", Short: "List newest preparations first", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			app := fromContext(cmd)
			page, err := app.Client.ListSoftwarePreparations(cmd.Context(), limit, cursor)
			if err != nil {
				return err
			}
			rows := make([][]string, 0, len(page.Items))
			for _, item := range page.Items {
				rows = append(
					rows,
					[]string{
						item.TaskID,
						string(item.Selection),
						string(item.SourceKind),
						item.Ref,
						item.Phase,
						item.CreatedAt,
					},
				)
			}
			return app.Out.Render([]string{"TASK", "COMPONENT", "SOURCE", "REF", "PHASE", "CREATED"}, rows, page)
		}}
	list.Flags().IntVar(&limit, "limit", 25, "preparations per page (1–100)")
	list.Flags().StringVar(&cursor, "cursor", "", "next_cursor from the preceding page")
	show := &cobra.Command{
		Use:   "show <preparation-task-id>",
		Short: "Show frozen provenance and verified outputs",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := ids.Validate(ids.KindTask, args[0]); err != nil {
				return err
			}
			item, err := fromContext(cmd).Client.ShowSoftwarePreparation(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			app := fromContext(cmd)
			artifacts := make([]string, 0, len(item.Artifacts))
			for _, artifact := range item.Artifacts {
				artifacts = append(artifacts, artifact.Component+": "+artifact.Reference)
			}
			return app.Out.Render(
				[]string{"TASK", "COMPONENT", "SOURCE", "REF", "PHASE", "ARTIFACTS", "ERROR"},
				[][]string{
					{
						item.TaskID,
						string(item.Selection),
						string(item.SourceKind),
						item.Ref,
						item.Phase,
						strings.Join(artifacts, ", "),
						item.ErrorDetail,
					},
				},
				item,
			)
		},
	}
	activation := &cobra.Command{
		Use:   "activation <activation-task-id>",
		Short: "Show Controller and Agent activation outcomes",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			item, err := fromContext(cmd).Client.ShowSoftwareActivation(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			app := fromContext(cmd)
			return app.Out.Render(
				[]string{
					"TASK",
					"PREPARATION",
					"PHASE",
					"CONTROLLER TASK",
					"CONTROLLER APPLIED",
					"AGENT TASK",
					"AGENT APPLIED",
					"ERROR",
				},
				[][]string{
					{
						item.TaskID,
						item.PreparationTaskID,
						item.Phase,
						item.ControllerTaskID,
						strconv.FormatBool(item.ControllerApplied),
						item.AgentTaskID,
						strconv.FormatBool(item.AgentApplied),
						item.ErrorDetail,
					},
				},
				item,
			)
		},
	}
	var releaseSelection string
	releases := &cobra.Command{Use: "releases", Short: "List recent published component releases", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := softwareSelection(releaseSelection); err != nil {
				return err
			}
			catalog, err := fromContext(
				cmd,
			).Client.SoftwareReleases(
				cmd.Context(),
				apiTypes.SoftwareSelection(releaseSelection),
			)
			if err != nil {
				return err
			}
			app := fromContext(cmd)
			rows := make([][]string, 0, len(catalog.Items))
			for _, item := range catalog.Items {
				rows = append(rows, []string{item.Ref, string(item.Selection), item.Name, item.PublishedAt})
			}
			return app.Out.Render([]string{"REF", "COMPONENT", "NAME", "PUBLISHED"}, rows, catalog)
		}}
	releases.Flags().StringVar(&releaseSelection, "component", "both", "controller, agent or both")
	command.AddCommand(prepare, apply, list, show, activation, releases)
	return withExecutionClass(command, executionAPI)
}

func softwareSelection(value string) error {
	if value != "controller" && value != "agent" && value != "both" {
		return errs.New(errs.KindValidationFailed, "component must be controller, agent or both")
	}
	return nil
}
