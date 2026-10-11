package cli

import (
	"strings"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newComponentDNSCheckCmd() *cobra.Command {
	var overrides []string
	cmd := &cobra.Command{
		Use:   "check <id>",
		Short: "Query CoreDNS and its configured upstreams",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			queries := make([]apiTypes.DNSCheckQuery, 0, len(overrides))
			for _, override := range overrides {
				domain, selected, ok := strings.Cut(override, "=")
				name, kind, typed := strings.Cut(selected, ":")
				if !ok || !typed || domain == "" || name == "" || kind != "A" && kind != "NS" {
					return errs.New(
						errs.KindValidationFailed,
						"--query uses domain=name:A or domain=name:NS; repeat for other forwarding domains",
					)
				}
				queries = append(queries, apiTypes.DNSCheckQuery{Domain: domain, Name: name, Type: kind})
			}
			app := fromContext(cmd)
			accepted, err := app.Client.CheckDNS(cmd.Context(), target(app, args[0]), queries)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	cmd.Flags().
		StringArrayVar(&overrides, "query", nil, "override a forwarding domain's query: domain=name:A or domain=name:NS")
	return cmd
}

func newComponentDNSCheckResultCmd() *cobra.Command {
	var taskID string
	cmd := &cobra.Command{
		Use:   "check-result <id>",
		Short: "Show the latest DNS check or a selected Task's per-server answers",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			result, err := app.Client.ShowDNSCheck(cmd.Context(), target(app, args[0]), taskID)
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(
				map[string]any{"task_id": result.TaskID, "phase": result.Phase, "outcome": result.Outcome,
					"failure": result.Failure, "stale": result.Stale, "execution_location": result.ExecutionLocation,
					"configuration": result.Configuration, "answers": result.Answers, "created_at": result.CreatedAt,
					"started_at": result.StartedAt, "finished_at": result.FinishedAt},
			)
			return app.Out.RenderOne(fields, values, result)
		},
	}
	cmd.Flags().StringVar(&taskID, "task", "", "show this DNS check Task instead of the latest retained result")
	return cmd
}
