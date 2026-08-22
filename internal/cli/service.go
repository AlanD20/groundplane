package cli

import (
	"fmt"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// service (svc): list | show | add | edit | remove | deploy | rollback |
// start | stop | destroy | logs | attach | detach. See mvp.md, "Service"
// and "Deploy"/"Rollback", blueprint.md's "x-gp-release" for on_failure,
// and api-cli.md's resource map.
func newServiceCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "service", Aliases: []string{"svc"}, Short: "Services — one container or shared runtime"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List services",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			environmentID, err := resolveEnvironmentTarget(cmd, fromContext(cmd).Scope.Environment)
			if err != nil {
				return err
			}
			page, err := fromContext(cmd).Client.ListServices(cmd.Context(), environmentID, 0, "")
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(page.Items))
			for index, service := range page.Items {
				items[index] = serviceFields(service)
			}
			headers, rows := tabulateVia(fromContext(cmd), items)
			return fromContext(cmd).Out.Render(headers, rows, page)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <name>",
		Short: "Show a service (includes its release ledger — the rollback source)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/services/"+target(fromContext(cmd), args[0]))
		},
	})

	var image, strategy, onFailure string
	add := &cobra.Command{
		Use:   "add <name>",
		Short: "Add a service to the environment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runCreate(cmd, "/api/v1/services", map[string]string{
				"name":        args[0],
				"image":       image,
				"strategy":    strategy,
				"on_failure":  onFailure,
				"environment": app.Scope.Environment,
			})
		},
	}
	add.Flags().StringVar(&image, "image", "", "container image")
	add.Flags().StringVar(&strategy, "strategy", "", "declared default strategy: blue-green | recreate")
	add.Flags().
		StringVar(&onFailure, "on-failure", "", "declared default: switch-back | leave-active (defaults to switch-back)")
	cmd.AddCommand(add)

	cmd.AddCommand(&cobra.Command{
		Use:   "edit <name>",
		Short: "Edit a service (applies on the next deploy)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPatch(cmd, "/api/v1/services/"+target(fromContext(cmd), args[0]), nil)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:     "remove <name>",
		Aliases: []string{"delete"},
		Short:   "Remove a service (destructive — dispatches a task)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDestroy(cmd, "/api/v1/services/"+target(fromContext(cmd), args[0]))
		},
	})

	var deployTag, deployStrategy, deployOnFailure string
	deploy := &cobra.Command{
		Use:   "deploy <name>",
		Short: "Deploy a service (tag defaults to the current tag — the redeploy case)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := "/api/v1/services/" + target(fromContext(cmd), args[0]) + "/deploy"
			return runAction(
				cmd,
				path,
				map[string]string{"tag": deployTag, "strategy": deployStrategy, "on_failure": deployOnFailure},
			)
		},
	}
	deploy.Flags().StringVar(&deployTag, "tag", "", "immutable image tag (defaults to the current tag)")
	deploy.Flags().
		StringVar(&deployStrategy, "strategy", "", "blue-green | recreate | rolling (rolling is declared-deferred)")
	deploy.Flags().StringVar(&deployOnFailure, "on-failure", "", "switch-back | leave-active (defaults to switch-back)")
	cmd.AddCommand(deploy)

	var rollbackTag string
	rollback := &cobra.Command{
		Use:   "rollback <name>",
		Short: "Roll back a service (defaults to the pre-selected previous tag)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := "/api/v1/services/" + target(fromContext(cmd), args[0]) + "/rollback"
			return runAction(cmd, path, map[string]string{"tag": rollbackTag})
		},
	}
	rollback.Flags().StringVar(&rollbackTag, "tag", "", "explicit tag (overrides the auto-selected previous tag)")
	cmd.AddCommand(rollback)

	cmd.AddCommand(&cobra.Command{
		Use:   "start <name>",
		Short: "Start a service",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(cmd, "/api/v1/services/"+target(fromContext(cmd), args[0])+"/start", nil)
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "stop <name>",
		Short: "Stop a service",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(cmd, "/api/v1/services/"+target(fromContext(cmd), args[0])+"/stop", nil)
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "destroy <name>",
		Short: "Destroy a service (typed confirmation in the Console)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(cmd, "/api/v1/services/"+target(fromContext(cmd), args[0])+"/destroy", nil)
		},
	})

	var follow bool
	logs := &cobra.Command{
		Use:   "logs <name>",
		Short: "Tail a service's logs",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			path := "/api/v1/services/" + target(app, args[0]) + "/logs"
			q := map[string]string{}
			if follow {
				q["follow"] = "true"
			}
			return app.Client.Stream(cmd.Context(), path, q, func(line string) error {
				_, err := fmt.Fprintln(cmd.OutOrStdout(), line)
				return err
			})
		},
	}
	logs.Flags().BoolVarP(&follow, "follow", "f", false, "stream new log lines as they arrive")
	cmd.AddCommand(logs)

	var attachName string
	var grants []string
	attach := &cobra.Command{
		Use:   "attach <name> <backing>",
		Short: "Attach a backing service to this service (provisions its own database + role)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			serviceID, err := resolveServiceTarget(cmd, args[0])
			if err != nil {
				return err
			}
			accepted, err := fromContext(cmd).Client.CreateAttach(cmd.Context(), apiTypes.AttachRequest{
				ServiceIDs:       []string{serviceID},
				BackingServiceID: target(fromContext(cmd), args[1]),
				Name:             attachName,
				GrantAttachIDs:   append([]string(nil), grants...),
			})
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	attach.Flags().
		StringVar(&attachName, "name", "", "attach name (default: Controller-suggested, unique within the environment)")
	attach.Flags().StringSliceVar(&grants, "grant", nil, "other attach id/name(s) this attach's role may also access")
	cmd.AddCommand(attach)

	cmd.AddCommand(&cobra.Command{
		Use:   "detach <attach>",
		Short: "Detach a backing service (revokes grants, drops role, optionally drops database)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveAttachTarget(cmd, args[0])
			if err != nil {
				return err
			}
			accepted, err := fromContext(cmd).Client.DetachAttach(cmd.Context(), id)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	})

	return cmd
}

func resolveServiceTarget(cmd *cobra.Command, argument string) (string, error) {
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
		page, err := app.Client.ListServices(cmd.Context(), environmentID, 200, cursor)
		if err != nil {
			return "", err
		}
		for _, service := range page.Items {
			if service.Name == argument {
				return target(app, service.ID), nil
			}
		}
		if page.NextCursor == "" {
			return "", errs.Newf(errs.KindServiceNotFound, "service name %q was not found", argument)
		}
		cursor = page.NextCursor
	}
}

func serviceFields(service apiTypes.Service) map[string]any {
	return map[string]any{
		"id": service.ID, "name": service.Name, "image": service.Image,
		"runtime_intent": service.RuntimeIntent, "zones": service.Zones,
		"strategy": service.Strategy, "on_failure": service.OnFailure, "replicas": service.Replicas,
		"adapter": service.Adapter, "facts_prefix": service.FactsPrefix,
		"backing_network_id": service.BackingNetworkID,
	}
}
