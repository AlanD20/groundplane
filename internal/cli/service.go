package cli

import (
	"fmt"

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
			return runList(cmd, "/api/v1/services", scopeQuery(fromContext(cmd), "environment"))
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
			return runEdit(cmd, "/api/v1/services/"+target(fromContext(cmd), args[0]), nil)
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
		Use:   "attach <backing>",
		Short: "Attach a backing service to this service (provisions its own database + role)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runCreate(cmd, "/api/v1/attaches", map[string]interface{}{
				"backing_service_id": args[0],
				"name":               attachName, // omitted => Controller-suggested from tenant-project-environment-service, made unique
				"grants":             grants,
				"environment":        app.Scope.Environment,
			})
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
			return runDestroy(cmd, "/api/v1/attaches/"+target(fromContext(cmd), args[0]))
		},
	})

	return cmd
}
