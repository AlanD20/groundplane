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
			id, err := resolveServiceTarget(cmd, args[0])
			if err != nil {
				return err
			}
			service, err := fromContext(cmd).Client.GetService(cmd.Context(), id)
			if err != nil {
				return err
			}
			fields := serviceFields(service.Service)
			fields["native_compose"] = service.NativeCompose
			fieldNames, values := fieldsOfVia(fields)
			return fromContext(cmd).Out.RenderOne(fieldNames, values, service)
		},
	})

	var image, strategy, onFailure, memory, restart string
	var zones, expose []string
	var cpus float64
	var replicas int
	add := &cobra.Command{
		Use:   "add <name>",
		Short: "Add a service to the environment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			environmentID, err := resolveEnvironmentTarget(cmd, app.Scope.Environment)
			if err != nil {
				return err
			}
			created, err := app.Client.CreateService(cmd.Context(), apiTypes.ServiceCreate{
				EnvironmentID: environmentID, Name: args[0], Image: image, Zones: zones,
				Strategy: strategy, OnFailure: apiTypes.OnFailure(onFailure),
				Resources: apiTypes.ServiceResources{Mem: memory, CPUs: cpus}, Expose: expose,
				Restart: restart, Replicas: replicas,
			})
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(serviceFields(created))
			return app.Out.RenderOne(fields, values, created)
		},
	}
	add.Flags().StringVar(&image, "image", "", "container image")
	add.Flags().StringSliceVar(&zones, "zone", nil, "Zone name (repeatable)")
	add.Flags().StringVar(&strategy, "strategy", "recreate", "declared default strategy: blue-green | recreate")
	add.Flags().StringVar(&onFailure, "on-failure", "switch_back", "declared default: switch_back | leave_active")
	add.Flags().StringVar(&memory, "memory", "", "memory limit, for example 512m")
	add.Flags().Float64Var(&cpus, "cpus", 0, "CPU limit, for example 0.5")
	add.Flags().StringSliceVar(&expose, "expose", nil, "internal exposed port (repeatable)")
	add.Flags().StringVar(&restart, "restart", "unless-stopped", "unless-stopped | always | no")
	add.Flags().IntVar(&replicas, "replicas", 1, "desired replica count")
	_ = add.MarkFlagRequired("image")
	cmd.AddCommand(add)

	var editImage, editStrategy, editOnFailure, editMemory, editRestart string
	var editZones, editExpose []string
	var editCPUs float64
	var editReplicas int
	edit := &cobra.Command{
		Use:   "edit <name>",
		Short: "Edit a service (applies on the next deploy)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			id, err := resolveServiceTarget(cmd, args[0])
			if err != nil {
				return err
			}
			current, err := app.Client.GetService(cmd.Context(), id)
			if err != nil {
				return err
			}
			input := apiTypes.ServiceEdit{
				Image:     current.Image,
				Zones:     append([]string(nil), current.Zones...),
				Strategy:  current.Strategy,
				OnFailure: current.OnFailure,
				Resources: current.Resources,
				Expose:    append([]string(nil), current.Expose...),
				Restart:   current.Restart,
				Replicas:  current.Replicas,
			}
			if current.Healthcheck != nil {
				input.Healthcheck = *current.Healthcheck
			}
			if cmd.Flags().Changed("image") {
				input.Image = editImage
			}
			if cmd.Flags().Changed("zone") {
				input.Zones = editZones
			}
			if cmd.Flags().Changed("strategy") {
				input.Strategy = editStrategy
			}
			if cmd.Flags().Changed("on-failure") {
				input.OnFailure = apiTypes.OnFailure(editOnFailure)
			}
			if cmd.Flags().Changed("memory") {
				input.Resources.Mem = editMemory
			}
			if cmd.Flags().Changed("cpus") {
				input.Resources.CPUs = editCPUs
			}
			if cmd.Flags().Changed("expose") {
				input.Expose = editExpose
			}
			if cmd.Flags().Changed("restart") {
				input.Restart = editRestart
			}
			if cmd.Flags().Changed("replicas") {
				input.Replicas = editReplicas
			}
			edited, err := app.Client.EditService(cmd.Context(), id, input)
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(serviceFields(edited))
			return app.Out.RenderOne(fields, values, edited)
		},
	}
	edit.Flags().StringVar(&editImage, "image", "", "container image")
	edit.Flags().StringSliceVar(&editZones, "zone", nil, "replacement Zone names")
	edit.Flags().StringVar(&editStrategy, "strategy", "", "blue-green | recreate")
	edit.Flags().StringVar(&editOnFailure, "on-failure", "", "switch_back | leave_active")
	edit.Flags().StringVar(&editMemory, "memory", "", "memory limit")
	edit.Flags().Float64Var(&editCPUs, "cpus", 0, "CPU limit")
	edit.Flags().StringSliceVar(&editExpose, "expose", nil, "replacement internal exposed ports")
	edit.Flags().StringVar(&editRestart, "restart", "", "unless-stopped | always | no")
	edit.Flags().IntVar(&editReplicas, "replicas", 0, "desired replica count")
	cmd.AddCommand(edit)

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
			serviceID, err := resolveServiceTarget(cmd, args[0])
			if err != nil {
				return err
			}
			accepted, err := fromContext(cmd).Client.DeployService(cmd.Context(), serviceID, apiTypes.DeployRequest{
				Tag: deployTag, Strategy: deployStrategy, OnFailure: apiTypes.OnFailure(deployOnFailure),
			})
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	deploy.Flags().StringVar(&deployTag, "tag", "", "immutable image tag (defaults to the current tag)")
	deploy.Flags().
		StringVar(&deployStrategy, "strategy", "", "blue-green | recreate | rolling (rolling is declared-deferred)")
	deploy.Flags().StringVar(&deployOnFailure, "on-failure", "", "switch_back | leave_active (defaults to the Service declaration)")
	cmd.AddCommand(deploy)

	var rollbackTag string
	rollback := &cobra.Command{
		Use:   "rollback <name>",
		Short: "Roll back a service (defaults to the pre-selected previous tag)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			serviceID, err := resolveServiceTarget(cmd, args[0])
			if err != nil {
				return err
			}
			accepted, err := fromContext(cmd).Client.RollbackService(
				cmd.Context(), serviceID, apiTypes.RollbackRequest{Tag: rollbackTag},
			)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	rollback.Flags().StringVar(&rollbackTag, "tag", "", "explicit tag (overrides the auto-selected previous tag)")
	cmd.AddCommand(rollback)

	cmd.AddCommand(&cobra.Command{
		Use:   "start <name>",
		Short: "Start a service",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			serviceID, err := resolveServiceTarget(cmd, args[0])
			if err != nil {
				return err
			}
			accepted, err := fromContext(cmd).Client.StartService(cmd.Context(), serviceID)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "stop <name>",
		Short: "Stop a service",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			serviceID, err := resolveServiceTarget(cmd, args[0])
			if err != nil {
				return err
			}
			accepted, err := fromContext(cmd).Client.StopService(cmd.Context(), serviceID)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "destroy <name>",
		Short: "Destroy a service (typed confirmation in the Console)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			serviceID, err := resolveServiceTarget(cmd, args[0])
			if err != nil {
				return err
			}
			accepted, err := fromContext(cmd).Client.DestroyService(cmd.Context(), serviceID)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
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
			backingServiceID, err := resolveBackingAdapterServiceTarget(cmd, args[1])
			if err != nil {
				return err
			}
			grantAttachIDs := make([]string, len(grants))
			for index, grant := range grants {
				grantAttachIDs[index], err = resolveAttachTarget(cmd, grant)
				if err != nil {
					return err
				}
			}
			accepted, err := fromContext(cmd).Client.CreateAttach(cmd.Context(), apiTypes.AttachRequest{
				ServiceIDs:       []string{serviceID},
				BackingServiceID: backingServiceID,
				Name:             attachName,
				GrantAttachIDs:   grantAttachIDs,
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
		"id": service.ID, "environment_id": service.EnvironmentID, "name": service.Name, "image": service.Image,
		"runtime_intent": service.RuntimeIntent, "zones": service.Zones,
		"strategy": service.Strategy, "on_failure": service.OnFailure, "replicas": service.Replicas,
		"healthcheck": service.Healthcheck, "resources": service.Resources,
		"expose": service.Expose, "restart": service.Restart,
		"adapter": service.Adapter, "facts_prefix": service.FactsPrefix,
		"backing_network_id": service.BackingNetworkID,
	}
}
