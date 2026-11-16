package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// backing-service (bs): list | show | create | start | stop | destroy | delete.
// Create accepts only operator decisions. Managed adapters own their runtime
// defaults; custom requires an image and adds no managed runtime defaults.
func newBackingServiceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "backing-service",
		Aliases: []string{"bs"},
		Short:   "Backing services — shared datastores/caches/queues",
	}
	cmd.AddCommand(newContainerTerminalCmd(resolveBackingAdapterServiceTarget))
	cmd.AddCommand(
		&cobra.Command{
			Use:   "adapters",
			Short: "List supported backing adapter families and server versions",
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				catalog, err := fromContext(cmd).Client.BackingAdapters(cmd.Context())
				if err != nil {
					return err
				}
				rows := make([]map[string]any, 0, len(catalog.Items))
				for _, adapter := range catalog.Items {
					versions := []string{}
					for _, version := range adapter.Versions {
						versions = append(versions, version.Version)
					}
					rows = append(
						rows,
						map[string]any{"adapter": adapter.Key, "label": adapter.Label, "versions": versions},
					)
				}
				headers, values := tabulateVia(fromContext(cmd), rows)
				return fromContext(cmd).Out.Render(headers, values, catalog)
			},
		},
	)

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List backing services",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			page, err := fromContext(cmd).Client.ListBackingServices(cmd.Context(), 0, "")
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(page.Items))
			for index, backing := range page.Items {
				items[index] = backingServiceFields(backing)
			}
			headers, rows := tabulateVia(fromContext(cmd), items)
			return fromContext(cmd).Out.Render(headers, rows, page)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <slug>",
		Short: "Show a backing service (consumers, connection info)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			projectID, err := resolveBackingProjectTarget(cmd, args[0])
			if err != nil {
				return err
			}
			backing, err := fromContext(cmd).Client.ShowBackingService(cmd.Context(), projectID)
			if err != nil {
				return err
			}
			return renderBackingService(cmd, backing)
		},
	})

	var adapter, adapterVersion, image, authentication, name, description, networkPool, zoneName, zoneSubnet, hooksFile string
	var zoneInternal bool
	var memory, volumeSlug, mountPath string
	var cpus float64
	var createAfter []string
	create := &cobra.Command{
		Use:   "create <slug>",
		Short: "Create a backing service",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			after, err := validatedTaskAfter(createAfter)
			if err != nil {
				return err
			}
			if adapter == "custom" && image == "" {
				return errs.New(errs.KindValidationFailed, "--image is required for custom")
			}
			if adapter != "custom" && adapterVersion == "" {
				return errs.New(errs.KindValidationFailed, "--adapter-version is required for a managed database")
			}
			if adapter == "valkey" && authentication == "" {
				return errs.New(errs.KindValidationFailed, "--authentication is required for valkey")
			}
			var hooks *apiTypes.BackingHookConfiguration
			var resources *apiTypes.ServiceResources
			if cmd.Flags().Changed("memory") || cmd.Flags().Changed("cpus") {
				resources = &apiTypes.ServiceResources{Mem: memory, CPUs: cpus}
			}
			var customVolume *apiTypes.BackingCustomVolume
			if cmd.Flags().Changed("volume-slug") || cmd.Flags().Changed("mount-path") {
				customVolume = &apiTypes.BackingCustomVolume{Slug: volumeSlug, MountPath: mountPath}
			}
			if cmd.Flags().Changed("hooks-file") {
				if adapter != "custom" {
					return errs.New(errs.KindValidationFailed, "--hooks-file is accepted only for custom")
				}
				var err error
				hooks, err = loadBackingHooks(cmd, hooksFile)
				if err != nil {
					return err
				}
			}
			created, err := fromContext(cmd).Client.CreateBackingService(cmd.Context(), apiTypes.BackingServiceCreate{
				After: after, Slug: args[0], Name: name, Description: description, Adapter: adapter, Image: image,
				AdapterVersion: adapterVersion,
				Authentication: authentication,
				Hooks:          hooks,
				Resources:      resources, CustomVolume: customVolume,
				NetworkPool: networkPool,
				Zone: apiTypes.BackingServiceZoneCreate{
					Name:     zoneName,
					Subnet:   zoneSubnet,
					Internal: zoneInternal,
				},
			})
			if err != nil {
				return err
			}
			return renderTaskAccepted(cmd, created)
		},
	}
	create.Flags().
		StringVar(&adapter, "adapter", "", "adapter family: postgres, valkey, mysql, or custom")
	create.Flags().
		StringVar(&adapterVersion, "adapter-version", "", "supported server version (required for managed databases)")
	create.Flags().
		StringVar(&image, "image", "", "container image; required for Custom, or an upstream image matching the selected database version")
	create.Flags().StringVar(&hooksFile, "hooks-file", "", "custom hook configuration JSON file; - reads stdin")
	create.Flags().StringVar(&memory, "memory", "", "memory limit enforced from the first start, for example 1536m")
	create.Flags().Float64Var(&cpus, "cpus", 0, "CPU limit enforced from the first start")
	create.Flags().
		StringVar(&volumeSlug, "volume-slug", "", "optional initial Custom Volume slug; requires --mount-path")
	create.Flags().StringVar(&mountPath, "mount-path", "", "absolute Custom Volume mount path; requires --volume-slug")
	create.Flags().StringVar(
		&authentication,
		"authentication",
		"",
		"required for Valkey; immutable choice: username_password, password, or none (no default)",
	)
	create.Flags().StringVar(&name, "name", "", "display name")
	create.Flags().StringVar(&description, "description", "", "optional description")
	create.Flags().StringVar(&networkPool, "network-pool", "", "reserved Environment pool, e.g. 10.200.0.0/24")
	create.Flags().StringVar(&zoneName, "zone-name", "", "dedicated backing Zone name")
	create.Flags().StringVar(&zoneSubnet, "zone-subnet", "", "Zone subnet inside the Environment pool")
	create.Flags().BoolVar(&zoneInternal, "zone-internal", false, "disable Zone egress through the host gateway")
	_ = create.MarkFlagRequired("adapter")
	_ = create.MarkFlagRequired("name")
	_ = create.MarkFlagRequired("network-pool")
	_ = create.MarkFlagRequired("zone-name")
	_ = create.MarkFlagRequired("zone-subnet")
	addTaskAfterFlag(create, &createAfter)
	cmd.AddCommand(create)

	for _, action := range []struct {
		name, description string
		kind              backingServiceAction
	}{
		{"start", "Start a backing service", backingServiceActionStart},
		{"stop", "Stop a backing service", backingServiceActionStop},
		{"destroy", "Remove backing-service runtime while retaining durable data", backingServiceActionDestroy},
	} {
		var runAfter []string
		command := &cobra.Command{
			Use:   action.name + " <slug>",
			Short: action.description,
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				after, err := validatedTaskAfter(runAfter)
				if err != nil {
					return err
				}
				return runBackingServiceAction(cmd, args[0], action.kind, after)
			},
		}
		addTaskAfterFlag(command, &runAfter)
		cmd.AddCommand(command)
	}

	var deleteAfter []string
	remove := &cobra.Command{
		Use: "delete <slug>", Short: "Permanently delete an unused Backing Service and its persistent data",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			after, err := validatedTaskAfter(deleteAfter)
			if err != nil {
				return err
			}
			projectID, err := resolveBackingProjectTarget(cmd, args[0])
			if err != nil {
				return err
			}
			accepted, err := fromContext(cmd).Client.RemoveBackingService(cmd.Context(), projectID, after)
			if err != nil {
				return err
			}
			return renderTaskAccepted(cmd, accepted)
		},
	}
	addTaskAfterFlag(remove, &deleteAfter)
	cmd.AddCommand(remove)
	return cmd
}

func resolveBackingProjectTarget(cmd *cobra.Command, argument string) (string, error) {
	app := fromContext(cmd)
	if app.Scope.AsID {
		return target(app, argument), nil
	}
	cursor := ""
	for {
		page, err := app.Client.ListProjects(cmd.Context(), "", "backing", 200, cursor)
		if err != nil {
			return "", err
		}
		for _, project := range page.Items {
			if project.Slug == argument {
				return target(app, project.ID), nil
			}
		}
		if page.NextCursor == "" {
			return "", errs.Newf(errs.KindBackingServiceNotFound, "backing-service slug %q was not found", argument)
		}
		cursor = page.NextCursor
	}
}

func resolveBackingAdapterServiceTarget(cmd *cobra.Command, argument string) (string, error) {
	app := fromContext(cmd)
	if app.Scope.AsID {
		return target(app, argument), nil
	}
	projectID, err := resolveBackingProjectTarget(cmd, argument)
	if err != nil {
		return "", err
	}
	backing, err := app.Client.ShowBackingService(cmd.Context(), projectID)
	if err != nil {
		return "", err
	}
	return target(app, backing.ServiceID), nil
}

type backingServiceAction uint8

const (
	backingServiceActionStart backingServiceAction = iota + 1
	backingServiceActionStop
	backingServiceActionDestroy
)

func runBackingServiceAction(cmd *cobra.Command, argument string, action backingServiceAction, after []string) error {
	projectID, err := resolveBackingProjectTarget(cmd, argument)
	if err != nil {
		return err
	}
	app := fromContext(cmd)
	var accepted apiTypes.TaskAccepted
	switch action {
	case backingServiceActionStart:
		accepted, err = app.Client.StartBackingService(cmd.Context(), projectID, after)
	case backingServiceActionStop:
		accepted, err = app.Client.StopBackingService(cmd.Context(), projectID, after)
	case backingServiceActionDestroy:
		accepted, err = app.Client.DestroyBackingService(cmd.Context(), projectID, after)
	default:
		return errs.New(errs.KindInternal, "Backing-service action is invalid")
	}
	if err != nil {
		return err
	}
	return renderDispatchedTask(cmd, accepted)
}

func renderBackingService(cmd *cobra.Command, backing apiTypes.BackingService) error {
	fields := backingServiceFields(backing)
	headers, values := fieldsOfVia(fields)
	return fromContext(cmd).Out.RenderOne(headers, values, backing)
}

func backingServiceFields(backing apiTypes.BackingService) map[string]any {
	return map[string]any{
		"project_id": backing.ProjectID, "environment_id": backing.EnvironmentID, "service_id": backing.ServiceID,
		"authentication":     backing.Authentication,
		"backing_network_id": backing.BackingNetworkID,
	}
}
