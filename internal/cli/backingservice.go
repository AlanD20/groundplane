package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// backing-service (bs): list | show | create | start | stop | destroy.
// Create accepts only operator decisions. Managed adapters own their runtime
// defaults; custom requires an image and adds no managed runtime defaults.
func newBackingServiceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "backing-service",
		Aliases: []string{"bs"},
		Short:   "Backing services — shared datastores/caches/queues",
	}
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
	create := &cobra.Command{
		Use:   "create <slug>",
		Short: "Create a backing service",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if adapter == "custom" && image == "" {
				return errs.New(errs.KindValidationFailed, "--image is required for custom")
			}
			if adapter != "custom" && adapterVersion == "" {
				return errs.New(errs.KindValidationFailed, "--adapter-version is required for a managed database")
			}
			if adapter != "custom" && image != "" {
				return errs.New(errs.KindValidationFailed, "--image is accepted only for custom")
			}
			if adapter == "valkey" && authentication == "" {
				return errs.New(errs.KindValidationFailed, "--authentication is required for valkey")
			}
			var hooks *apiTypes.BackingHookConfiguration
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
				Slug: args[0], Name: name, Description: description, Adapter: adapter, Image: image,
				AdapterVersion: adapterVersion,
				Authentication: authentication,
				Hooks:          hooks,
				NetworkPool:    networkPool,
				Zone: apiTypes.BackingServiceZoneCreate{
					Name:     zoneName,
					Subnet:   zoneSubnet,
					Internal: zoneInternal,
				},
			})
			if err != nil {
				return err
			}
			fields := backingServiceFields(created.BackingService)
			fields["task_id"] = created.TaskID
			headers, values := fieldsOfVia(fields)
			return fromContext(cmd).Out.RenderOne(headers, values, created)
		},
	}
	create.Flags().
		StringVar(&adapter, "adapter", "", "adapter family: postgres, valkey, mysql, or custom")
	create.Flags().
		StringVar(&adapterVersion, "adapter-version", "", "supported server version (required for managed databases)")
	create.Flags().StringVar(&image, "image", "", "container image (required only for custom)")
	create.Flags().StringVar(&hooksFile, "hooks-file", "", "custom hook configuration JSON file; - reads stdin")
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
	cmd.AddCommand(create)

	cmd.AddCommand(&cobra.Command{
		Use:   "start <slug>",
		Short: "Start a backing service",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runBackingServiceAction(cmd, args[0], backingServiceActionStart)
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "stop <slug>",
		Short: "Stop a backing service",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runBackingServiceAction(cmd, args[0], backingServiceActionStop)
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "destroy <slug>",
		Short: "Remove backing-service runtime while retaining durable data",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runBackingServiceAction(cmd, args[0], backingServiceActionDestroy)
		},
	})

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

func runBackingServiceAction(cmd *cobra.Command, argument string, action backingServiceAction) error {
	projectID, err := resolveBackingProjectTarget(cmd, argument)
	if err != nil {
		return err
	}
	app := fromContext(cmd)
	var accepted apiTypes.TaskAccepted
	switch action {
	case backingServiceActionStart:
		accepted, err = app.Client.StartBackingService(cmd.Context(), projectID)
	case backingServiceActionStop:
		accepted, err = app.Client.StopBackingService(cmd.Context(), projectID)
	case backingServiceActionDestroy:
		accepted, err = app.Client.DestroyBackingService(cmd.Context(), projectID)
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
