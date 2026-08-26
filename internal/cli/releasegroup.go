package cli

import (
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// release-group: list | show | add | edit | remove | deploy |
// rollback. Coordinates multiple services under one task lock and one
// failure policy — membership is always explicit, never inferred from
// shared image or service names. See blueprint.md, "x-gp-release-groups".
func newReleaseGroupCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "release-group", Short: "Release groups — explicit multi-service deploy coordination"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List release groups",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			environmentID, err := resolveEnvironmentTarget(cmd, fromContext(cmd).Scope.Environment)
			if err != nil {
				return err
			}
			return runList(cmd, "/api/v1/release-groups", map[string]string{"environment_id": environmentID})
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <name>",
		Short: "Show a release group",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			groupID, err := resolveReleaseGroupTarget(cmd, args[0])
			if err != nil {
				return err
			}
			return runShow(cmd, "/api/v1/release-groups/"+groupID)
		},
	})

	var services, order []string
	var onFailure, tag string
	add := &cobra.Command{
		Use:   "add <name>",
		Short: "Add a release group",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			environmentID, err := resolveEnvironmentTarget(cmd, app.Scope.Environment)
			if err != nil {
				return err
			}
			serviceIDs, err := resolveReleaseGroupServices(cmd, services)
			if err != nil {
				return err
			}
			orderIDs, err := resolveReleaseGroupServices(cmd, order)
			if err != nil {
				return err
			}
			policy, err := releaseGroupOnFailure(onFailure)
			if err != nil {
				return err
			}
			body := map[string]interface{}{
				"name": args[0], "service_ids": serviceIDs, "order": orderIDs, "environment_id": environmentID,
				"on_failure": policy,
			}
			if cmd.Flags().Changed("tag") {
				body["tag"] = tag
			}
			return runCreate(cmd, "/api/v1/release-groups", body)
		},
	}
	add.Flags().StringSliceVar(&services, "services", nil, "comma-separated stable member service ids; 2 to 32 required")
	add.Flags().StringSliceVar(&order, "order", nil, "comma-separated stable service ids in deploy order")
	add.Flags().StringVar(&tag, "tag", "", "default image tag for group deploys")
	add.Flags().
		StringVar(&onFailure, "on-failure", "switch_back", "switch_back | leave_active (defaults to switch_back)")
	_ = add.MarkFlagRequired("services")
	cmd.AddCommand(add)

	var editServices, editOrder []string
	var editOnFailure, editName, editTag string
	var clearTag bool
	edit := &cobra.Command{
		Use:   "edit <name>",
		Short: "Edit a release group's membership or order",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !cmd.Flags().Changed("name") && !cmd.Flags().Changed("services") &&
				!cmd.Flags().Changed("order") && !cmd.Flags().Changed("on-failure") &&
				!cmd.Flags().Changed("tag") && !clearTag {
				return errs.New(errs.KindValidationFailed, "release group edit requires at least one changed field")
			}
			groupID, err := resolveReleaseGroupTarget(cmd, args[0])
			if err != nil {
				return err
			}
			body := map[string]interface{}{}
			if cmd.Flags().Changed("name") {
				body["name"] = editName
			}
			if cmd.Flags().Changed("services") {
				resolved, err := resolveReleaseGroupServices(cmd, editServices)
				if err != nil {
					return err
				}
				body["service_ids"] = resolved
			}
			if cmd.Flags().Changed("order") {
				resolved, err := resolveReleaseGroupServices(cmd, editOrder)
				if err != nil {
					return err
				}
				body["order"] = resolved
			}
			if cmd.Flags().Changed("on-failure") {
				policy, err := releaseGroupOnFailure(editOnFailure)
				if err != nil {
					return err
				}
				body["on_failure"] = policy
			}
			if cmd.Flags().Changed("tag") {
				body["tag"] = editTag
			}
			if clearTag {
				body["tag"] = nil
			}
			if len(body) == 0 {
				return errs.New(errs.KindValidationFailed, "release group edit requires at least one changed field")
			}
			return runPatch(cmd, "/api/v1/release-groups/"+groupID, body)
		},
	}
	edit.Flags().StringVar(&editName, "name", "", "new release group name")
	edit.Flags().StringSliceVar(&editServices, "services", nil, "new comma-separated stable member service ids")
	edit.Flags().StringSliceVar(&editOrder, "order", nil, "new comma-separated stable deploy order")
	edit.Flags().StringVar(&editTag, "tag", "", "new default image tag")
	edit.Flags().BoolVar(&clearTag, "clear-tag", false, "clear the default image tag")
	edit.Flags().StringVar(&editOnFailure, "on-failure", "", "new failure policy: switch_back | leave_active")
	edit.MarkFlagsMutuallyExclusive("tag", "clear-tag")
	cmd.AddCommand(edit)

	cmd.AddCommand(&cobra.Command{
		Use:     "remove <name>",
		Aliases: []string{"delete"},
		Short:   "Remove a release group (dispatches a task; member services are untouched)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			groupID, err := resolveReleaseGroupTarget(cmd, args[0])
			if err != nil {
				return err
			}
			return runDestroy(cmd, "/api/v1/release-groups/"+groupID)
		},
	})

	var deployTag string
	deploy := &cobra.Command{
		Use:   "deploy <name>",
		Short: "Deploy every member service under one task lock",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			groupID, err := resolveReleaseGroupTarget(cmd, args[0])
			if err != nil {
				return err
			}
			path := "/api/v1/release-groups/" + groupID + "/deploy"
			body := map[string]string{}
			if strings.TrimSpace(deployTag) != "" {
				body["tag"] = deployTag
			}
			return runAction(cmd, path, body)
		},
	}
	deploy.Flags().StringVar(&deployTag, "tag", "", "immutable image tag applied to every member")
	cmd.AddCommand(deploy)

	cmd.AddCommand(&cobra.Command{
		Use:   "rollback <name>",
		Short: "Roll back every member service under one task lock",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			groupID, err := resolveReleaseGroupTarget(cmd, args[0])
			if err != nil {
				return err
			}
			return runAction(cmd, "/api/v1/release-groups/"+groupID+"/rollback", nil)
		},
	})

	return cmd
}

func resolveReleaseGroupTarget(cmd *cobra.Command, value string) (string, error) {
	app := fromContext(cmd)
	if app.Scope.AsID {
		return target(app, value), nil
	}
	environmentID, err := resolveEnvironmentTarget(cmd, app.Scope.Environment)
	if err != nil {
		return "", err
	}
	for cursor := ""; ; {
		page, err := app.Client.ListReleaseGroups(cmd.Context(), environmentID, 200, cursor)
		if err != nil {
			return "", err
		}
		for _, group := range page.Items {
			if group.Name == value {
				return group.ID, nil
			}
		}
		if page.NextCursor == "" {
			return "", errs.New(errs.KindReleaseGroupNotFound, "release group was not found")
		}
		cursor = page.NextCursor
	}
}

func resolveReleaseGroupServices(cmd *cobra.Command, values []string) ([]string, error) {
	if values == nil {
		return nil, nil
	}
	result := make([]string, len(values))
	for index, value := range values {
		resolved, err := resolveServiceTarget(cmd, value)
		if err != nil {
			return nil, err
		}
		result[index] = resolved
	}
	return result, nil
}

func releaseGroupOnFailure(value string) (string, error) {
	switch value {
	case "switch_back", "leave_active":
		return value, nil
	default:
		return "", errs.Newf(errs.KindValidationFailed,
			"invalid --on-failure %q: expected switch_back or leave_active", value)
	}
}
