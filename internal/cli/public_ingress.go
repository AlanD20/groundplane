package cli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newPublicIngressCmd() *cobra.Command {
	command := &cobra.Command{Use: "public-ingress", Short: "Review and manage public ingress on a Host"}
	command.AddCommand(newPublicIngressShowCmd(), newPublicIngressPreflightCmd(), newPublicIngressApplyCmd())
	return withExecutionClass(command, executionAPI)
}

func newPublicIngressShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show [host]",
		Short: "Show the Host's public ingress configuration and Route options",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			hostID, err := resolvePublicIngressHost(cmd, args)
			if err != nil {
				return err
			}
			app := fromContext(cmd)
			state, err := app.Client.ShowPublicIngress(cmd.Context(), hostID)
			if err != nil {
				return err
			}
			config := state.Configuration
			routeOptions := make([]string, 0, len(state.Routes))
			for _, route := range state.Routes {
				option := fmt.Sprintf(
					"%s %s%s (%s / %s)",
					route.ID,
					route.Hostname,
					route.Path,
					route.Environment,
					route.Service,
				)
				if route.Unavailable != "" {
					option += " · " + route.Unavailable
				}
				routeOptions = append(routeOptions, option)
			}
			fields, values := fieldsOfVia(map[string]any{
				"host_id": hostID, "enabled": config.Enabled, "state": config.State, "revision": config.Revision,
				"addresses": strings.Join(config.Addresses, ", "), "route_ids": strings.Join(config.RouteIDs, ", "),
				"task_id": config.TaskID, "message": config.Message, "route_options": strings.Join(routeOptions, "\n"),
			})
			return app.Out.RenderOne(fields, values, state)
		},
	}
}

func newPublicIngressPreflightCmd() *cobra.Command {
	var flags publicIngressDraftFlags
	command := &cobra.Command{
		Use:   "preflight [host]",
		Short: "Check a proposed public ingress configuration",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			draft, err := flags.draft()
			if err != nil {
				return err
			}
			hostID, err := resolvePublicIngressHost(cmd, args)
			if err != nil {
				return err
			}
			app := fromContext(cmd)
			revision := strconv.FormatInt(flags.revision, 10)
			if !cmd.Flags().Changed("revision") {
				state, err := app.Client.ShowPublicIngress(cmd.Context(), hostID)
				if err != nil {
					return err
				}
				revision = state.Configuration.Revision
			}
			review, err := app.Client.PreflightPublicIngress(cmd.Context(), hostID, revision, draft)
			if err != nil {
				return err
			}
			checks := make([]string, 0, len(review.Checks))
			for _, check := range review.Checks {
				checks = append(checks, fmt.Sprintf("%s [%s] %s", check.Scope, check.Status, check.Message))
			}
			fields, values := fieldsOfVia(map[string]any{
				"revision": review.Revision, "can_apply": review.CanApply,
				"review_token": review.ReviewToken, "checks": strings.Join(checks, "\n"),
			})
			return app.Out.RenderOne(fields, values, review)
		},
	}
	flags.bind(command, "configuration revision to review (defaults to the current revision)")
	return command
}

func newPublicIngressApplyCmd() *cobra.Command {
	var flags publicIngressDraftFlags
	var token, key string
	command := &cobra.Command{
		Use:   "apply [host]",
		Short: "Submit a Task for the exact public ingress configuration you reviewed",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			draft, err := flags.draft()
			if err != nil {
				return err
			}
			if strings.TrimSpace(token) == "" {
				return errs.New(errs.KindValidationFailed, "apply requires the token returned by preflight")
			}
			hostID, err := resolvePublicIngressHost(cmd, args)
			if err != nil {
				return err
			}
			accepted, err := fromContext(cmd).Client.ApplyPublicIngress(
				cmd.Context(), hostID, strconv.FormatInt(flags.revision, 10),
				apiTypes.PublicIngressApplyRequest{PublicIngressDraft: draft, ReviewToken: token}, key,
			)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	flags.bind(command, "configuration revision returned by preflight")
	command.Flags().StringVar(&token, "review-token", "", "token returned by preflight")
	command.Flags().
		StringVar(&key, "idempotency-key", "", "reuse this key with the same revision and input after an uncertain response")
	_ = command.MarkFlagRequired("review-token")
	_ = command.MarkFlagRequired("revision")
	return command
}

type publicIngressDraftFlags struct {
	addresses []string
	routes    []string
	disabled  bool
	revision  int64
}

func (flags *publicIngressDraftFlags) bind(command *cobra.Command, revisionHelp string) {
	command.Flags().StringSliceVar(&flags.addresses, "address", nil, "explicit Host bind address (repeatable)")
	command.Flags().
		StringSliceVar(&flags.routes, "route-id", nil, "stable ID of a public Route to publish (repeatable)")
	command.Flags().BoolVar(&flags.disabled, "disable", false, "request disabled public ingress")
	command.Flags().Int64Var(&flags.revision, "revision", 0, revisionHelp)
}

func (flags publicIngressDraftFlags) draft() (apiTypes.PublicIngressDraft, error) {
	if flags.revision < 0 {
		return apiTypes.PublicIngressDraft{}, errs.New(
			errs.KindValidationFailed,
			"public ingress revision cannot be negative",
		)
	}
	if !flags.disabled && (len(flags.addresses) == 0 || len(flags.routes) == 0) {
		return apiTypes.PublicIngressDraft{}, errs.New(
			errs.KindValidationFailed,
			"public ingress requires an --address and a --route-id",
		)
	}
	return apiTypes.PublicIngressDraft{
		Enabled:   !flags.disabled,
		Addresses: flags.addresses,
		RouteIDs:  flags.routes,
	}, nil
}

func resolvePublicIngressHost(cmd *cobra.Command, args []string) (string, error) {
	app := fromContext(cmd)
	if len(args) == 1 && app.Scope.AsID {
		if ids.Validate(ids.KindHost, args[0]) != nil {
			return "", errs.New(errs.KindValidationFailed, "public ingress requires a stable Host ID with --id")
		}
		return args[0], nil
	}
	host, err := app.Client.ShowHost(cmd.Context())
	if err != nil {
		return "", err
	}
	if len(args) == 1 && args[0] != host.Hostname {
		return "", errs.Newf(errs.KindRequestNotFound, "Host %q was not found", args[0])
	}
	if ids.Validate(ids.KindHost, host.ID) != nil {
		return "", errs.New(errs.KindRequestUnavailable, "the local Host has no stable ID")
	}
	return host.ID, nil
}
