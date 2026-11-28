// Package publicingressoperator connects operator requests to reviewed Host
// ingress publication and native Task admission.
package publicingressoperator

import (
	"context"
	"net/netip"
	"slices"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/publicingress"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	storage "github.com/AlanD20/groundplane/internal/infra/etcd/publicingress"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const preflightTimeout = 8 * time.Second

// Catalog resolves an enrolled Host and supplies operator-readable Routes from
// the same authority used to capture a candidate. Options may be ineligible.
type Catalog interface {
	PublicIngressRoutes(context.Context, string) ([]api.PublicIngressRouteOption, error)
}

type Operator struct {
	ingress   *publicingress.Service
	catalog   Catalog
	submitter *acceptedaction.Submitter
	replays   *etcd.IdempotencyRepository
	intents   *idempotency.Coordinator
}

func New(
	ingress *publicingress.Service,
	catalog Catalog,
	submitter *acceptedaction.Submitter,
	replays *etcd.IdempotencyRepository,
	intents *idempotency.Coordinator,
) (*Operator, error) {
	if ingress == nil || catalog == nil || submitter == nil || replays == nil || intents == nil {
		return nil, errs.New(
			errs.KindInternal,
			"public ingress operator requires an ingress authority and Task admission",
		)
	}
	return &Operator{ingress: ingress, catalog: catalog, submitter: submitter, replays: replays, intents: intents}, nil
}

func (operator *Operator) ReadPublicIngress(ctx context.Context, hostID string) (api.PublicIngressState, error) {
	routes, err := operator.catalog.PublicIngressRoutes(ctx, hostID)
	if err != nil {
		return api.PublicIngressState{}, err
	}
	state, err := operator.ingress.Read(ctx, hostID)
	if err != nil {
		return api.PublicIngressState{}, err
	}
	return api.PublicIngressState{Configuration: configuration(state), Routes: routes}, nil
}

func (operator *Operator) PreflightPublicIngress(
	ctx context.Context, hostID string, revision int64, draft api.PublicIngressDraft,
) (api.PublicIngressReview, error) {
	ctx, cancel := context.WithTimeout(ctx, preflightTimeout)
	defer cancel()
	desired, err := parseDraft(draft)
	if err != nil {
		return api.PublicIngressReview{}, err
	}
	review, err := operator.ingress.Preflight(ctx, hostID, desired)
	if err != nil {
		return api.PublicIngressReview{}, err
	}
	if review.Revision != revision {
		return api.PublicIngressReview{}, errs.New(
			errs.KindStateConflict,
			"Reload public ingress and review the current revision",
		)
	}
	result := api.PublicIngressReview{
		Revision: strconv.FormatInt(revision, 10), ReviewToken: review.ReviewToken, CanApply: review.ReviewToken != "",
	}
	if !result.CanApply {
		result.Checks = []api.PublicIngressCheck{{
			Scope: "Public listener", Status: "blocked", Message: "Public ingress is disabled. Enable it to review a publication.",
		}}
		return result, nil
	}
	result.Checks = []api.PublicIngressCheck{
		{
			Scope:   "Routes and listeners",
			Status:  "passed",
			Message: "The selected Routes and listener addresses passed authority checks.",
		},
		{
			Scope:   "DNS and HTTPS",
			Status:  "not_checked",
			Message: "Activation verifies trusted TLS for each hostname. Public DNS and firewall rules are managed separately.",
		},
	}
	return result, nil
}

func parseDraft(draft api.PublicIngressDraft) (publicingress.Desired, error) {
	if len(draft.Addresses) > 16 || len(draft.RouteIDs) > 512 {
		return publicingress.Desired{}, errs.New(
			errs.KindValidationFailed,
			"The public ingress selection exceeds its limit",
		)
	}
	desired := publicingress.Desired{
		Enabled: draft.Enabled, Addresses: make([]netip.Addr, 0, len(draft.Addresses)), RouteIDs: slices.Clone(draft.RouteIDs),
	}
	for _, raw := range draft.Addresses {
		address, err := netip.ParseAddr(raw)
		if err != nil || address.Zone() != "" || address.Is4In6() {
			return publicingress.Desired{}, errs.New(
				errs.KindValidationFailed,
				"Enter an exact IP address for each listener",
			)
		}
		desired.Addresses = append(desired.Addresses, address)
	}
	for _, id := range desired.RouteIDs {
		if err := ids.Validate(ids.KindRoute, id); err != nil {
			return publicingress.Desired{}, errs.New(
				errs.KindValidationFailed,
				"Select existing Routes by their stable IDs",
			)
		}
	}
	slices.SortFunc(desired.Addresses, netip.Addr.Compare)
	slices.Sort(desired.RouteIDs)
	return desired, nil
}

func configuration(state storage.State) api.PublicIngressConfiguration {
	config := api.PublicIngressConfiguration{
		Revision: strconv.FormatInt(state.Revision, 10), State: "disabled",
		PublicIngressDraft: api.PublicIngressDraft{Addresses: []string{}, RouteIDs: []string{}},
	}
	selected := state.Applied
	if selected != nil {
		config.State, config.TaskID = "serving", selected.TaskID
	}
	if state.LastFailedTaskID != "" && !followsFailure(state) {
		config.State, config.TaskID = "failed", state.LastFailedTaskID
		if state.Applied != nil {
			config.Message = "Groundplane retained the last verified configuration."
		}
	}
	if state.Pending != nil {
		selected = state.Pending
		config.State, config.TaskID = "pending", selected.TaskID
		config.Message = ""
		if selected.Authorized {
			config.Message = "Follow the Task for verified activation or recovery."
		}
	}
	if selected != nil {
		config.Enabled = selected.Desired.Enabled
		for _, address := range selected.Desired.Addresses {
			config.Addresses = append(config.Addresses, address.String())
		}
		config.RouteIDs = append(config.RouteIDs, selected.Desired.RouteIDs...)
	}
	return config
}

func followsFailure(state storage.State) bool {
	return state.Applied != nil && state.Applied.Revision == state.LastIssued
}
