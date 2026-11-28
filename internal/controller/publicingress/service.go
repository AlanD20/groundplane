// Package publicingress captures, reviews, and settles an exact host ingress
// publication. The authenticated Agent owns the live edge effect.
package publicingress

import (
	"context"
	"encoding/json"
	"math"
	"net/netip"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	edge "github.com/AlanD20/groundplane/internal/core/publicingress"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	storage "github.com/AlanD20/groundplane/internal/infra/etcd/publicingress"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Desired = storage.Desired

// Authority captures route membership, exposure, Host and Tunnel ownership,
// listener reachability and current acknowledged release readiness from one
// authoritative view. Guards fence those reads until durable admission.
type Authority interface {
	Capture(context.Context, string, Desired) (Capture, error)
}

type Capture struct {
	Input  edge.Input
	Guards []keyvalue.Condition
}

type Repository interface {
	Read(context.Context, string) (storage.State, error)
	CheckClaims(context.Context, storage.State, storage.State) error
	Replace(context.Context, storage.State, storage.State, []keyvalue.Condition) (storage.State, error)
	PrepareReplace(context.Context, storage.State, storage.State, []keyvalue.Condition) (workqueue.Change, error)
}

type Service struct {
	repository Repository
	authority  Authority
}

func New(repository Repository, authority Authority) *Service {
	return &Service{repository: repository, authority: authority}
}

type Preview struct {
	Revision    int64
	ReviewToken string
	Desired     Desired
}

func (service *Service) Read(ctx context.Context, hostID string) (storage.State, error) {
	return service.repository.Read(ctx, hostID)
}

func (service *Service) Preflight(ctx context.Context, hostID string, desired Desired) (Preview, error) {
	state, err := service.Read(ctx, hostID)
	if err != nil {
		return Preview{}, err
	}
	if err := available(state); err != nil {
		return Preview{}, err
	}
	preview := Preview{Revision: state.Revision}
	if !desired.Enabled && state.Applied == nil {
		preview.Desired = Desired{}
		return preview, nil
	}
	item, _, err := service.capture(ctx, hostID, state.Revision, desired, state.Applied)
	if err != nil {
		return Preview{}, err
	}
	next := state
	next.Pending = &item
	if err := service.repository.CheckClaims(ctx, state, next); err != nil {
		return Preview{}, err
	}
	preview.ReviewToken, preview.Desired = item.ReviewToken, item.Desired
	return preview, nil
}

// PrepareBegin captures a publication and its global hostname claims in
// the same transaction as the accepted Task and exact Idempotency-Key.
func (service *Service) PrepareBegin(
	ctx context.Context, hostID, taskID string, reviewedRevision int64, reviewToken string, desired Desired,
) (storage.Publication, workqueue.Change, error) {
	if ids.Validate(ids.KindTask, taskID) != nil || len(reviewToken) != 64 {
		return storage.Publication{}, workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"public ingress requires an exact reviewed Task",
		)
	}
	state, err := service.Read(ctx, hostID)
	if err != nil {
		return storage.Publication{}, workqueue.Change{}, err
	}
	if prior := existingPublication(state, taskID); prior != nil {
		if prior.ReviewedRevision != reviewedRevision || prior.ReviewToken != reviewToken ||
			!equalDesired(prior.Desired, normalize(desired)) {
			return storage.Publication{}, workqueue.Change{}, conflict(
				"public ingress Task already captured another configuration",
			)
		}
		return *prior, workqueue.Change{}, nil
	}
	if state.LastFailedTaskID == taskID || state.Revision != reviewedRevision {
		return storage.Publication{}, workqueue.Change{}, conflict("public ingress review expired")
	}
	if err := available(state); err != nil {
		return storage.Publication{}, workqueue.Change{}, err
	}
	item, guards, err := service.capture(ctx, hostID, reviewedRevision, desired, state.Applied)
	if err != nil {
		return storage.Publication{}, workqueue.Change{}, err
	}
	if item.ReviewToken != reviewToken {
		return storage.Publication{}, workqueue.Change{}, conflict(
			"public ingress routing or release changed since review",
		)
	}
	item.TaskID, item.Revision = taskID, state.LastIssued+1
	next := state
	next.LastIssued, next.Pending = item.Revision, &item
	change, err := service.repository.PrepareReplace(ctx, state, next, guards)
	if err != nil {
		return storage.Publication{}, workqueue.Change{}, err
	}
	return item, change, nil
}

// Authorize rechecks the reviewed authority and its guards immediately
// before dispatch. Queued work must not use an unverified stale upstream.
func (service *Service) Authorize(ctx context.Context, hostID, taskID string) (storage.State, error) {
	state, next, guards, err := service.captureAuthorization(ctx, hostID, taskID)
	if err != nil {
		return storage.State{}, err
	}
	return service.repository.Replace(ctx, state, next, guards)
}

// PrepareAuthorize joins authoritative route revalidation and the irreversible
// authorization flag to the native Agent attempt grant.
func (service *Service) PrepareAuthorize(
	ctx context.Context,
	hostID, taskID string,
) (storage.State, workqueue.Change, error) {
	state, next, guards, err := service.captureAuthorization(ctx, hostID, taskID)
	if err != nil {
		return storage.State{}, workqueue.Change{}, err
	}
	change, err := service.repository.PrepareReplace(ctx, state, next, guards)
	if err != nil {
		return storage.State{}, workqueue.Change{}, err
	}
	return next, change, nil
}

func (service *Service) captureAuthorization(
	ctx context.Context, hostID, taskID string,
) (storage.State, storage.State, []keyvalue.Condition, error) {
	state, err := service.Read(ctx, hostID)
	if err != nil {
		return storage.State{}, storage.State{}, nil, err
	}
	if state.Pending == nil || state.Pending.TaskID != taskID {
		return storage.State{}, storage.State{}, nil, conflict("public ingress activation is not pending for this Task")
	}
	pending := *state.Pending
	item, guards, err := service.capture(ctx, hostID, pending.ReviewedRevision, pending.Desired, state.Applied)
	if err != nil {
		return storage.State{}, storage.State{}, nil, err
	}
	if item.ReviewToken != pending.ReviewToken || item.Digest != pending.Digest {
		return storage.State{}, storage.State{}, nil, conflict(
			"public ingress routing or release changed before activation",
		)
	}
	pending.Authorized = true
	next := state
	next.Pending = &pending
	return state, next, guards, nil
}

// Settle promotes only an authenticated Agent receipt of the captured
// revision on the existing immutable runtime, or the first provisioned runtime.
func (service *Service) Settle(
	ctx context.Context,
	hostID, taskID string,
	receipt storage.Receipt,
) (storage.State, error) {
	return service.finalize(ctx, hostID, taskID, receipt, promote)
}

// Restore settles a failed activation using independent evidence that the
// predecessor is again active. With no predecessor, an unresolved first
// activation remains fenced until its host effect can be proven absent.
func (service *Service) Restore(
	ctx context.Context,
	hostID, taskID string,
	receipt storage.Receipt,
) (storage.State, error) {
	return service.finalize(ctx, hostID, taskID, receipt, restore)
}

func (service *Service) PrepareSettle(
	ctx context.Context,
	hostID, taskID string,
	receipt storage.Receipt,
) (workqueue.Change, error) {
	return service.prepareFinalization(ctx, hostID, taskID, receipt, promote)
}

func (service *Service) PrepareRestore(
	ctx context.Context,
	hostID, taskID string,
	receipt storage.Receipt,
) (workqueue.Change, error) {
	return service.prepareFinalization(ctx, hostID, taskID, receipt, restore)
}

func (service *Service) PrepareCancel(ctx context.Context, hostID, taskID string) (workqueue.Change, error) {
	return service.prepareFinalization(ctx, hostID, taskID, storage.Receipt{}, cancel)
}

type finalization func(storage.State, string, storage.Receipt) (storage.State, bool, error)

func (service *Service) finalize(
	ctx context.Context, hostID, taskID string, receipt storage.Receipt, update finalization,
) (storage.State, error) {
	state, err := service.Read(ctx, hostID)
	if err != nil {
		return storage.State{}, err
	}
	next, changed, err := update(state, taskID, receipt)
	if err != nil || !changed {
		return next, err
	}
	return service.repository.Replace(ctx, state, next, nil)
}

func (service *Service) prepareFinalization(
	ctx context.Context, hostID, taskID string, receipt storage.Receipt, update finalization,
) (workqueue.Change, error) {
	state, err := service.Read(ctx, hostID)
	if err != nil {
		return workqueue.Change{}, err
	}
	next, changed, err := update(state, taskID, receipt)
	if err != nil || !changed {
		return workqueue.Change{}, err
	}
	return service.repository.PrepareReplace(ctx, state, next, nil)
}

func promote(state storage.State, taskID string, receipt storage.Receipt) (storage.State, bool, error) {
	if state.Applied != nil && state.Applied.TaskID == taskID && *state.Active == receipt {
		return state, false, nil
	}
	if state.Pending == nil || state.Pending.TaskID != taskID || !state.Pending.Authorized ||
		!state.Pending.Matches(receipt) || state.Active != nil && state.Active.RuntimeID != receipt.RuntimeID {
		return storage.State{}, false, conflict("public ingress receipt does not match the exact pending activation")
	}
	state.Applied, state.Active, state.Pending = state.Pending, &receipt, nil
	return state, true, nil
}

func restore(state storage.State, taskID string, receipt storage.Receipt) (storage.State, bool, error) {
	if state.Active == nil || *state.Active != receipt {
		return storage.State{}, false, conflict("public ingress restoration does not prove the last known good runtime")
	}
	if state.LastFailedTaskID == taskID {
		return state, false, nil
	}
	if state.Pending == nil || state.Pending.TaskID != taskID || !state.Pending.Authorized {
		return storage.State{}, false, conflict(
			"public ingress restoration does not match the exact pending activation",
		)
	}
	state.Pending, state.LastFailedTaskID = nil, taskID
	return state, true, nil
}

func cancel(state storage.State, taskID string, _ storage.Receipt) (storage.State, bool, error) {
	if state.LastFailedTaskID == taskID {
		return state, false, nil
	}
	if state.Pending == nil || state.Pending.TaskID != taskID || state.Pending.Authorized {
		return storage.State{}, false, conflict(
			"public ingress cancellation requires proof activation was never authorized",
		)
	}
	state.Pending, state.LastFailedTaskID = nil, taskID
	return state, true, nil
}

func conflict(message string) error { return errs.New(errs.KindStateConflict, message) }

func existingPublication(state storage.State, taskID string) *storage.Publication {
	for _, item := range []*storage.Publication{state.Pending, state.Applied} {
		if item != nil && item.TaskID == taskID {
			return item
		}
	}
	return nil
}

func available(state storage.State) error {
	if state.Pending != nil || state.LastIssued == math.MaxInt64 {
		return conflict("public ingress has an activation to settle before another change")
	}
	return nil
}

func normalize(desired Desired) Desired {
	desired.Addresses = slices.Clone(desired.Addresses)
	desired.RouteIDs = slices.Clone(desired.RouteIDs)
	slices.SortFunc(desired.Addresses, netip.Addr.Compare)
	slices.Sort(desired.RouteIDs)
	return desired
}

func equalDesired(left, right Desired) bool {
	return left.Enabled == right.Enabled && slices.Equal(left.Addresses, right.Addresses) &&
		slices.Equal(left.RouteIDs, right.RouteIDs)
}

func (service *Service) capture(
	ctx context.Context, hostID string, reviewedRevision int64, desired Desired, applied *storage.Publication,
) (storage.Publication, []keyvalue.Condition, error) {
	desired = normalize(desired)
	if !desired.Enabled {
		return storage.Publication{}, nil, conflict(
			"public ingress requires confirmed listener withdrawal before disabling",
		)
	}
	if !desired.Validate() {
		return storage.Publication{}, nil, errs.New(
			errs.KindValidationFailed,
			"public ingress addresses or Routes are invalid or repeated",
		)
	}
	if applied != nil && !slices.Equal(applied.Desired.Addresses, desired.Addresses) {
		return storage.Publication{}, nil, conflict(
			"public ingress requires confirmed listener withdrawal before changing addresses",
		)
	}
	if service.authority == nil {
		return storage.Publication{}, nil, conflict("public ingress authority is unavailable")
	}
	source, err := service.authority.Capture(ctx, hostID, desired)
	if err != nil {
		return storage.Publication{}, nil, err
	}
	guards, err := validateCapture(hostID, desired, source)
	if err != nil {
		return storage.Publication{}, nil, err
	}
	plan, err := edge.Build(source.Input)
	if err != nil {
		return storage.Publication{}, nil, err
	}
	configuration, err := jcs.Canonicalize(plan.Configuration)
	if err != nil {
		return storage.Publication{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	item := storage.Publication{
		Desired: desired, Configuration: configuration, Digest: storage.Digest(configuration),
		Claims: claimsFor(plan.Routes), ReviewedRevision: reviewedRevision,
	}
	review, err := json.Marshal(struct {
		HostID   string
		Revision int64
		Desired  Desired
		Routes   []edge.Route
		Digest   string
	}{hostID, reviewedRevision, desired, plan.Routes, item.Digest})
	if err != nil {
		return storage.Publication{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	item.ReviewToken = strings.TrimPrefix(storage.Digest(review), "sha256:")
	return item, guards, nil
}

func claimsFor(routes []edge.Route) []storage.Claim {
	var claims []storage.Claim
	for _, route := range routes {
		if len(claims) == 0 || claims[len(claims)-1].Hostname != route.Source.Host {
			claims = append(claims, storage.Claim{Hostname: route.Source.Host, EnvironmentID: route.EnvironmentID})
		}
	}
	return claims
}

func validateCapture(hostID string, desired Desired, source Capture) ([]keyvalue.Condition, error) {
	input := source.Input
	addresses := slices.Clone(input.Addresses)
	slices.SortFunc(addresses, netip.Addr.Compare)
	routeIDs := make([]string, 0, len(input.Routes))
	for _, route := range input.Routes {
		routeIDs = append(routeIDs, route.Source.ID)
	}
	slices.Sort(routeIDs)
	if hostID != input.HostID || !slices.Equal(desired.Addresses, addresses) ||
		!slices.Equal(desired.RouteIDs, routeIDs) || len(source.Guards) == 0 {
		return nil, conflict("public ingress authority did not capture the exact Host, Routes and listener bindings")
	}
	guards := make([]keyvalue.Condition, 0, len(source.Guards))
	seen := make(map[string]keyvalue.Condition, len(source.Guards))
	for _, guard := range source.Guards {
		if guard.Key == "" || guard.ModRevision < 0 || guard.ModRevision > input.Revision ||
			guard.Prefix && guard.ModRevision != 0 {
			return nil, conflict("public ingress authority contains invalid revision evidence")
		}
		if prior, exists := seen[guard.Key]; exists {
			if prior != guard {
				return nil, conflict("public ingress authority has conflicting revision evidence")
			}
			continue
		}
		guards = append(guards, guard)
		seen[guard.Key] = guard
	}
	return guards, nil
}
