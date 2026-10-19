package componentplanning

import (
	"context"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ManagedRuntimeSourceAuthority string

const (
	ManagedRuntimeSourceAuthorityDesired ManagedRuntimeSourceAuthority = "desired"
	ManagedRuntimeSourceAuthorityApplied ManagedRuntimeSourceAuthority = "applied"
)

// FrozenProjectionAuthority is the exact desired-head, Zone and applied-runtime
// evidence used by one Component candidate preparation. Preparation consumes
// this value only; it never substitutes a later mutable desired projection.
type FrozenProjectionAuthority struct {
	environmentID            string
	readRevision             int64
	desiredHeadRevision      int64
	desiredRevisionID        string
	desiredZones             []etcdstore.Versioned[zonerecord.Record]
	managedRuntimeSources    []projectionrecord.ManagedComponentRuntimeSource
	appliedProjection        etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
	appliedProjectionPresent bool
	unitRuntimeCondition     *etcdstore.Condition
	proxyServicePreflight    *ProxyServicePreflight
	proxyServiceConditions   []etcdstore.Condition
	// Frozen from captured membership, then extended only by validated inputs.
	proxyServiceConditionCount int
	proxySourceMutations       []etcdstore.Mutation
}

type FrozenProjectionAuthorityInput struct {
	EnvironmentID          string
	ReadRevision           int64
	DesiredHeadRevision    int64
	DesiredRevisionID      string
	DesiredZones           []etcdstore.Versioned[zonerecord.Record]
	DesiredComponents      []componentrecord.Record
	ManagedRuntimeSources  []projectionrecord.ManagedComponentRuntimeSource
	RuntimeSourceAuthority ManagedRuntimeSourceAuthority
}

func (repository *Planner) prepareFrozenDesiredAuthority(
	ctx context.Context,
	input FrozenProjectionAuthorityInput,
) (FrozenProjectionAuthority, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return FrozenProjectionAuthority{}, err
	}
	desiredHeadAbsent := input.DesiredHeadRevision == 0 && input.DesiredRevisionID == ""
	desiredHeadPresent := input.DesiredHeadRevision > 0 &&
		ids.Validate(ids.KindTask, input.DesiredRevisionID) == nil
	if repository == nil || repository.store == nil ||
		ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		input.ReadRevision <= 0 || (!desiredHeadAbsent && !desiredHeadPresent) ||
		desiredHeadAbsent && (len(input.DesiredZones) != 0 ||
			len(input.DesiredComponents) != 0 || len(input.ManagedRuntimeSources) != 0) ||
		(input.RuntimeSourceAuthority != ManagedRuntimeSourceAuthorityDesired &&
			input.RuntimeSourceAuthority != ManagedRuntimeSourceAuthorityApplied) {
		return FrozenProjectionAuthority{}, errs.New(
			errs.KindValidationFailed,
			"Component projection authority is invalid",
		)
	}
	if input.RuntimeSourceAuthority == ManagedRuntimeSourceAuthorityApplied &&
		(len(input.DesiredComponents) != 0 || len(input.ManagedRuntimeSources) != 0) {
		return FrozenProjectionAuthority{}, errs.New(
			errs.KindValidationFailed,
			"applied Component runtime authority includes desired sources",
		)
	}
	zones := append([]etcdstore.Versioned[zonerecord.Record](nil), input.DesiredZones...)
	sort.Slice(zones, func(left, right int) bool {
		return zones[left].Record.Desired.ID < zones[right].Record.Desired.ID
	})
	previousZoneID := ""
	for _, zone := range zones {
		if zonerecord.ValidateRecord(zone.Record) != nil ||
			zone.Record.EnvironmentID != input.EnvironmentID ||
			zone.Record.Desired.ID <= previousZoneID || zone.Revision <= 0 ||
			zone.ReadRevision != input.ReadRevision || zone.ReadRevision < zone.Revision {
			return FrozenProjectionAuthority{}, errs.New(
				errs.KindValidationFailed,
				"Component projection Zone authority is invalid",
			)
		}
		previousZoneID = zone.Record.Desired.ID
	}
	authorityState, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{blueprints.EnvironmentBlueprintHeadKey(input.EnvironmentID)},
		Revision: input.ReadRevision,
	})
	if err != nil {
		return FrozenProjectionAuthority{}, err
	}
	if authorityState == nil || authorityState.ReadRevision != input.ReadRevision ||
		len(authorityState.Values) != 1 {
		return FrozenProjectionAuthority{}, errs.New(
			errs.KindInternal,
			"Component candidate projection authority read is incomplete",
		)
	}
	defer etcdstore.ClearValues(authorityState.Values)
	desiredHeadValue := authorityState.Values[0]
	if desiredHeadAbsent {
		if desiredHeadValue != nil {
			return FrozenProjectionAuthority{}, errs.New(
				errs.KindStateConflict,
				"Component candidate desired projection was published",
			)
		}
	} else {
		if desiredHeadValue == nil || desiredHeadValue.ModRevision != input.DesiredHeadRevision {
			return FrozenProjectionAuthority{}, errs.New(
				errs.KindStateConflict,
				"Component candidate desired projection changed",
			)
		}
		revisionID, decodeErr := idempotencyrecord.DecodeTaskReference(desiredHeadValue.Value)
		if decodeErr != nil {
			return FrozenProjectionAuthority{}, decodeErr
		}
		if revisionID != input.DesiredRevisionID {
			return FrozenProjectionAuthority{}, errs.New(
				errs.KindStateConflict,
				"Component candidate desired projection changed",
			)
		}
	}
	return FrozenProjectionAuthority{
		environmentID: input.EnvironmentID, readRevision: input.ReadRevision,
		desiredHeadRevision: input.DesiredHeadRevision,
		desiredRevisionID:   input.DesiredRevisionID,
		desiredZones:        zones,
	}, nil
}
