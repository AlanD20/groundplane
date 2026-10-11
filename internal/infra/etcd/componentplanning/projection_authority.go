package componentplanning

import (
	"context"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	environmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
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
	proxySourceMutations     []etcdstore.Mutation
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

// PrepareFrozenProjectionAuthority captures the applied runtime at the same
// revision as the supplied desired authority. Authored desired heads select the
// applied runtime explicitly because they do not yet have an effective artifact.
func (repository *Planner) PrepareFrozenProjectionAuthority(
	ctx context.Context,
	input FrozenProjectionAuthorityInput,
) (FrozenProjectionAuthority, error) {
	authority, err := repository.prepareFrozenDesiredAuthority(ctx, input)
	if err != nil {
		return FrozenProjectionAuthority{}, err
	}
	appliedState, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{projectionrecord.EnvironmentComposeProjectionStorageKey(input.EnvironmentID)},
		Revision: input.ReadRevision,
	})
	if err != nil {
		return FrozenProjectionAuthority{}, err
	}
	if appliedState == nil || appliedState.ReadRevision != input.ReadRevision || len(appliedState.Values) != 1 {
		return FrozenProjectionAuthority{}, errs.New(
			errs.KindInternal,
			"Component applied projection read is incomplete",
		)
	}
	defer etcdstore.ClearValues(appliedState.Values)
	appliedValue := appliedState.Values[0]
	appliedPresent := appliedValue != nil
	applied := etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{ReadRevision: input.ReadRevision}
	if appliedPresent {
		projection, decodeErr := projectionrecord.DecodeEnvironmentComposeProjectionStorage(appliedValue.Value)
		if decodeErr != nil {
			return FrozenProjectionAuthority{}, decodeErr
		}
		if projection.EnvironmentID != input.EnvironmentID {
			return FrozenProjectionAuthority{}, errs.New(
				errs.KindStateConflict,
				"Component candidate applied Environment projection changed",
			)
		}
		applied.Record = projection
		applied.Revision = appliedValue.ModRevision
	}
	desired := projectionrecord.EnvironmentComposeProjection{
		EnvironmentID: input.EnvironmentID,
		RevisionID:    input.DesiredRevisionID,
		Components:    append([]componentrecord.Record(nil), input.DesiredComponents...),
		ManagedComponentRuntimeSources: append(
			[]projectionrecord.ManagedComponentRuntimeSource(nil),
			input.ManagedRuntimeSources...),
	}
	var managed []projectionrecord.ManagedComponentRuntimeSource
	switch input.RuntimeSourceAuthority {
	case ManagedRuntimeSourceAuthorityDesired:
		managed, err = projectionrecord.SelectManagedComponentRuntimeSources(desired, applied.Record, appliedPresent)
	case ManagedRuntimeSourceAuthorityApplied:
		if appliedPresent {
			managed, err = projectionrecord.SelectManagedComponentRuntimeSources(applied.Record, applied.Record, true)
		}
	}
	if err != nil {
		return FrozenProjectionAuthority{}, err
	}
	authority.managedRuntimeSources = append([]projectionrecord.ManagedComponentRuntimeSource(nil), managed...)
	authority.appliedProjection = applied
	authority.appliedProjectionPresent = appliedPresent
	return authority, nil
}

// Both aggregate and single-Component planning use this exact desired-head
// fence. Runtime selection belongs to the caller's explicit authority source.
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

// ReadCurrentProjectionAuthority reads the complete desired projection
// into the same frozen authority consumed by candidate preparation.
func (repository *Planner) ReadCurrentProjectionAuthority(
	ctx context.Context,
	environmentID string,
	readRevision int64,
) (FrozenProjectionAuthority, error) {
	desired, found, err := blueprints.ReadCurrentProjection(ctx, repository.store, environmentID, readRevision)
	if err != nil {
		return FrozenProjectionAuthority{}, err
	}
	if !found {
		return repository.PrepareFrozenProjectionAuthority(ctx, FrozenProjectionAuthorityInput{
			EnvironmentID: environmentID, ReadRevision: readRevision,
			RuntimeSourceAuthority: ManagedRuntimeSourceAuthorityDesired,
		})
	}
	zones := make([]etcdstore.Versioned[zonerecord.Record], len(desired.Record.DesiredZones))
	for index, item := range desired.Record.DesiredZones {
		zones[index], err = environmentqueries.JoinZone(desired, item)
		if err != nil {
			return FrozenProjectionAuthority{}, err
		}
	}
	return repository.PrepareFrozenProjectionAuthority(ctx, FrozenProjectionAuthorityInput{
		EnvironmentID: environmentID, ReadRevision: readRevision,
		DesiredHeadRevision: desired.Revision, DesiredRevisionID: desired.Record.RevisionID,
		DesiredZones: zones, DesiredComponents: desired.Record.Components,
		ManagedRuntimeSources:  desired.Record.ManagedComponentRuntimeSources,
		RuntimeSourceAuthority: ManagedRuntimeSourceAuthorityDesired,
	})
}
