package blueprint

import (
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// authoredOwnedUnitTarget binds one immutable authored name to the stable id
// selected at desired-head publication. Planning uses the id as reconciliation
// identity; the name is retained only to select that resource's exact authored
// input.
type authoredOwnedUnitTarget struct {
	Target blueprintunits.ResourceKey
	Name   string
}

// selectAuthoredOwnedUnitTargets returns the complete set of resource kinds
// whose stable identities are carried by the authored revision.
func selectAuthoredOwnedUnitTargets(
	identities projectionrecord.EnvironmentOwnedIdentities,
) ([]authoredOwnedUnitTarget, error) {
	if err := projectionrecord.ValidateEnvironmentOwnedIdentities(identities); err != nil {
		return nil, err
	}
	targets := make([]authoredOwnedUnitTarget, 0,
		len(identities.Services)+len(identities.Networks)+len(identities.Volumes)+
			len(identities.Entries)+len(identities.Routes)+len(identities.Attaches)+
			len(identities.Components)+len(identities.Scripts)+len(identities.DNSRecords),
	)
	appendIdentities := func(kind ids.Kind, values []projectionrecord.OwnedIdentity) {
		for _, identity := range values {
			targets = append(targets, authoredOwnedUnitTarget{
				Target: blueprintunits.ResourceKey{Kind: kind, ID: identity.ID},
				Name:   identity.Name,
			})
		}
	}
	appendIdentities(ids.KindService, identities.Services)
	appendIdentities(ids.KindNetwork, identities.Networks)
	appendIdentities(ids.KindVolume, identities.Volumes)
	appendIdentities(ids.KindEnvEntry, identities.Entries)
	appendIdentities(ids.KindRoute, identities.Routes)
	appendIdentities(ids.KindDNSRecord, identities.DNSRecords)
	appendIdentities(ids.KindAttach, identities.Attaches)
	appendIdentities(ids.KindComponent, identities.Components)
	appendIdentities(ids.KindScript, identities.Scripts)
	slices.SortFunc(targets, compareAuthoredOwnedUnitTargets)
	for index, target := range targets {
		if index > 0 && targets[index-1].Target == target.Target {
			return nil, errs.New(errs.KindInternal, "Environment desired unit identity is duplicated")
		}
	}
	return targets, nil
}

// planAcknowledgedOwnedOmissions combines retained omission intent with the
// ledger's acknowledged effect state. Desired identity omission never implies
// runtime absence; only an acknowledged absence can skip native removal.
func planAcknowledgedOwnedOmissions(
	environmentID string,
	selected []authoredOwnedUnitTarget,
	snapshot blueprintunits.Snapshot,
	pending []projectionrecord.OwnedOmission,
) ([]blueprintunits.Unit, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		snapshot.EnvironmentID != environmentID {
		return nil, errs.New(errs.KindInternal, "Blueprint unit snapshot belongs to another Environment")
	}
	selectedByTarget := make(map[blueprintunits.ResourceKey]struct{}, len(selected))
	for index, target := range selected {
		if target.Name == "" || index > 0 && compareAuthoredOwnedUnitTargets(selected[index-1], target) >= 0 {
			return nil, errs.New(errs.KindInternal, "Environment desired unit identities are invalid")
		}
		selectedByTarget[target.Target] = struct{}{}
	}
	omissions := make([]blueprintunits.Unit, 0)
	pendingByTarget := make(map[blueprintunits.ResourceKey]bool, len(pending))
	for _, target := range pending {
		pendingByTarget[blueprintunits.ResourceKey{Kind: target.Kind, ID: target.ID}] = true
	}
	seenApplied := make(map[blueprintunits.ResourceKey]struct{}, len(snapshot.Applied))
	for _, versioned := range snapshot.Applied {
		applied := versioned.Record
		if applied.EnvironmentID != environmentID {
			return nil, errs.New(errs.KindInternal, "Blueprint applied unit belongs to another Environment")
		}
		if _, duplicate := seenApplied[applied.Target]; duplicate {
			return nil, errs.New(errs.KindInternal, "Blueprint applied unit is duplicated")
		}
		seenApplied[applied.Target] = struct{}{}
		if _, retained := selectedByTarget[applied.Target]; retained ||
			applied.State == blueprintunits.Absent && (versioned.Revision > 0 || !pendingByTarget[applied.Target]) {
			continue
		}
		switch applied.Target.Kind {
		case ids.KindVolume:
			return nil, errs.New(
				errs.KindResourceInUse,
				"Blueprint omits a persistent Volume; remove the Volume separately before Apply",
			)
		case ids.KindService, ids.KindNetwork, ids.KindEnvEntry, ids.KindRoute, ids.KindDNSRecord,
			ids.KindAttach, ids.KindComponent, ids.KindScript:
			omissions = append(omissions, blueprintunits.Unit{
				Target: applied.Target, Removal: true,
				Writes: []blueprintunits.ResourceKey{applied.Target},
			})
		default:
			return nil, errs.New(
				errs.KindStateConflict,
				"Blueprint cannot reconcile an owned resource whose immutable desired identity is unavailable",
			)
		}
	}
	// Admission retains omitted identities even when their direct lifecycle never
	// wrote a Blueprint-unit receipt. Their removal still needs native source
	// capture; this inventory is not synthesized applied-effect evidence.
	for _, target := range pending {
		key := blueprintunits.ResourceKey{Kind: target.Kind, ID: target.ID}
		if _, retained := selectedByTarget[key]; retained {
			return nil, errs.New(errs.KindInternal, "Blueprint omission remains declared")
		}
		if _, acknowledged := seenApplied[key]; acknowledged {
			continue
		}
		if key.Kind == ids.KindVolume {
			return nil, errs.New(errs.KindResourceInUse,
				"Blueprint omits a persistent Volume; remove the Volume separately before Apply")
		}
		omissions = append(omissions, blueprintunits.Unit{
			Target: key, Removal: true, Writes: []blueprintunits.ResourceKey{key},
		})
	}
	slices.SortFunc(omissions, func(left, right blueprintunits.Unit) int {
		return compareBlueprintUnitKeys(left.Target, right.Target)
	})
	return omissions, nil
}

func compareAuthoredOwnedUnitTargets(left, right authoredOwnedUnitTarget) int {
	return compareBlueprintUnitKeys(left.Target, right.Target)
}

func compareBlueprintUnitKeys(left, right blueprintunits.ResourceKey) int {
	if order := strings.Compare(string(left.Kind), string(right.Kind)); order != 0 {
		return order
	}
	return strings.Compare(left.ID, right.ID)
}
