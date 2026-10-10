package blueprint

import (
	"context"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type authoredRuntimeComponents struct {
	authorities []etcd.BlueprintRuntimeComponent
	receipts    []etcd.BlueprintRuntimeComponentReceipt
	records     []componentrecord.Record
	projected   []core.Component
	sources     []projectionrecord.ManagedComponentRuntimeSource
}

// loadAuthoredRuntimeComponents joins the immutable authored selection to the
// exact applied receipts and acknowledged runtime records at the unit snapshot.
// The complete singleton pair is returned for rendering; an omitted singleton
// is accepted only when the ledger does not retain a possible applied effect.
func (service *Service) loadAuthoredRuntimeComponents(
	ctx context.Context,
	parent etcd.TaskRecord,
	source authoredRuntimeProjectionSource,
	snapshot blueprintunits.Snapshot,
) (authoredRuntimeComponents, error) {
	authorities, err := service.repository.ReadEnvironmentBlueprintRuntimeComponents(
		ctx, parent.Owner.EnvironmentID, snapshot.ReadRevision,
	)
	if err != nil {
		return authoredRuntimeComponents{}, err
	}
	result := authoredRuntimeComponents{
		authorities: authorities,
		records:     make([]componentrecord.Record, 0, len(authorities)),
		projected:   make([]core.Component, 0, len(authorities)),
		sources:     make([]projectionrecord.ManagedComponentRuntimeSource, 0, len(authorities)),
	}
	selected := make(map[string]projectionrecord.OwnedIdentity, len(source.identities.Record.Components))
	for _, identity := range source.identities.Record.Components {
		selected[identity.ID] = identity
	}
	currentIDs := make(map[string]struct{}, len(authorities))
	for _, authority := range authorities {
		record := authority.Component.Record
		currentIDs[record.Desired.ID] = struct{}{}
		identity, explicitlyOwned := selected[record.Desired.ID]
		if !explicitlyOwned {
			if record.Desired.Enabled || len(record.Runtime.GeneratedServices) != 0 ||
				record.Runtime.PinnedIPv4 != "" || authoredRuntimeComponentEffectPossible(snapshot, record.Desired.ID) {
				return authoredRuntimeComponents{}, errs.New(
					errs.KindStateConflict,
					"omitted Blueprint Component has not settled disabled",
				)
			}
		} else {
			spec, found := source.desired.Record.Input.Components[identity.Name]
			if !found || spec.Implementation != record.Desired.Kind || !authority.RuntimePresent {
				return authoredRuntimeComponents{}, errs.New(
					errs.KindStateConflict,
					"Blueprint Component runtime identity changed",
				)
			}
			unit, applied, found := authoredRuntimeAppliedReceipt(
				snapshot, blueprintunits.ResourceKey{Kind: ids.KindComponent, ID: identity.ID},
			)
			if !found || applied.Record.SourceTaskID != authority.Runtime.Record.TaskID ||
				applied.Record.SourcePlanID != authority.Runtime.Record.PlanID ||
				applied.Record.ParentTaskID != authority.Runtime.Record.RevisionID {
				return authoredRuntimeComponents{}, errs.New(
					errs.KindStateConflict,
					"Blueprint Component receipt does not match acknowledged runtime",
				)
			}
			result.receipts = append(result.receipts, etcd.BlueprintRuntimeComponentReceipt{
				Identity: identity, Unit: unit, Applied: applied,
			})
			delete(selected, record.Desired.ID)
		}
		projected, err := componentrecord.ProjectRecord(record)
		if err != nil {
			return authoredRuntimeComponents{}, err
		}
		result.records = append(result.records, record)
		result.projected = append(result.projected, projected)
		if record.Desired.Enabled {
			source, err := authoredManagedComponentRuntimeSource(authority)
			if err != nil {
				return authoredRuntimeComponents{}, err
			}
			result.sources = append(result.sources, source)
		}
	}
	if len(selected) != 0 {
		return authoredRuntimeComponents{}, errs.New(
			errs.KindStateConflict, "Blueprint Component singleton is unavailable",
		)
	}
	for _, applied := range snapshot.Applied {
		if applied.Record.Target.Kind != ids.KindComponent {
			continue
		}
		if _, current := currentIDs[applied.Record.Target.ID]; !current &&
			applied.Record.State != blueprintunits.Absent {
			return authoredRuntimeComponents{}, errs.New(
				errs.KindStateConflict, "Blueprint Component ledger retains an unknown effect",
			)
		}
	}
	sort.Slice(result.receipts, func(left, right int) bool {
		return result.receipts[left].Identity.ID < result.receipts[right].Identity.ID
	})
	sort.Slice(result.sources, func(left, right int) bool {
		return result.sources[left].ComponentKind < result.sources[right].ComponentKind
	})
	return result, nil
}

func authoredRuntimeAppliedReceipt(
	snapshot blueprintunits.Snapshot,
	target blueprintunits.ResourceKey,
) (blueprintunits.Unit, etcdstore.Versioned[blueprintunits.AppliedRecord], bool) {
	if snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID {
		return blueprintunits.Unit{}, etcdstore.Versioned[blueprintunits.AppliedRecord]{}, false
	}
	for _, unit := range snapshot.Desired.Record.Units {
		if unit.Target != target || unit.Removal {
			continue
		}
		for _, applied := range snapshot.Applied {
			if applied.Record.Target == target && applied.Record.State == blueprintunits.Applied &&
				applied.Record.Fingerprint == unit.Fingerprint {
				return unit, applied, true
			}
		}
		return blueprintunits.Unit{}, etcdstore.Versioned[blueprintunits.AppliedRecord]{}, false
	}
	return blueprintunits.Unit{}, etcdstore.Versioned[blueprintunits.AppliedRecord]{}, false
}

func authoredRuntimeComponentEffectPossible(snapshot blueprintunits.Snapshot, componentID string) bool {
	for _, applied := range snapshot.Applied {
		if applied.Record.Target.Kind == ids.KindComponent && applied.Record.Target.ID == componentID {
			return applied.Record.State != blueprintunits.Absent
		}
	}
	return false
}

func authoredManagedComponentRuntimeSource(
	authority etcd.BlueprintRuntimeComponent,
) (projectionrecord.ManagedComponentRuntimeSource, error) {
	if !authority.RuntimePresent {
		return projectionrecord.ManagedComponentRuntimeSource{}, errs.New(
			errs.KindStateConflict, "enabled Component acknowledged runtime is missing",
		)
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		authority.Runtime.Record.Artifact, artifact,
	); err != nil {
		return projectionrecord.ManagedComponentRuntimeSource{}, errs.New(
			errs.KindStateConflict, "Component acknowledged runtime artifact changed",
		)
	}
	return projectionrecord.PrepareManagedComponentRuntimeSource(
		projectionrecord.EnvironmentComposeProjection{
			EnvironmentID:    authority.Runtime.Record.EnvironmentID,
			RevisionID:       authority.Runtime.Record.RevisionID,
			RenderGeneration: authority.Runtime.Record.RenderGeneration,
			ComposeArtifact:  authority.Runtime.Record.Artifact,
		},
		authority.Runtime.Record.Component,
		artifact,
	)
}
