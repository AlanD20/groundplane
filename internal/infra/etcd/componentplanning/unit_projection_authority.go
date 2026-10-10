package componentplanning

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ComponentPredecessor returns the same captured artifact used by the unit's
// publication fence. Sealing must not reread a newer acknowledged runtime.
func (authority FrozenProjectionAuthority) ComponentPredecessor() (projectionrecord.EnvironmentComposeProjection, bool) {
	if authority.unitRuntimeCondition == nil || !authority.appliedProjectionPresent {
		return projectionrecord.EnvironmentComposeProjection{}, false
	}
	return projectionrecord.CloneEnvironmentComposeProjection(authority.appliedProjection.Record), true
}

// PrepareFrozenComponentUnitAuthority selects the acknowledged singleton
// runtime, not the Environment's last aggregate Apply. Every predecessor and
// its absence is compared again in the child's atomic publication.
func (repository *Planner) PrepareFrozenComponentUnitAuthority(ctx context.Context, input FrozenProjectionAuthorityInput, current keyvalue.Versioned[componentrecord.Record]) (FrozenProjectionAuthority, error) {
	if input.RuntimeSourceAuthority != ManagedRuntimeSourceAuthorityApplied || current.ReadRevision != input.ReadRevision || current.Revision <= 0 ||
		current.Record.Desired.OwnerID != input.EnvironmentID || componentrecord.ValidateRecord(current.Record) != nil ||
		ids.Validate(ids.KindComponent, current.Record.Desired.ID) != nil {
		return FrozenProjectionAuthority{}, errs.New(errs.KindValidationFailed, "Blueprint Component predecessor identity is invalid")
	}
	authority, err := repository.prepareFrozenDesiredAuthority(ctx, input)
	if err != nil {
		return FrozenProjectionAuthority{}, err
	}
	runtime, found, err := ReadRuntime(ctx, repository.store, current.Record.Desired.ID, input.ReadRevision)
	if err != nil {
		return FrozenProjectionAuthority{}, err
	}
	condition := keyvalue.Condition{Key: RuntimeKey(current.Record.Desired.ID), ModRevision: runtime.Revision}
	authority.unitRuntimeCondition = &condition
	authority.managedRuntimeSources = nil
	authority.appliedProjection = keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]{}
	authority.appliedProjectionPresent = false
	if !found {
		if current.Record.Desired.Enabled || len(current.Record.Runtime.GeneratedServices) != 0 || current.Record.Runtime.PinnedIPv4 != "" {
			return FrozenProjectionAuthority{}, errs.New(errs.KindStateConflict, "enabled Component has no acknowledged runtime; its exact predecessor cannot be inferred")
		}
		return authority, nil
	}
	acknowledged := runtime.Record.Component
	acknowledged.Runtime.Healthy = current.Record.Runtime.Healthy
	if runtime.Record.EnvironmentID != input.EnvironmentID ||
		!componentrecord.EqualRecord(acknowledged, current.Record) {
		return FrozenProjectionAuthority{}, errs.New(errs.KindStateConflict, "Blueprint Component acknowledged predecessor changed")
	}
	projection := projectionrecord.EnvironmentComposeProjection{
		EnvironmentID: input.EnvironmentID, RevisionID: runtime.Record.RevisionID,
		RenderGeneration: runtime.Record.RenderGeneration, ComposeArtifact: append([]byte(nil), runtime.Record.Artifact...),
		Components: []componentrecord.Record{runtime.Record.Component},
	}
	for _, zone := range runtime.Record.Zones {
		projection.DesiredZones = append(projection.DesiredZones, projectionrecord.EnvironmentZoneProjection{EnvironmentID: input.EnvironmentID, Desired: zone.Desired})
	}
	authority.appliedProjection = keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]{Record: projection, Revision: runtime.Revision, ReadRevision: input.ReadRevision}
	authority.appliedProjectionPresent = true
	if !runtime.Record.Component.Desired.Enabled {
		return authority, nil
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(runtime.Record.Artifact, artifact); err != nil {
		return FrozenProjectionAuthority{}, errs.New(errs.KindStateConflict, "Blueprint Component predecessor artifact is unreadable")
	}
	source, err := projectionrecord.PrepareManagedComponentRuntimeSource(projection, runtime.Record.Component, artifact)
	if err != nil {
		return FrozenProjectionAuthority{}, err
	}
	authority.managedRuntimeSources = []projectionrecord.ManagedComponentRuntimeSource{source}
	return authority, nil
}
