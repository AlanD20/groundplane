package etcd

import (
	"context"
	"net/netip"

	blueprintplanning "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	groupstore "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublishEnvironmentDesiredRevisionWithTask atomically advances the sole
// Environment desired-state pointer and enqueues the Task pinned to that
// already sealed revision. Direct desired mutations preserve the existing
// Environment pool.
func (repository *HierarchyRepository) PublishEnvironmentDesiredRevisionWithTask(
	ctx context.Context,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	expectedHeadRevision int64,
	claim blueprints.EnvironmentBlueprintStageClaim,
	revision blueprints.EnvironmentDesiredRevisionIdentity,
	projection projectionrecord.EnvironmentComposeProjection,
	zoneChanges []blueprints.EnvironmentBlueprintZoneChange,
	serviceChanges []blueprints.EnvironmentBlueprintServiceChange,
	routeChanges []blueprints.EnvironmentBlueprintRouteChange,
	releaseGroupPreparation groupstore.ReleaseGroupBlueprintPreparedMutation,
	componentPreparation componentplanning.ComponentTaskPreparation,
	attachPreparation blueprintplanning.BlueprintAttachTaskPreparation,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"direct Environment desired publication requires mutation source authority",
		)
	}
	return repository.publishEnvironmentDesiredRevisionWithTask(
		ctx, netip.Prefix{}, environment.Record.NetworkPool,
		project, environment, expectedHeadRevision, claim, revision, projection,
		zoneChanges, serviceChanges, routeChanges, releaseGroupPreparation,
		componentPreparation, attachPreparation, blueprintplanning.BlueprintBackupPolicyPreparation{},
		BlueprintScriptPublication{}, BlueprintReleasePublication{},
		BlueprintRequirementGate{}, VolumeRemovalBackupPolicyPreparation{}, nil, task, marker, nil,
	)
}

// PublishEnvironmentBlueprintDesiredRevision publishes the one authored
// Blueprint Task with its prepared Script and candidate Release fragments.
func (repository *EnvironmentBlueprintRepository) PublishEnvironmentBlueprintDesiredRevision(
	ctx context.Context,
	environmentPool netip.Prefix,
	desiredNetworkPool string,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	expectedHeadRevision int64,
	claim blueprints.EnvironmentBlueprintStageClaim,
	revision blueprints.EnvironmentDesiredRevisionIdentity,
	projection projectionrecord.EnvironmentComposeProjection,
	zoneChanges []blueprints.EnvironmentBlueprintZoneChange,
	serviceChanges []blueprints.EnvironmentBlueprintServiceChange,
	routeChanges []blueprints.EnvironmentBlueprintRouteChange,
	releaseGroupPreparation groupstore.ReleaseGroupBlueprintPreparedMutation,
	componentPreparation componentplanning.ComponentTaskPreparation,
	attachPreparation blueprintplanning.BlueprintAttachTaskPreparation,
	backupPreparation blueprintplanning.BlueprintBackupPolicyPreparation,
	scriptPublication BlueprintScriptPublication,
	releasePublication BlueprintReleasePublication,
	requirementGate BlueprintRequirementGate,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if claim.SourceKind != blueprints.EnvironmentBlueprintSourceApply {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Environment Blueprint publication requires apply source authority",
		)
	}
	return repository.publishEnvironmentDesiredRevisionWithTask(
		ctx, environmentPool, desiredNetworkPool,
		project, environment, expectedHeadRevision, claim, revision, projection,
		zoneChanges, serviceChanges, routeChanges, releaseGroupPreparation,
		componentPreparation, attachPreparation, backupPreparation, scriptPublication, releasePublication,
		requirementGate, VolumeRemovalBackupPolicyPreparation{}, nil, task, marker, repository.transactions,
	)
}
