package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type NetworkWorkPublication struct {
	Project     keyvalue.Versioned[hierarchyrecord.ProjectRecord]
	Environment keyvalue.Versioned[hierarchyrecord.EnvironmentRecord]
	Zone        keyvalue.Versioned[zones.Record]
	Authorities environmentchanges.EnvironmentZoneRemovalAuthorities
	Claim       blueprints.EnvironmentBlueprintStageClaim
	Projection  projectionrecord.EnvironmentComposeProjection
	Task        workexecution.Task
	Item        workexecution.Item
	Input       *agentpb.NetworkWorkInput
	Marker      idempotency.IdempotencyMarker
}

// PublishNetworkWork publishes direct removal of an Environment-owned Zone.
// Backing Zone cascades must first capture their cross-Environment dependencies
// and impact approval; they cannot use this narrower publication boundary.
func (repository *EnvironmentBlueprintRepository) PublishNetworkWork(ctx context.Context, request NetworkWorkPublication) (IdempotencyTransactionResult, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	task, item := request.Task, request.Item
	if repository == nil || repository.HierarchyRepository == nil || repository.transactions == nil ||
		request.Project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		request.Environment.Record.ProjectID != request.Project.Record.ID ||
		request.Environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
		task.Scope != (workexecution.Scope{TenantID: request.Project.Record.TenantID,
			ProjectID: request.Project.Record.ID, EnvironmentID: request.Environment.Record.ID}) ||
		task.Actor != workexecution.OperatorActor || task.Action != workexecution.ActionRemove ||
		item.ID != ids.DeriveAt(ids.KindWorkItem, request.Claim.CreatedAt, request.Claim.TaskID, "network") ||
		request.Input == nil || request.Input.NetworkId != request.Zone.Record.Desired.ID ||
		request.Input.RemovalSubnet != request.Zone.Record.Desired.Subnet ||
		request.Zone.Record.Desired.OwnerKind != core.ZoneOwnerEnvironment ||
		request.Zone.Record.Desired.OwnerID != request.Environment.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Zone removal requires its ready owning Environment")
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, request.Marker); err != nil || found {
		return existing, err
	}
	if _, err := environmentqueries.SelectZoneForDeletion(request.Authorities.Desired, request.Zone, task.TargetID); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateZoneDeletionHierarchy(ctx, request.Environment, request.Project, request.Zone); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := environmentchanges.ValidateZoneRemovalCandidate(request.Authorities.Desired.Record,
		request.Projection, task.TargetID, request.Zone.Record.Desired.Name, request.Input.DisconnectServiceIds); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if selectedByEnabledComponent(request.Authorities.Desired.Record.Components, task.TargetID) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindResourceInUse, "Zone is selected by an enabled Component; change or disable it first")
	}
	if request.Marker.ReplayTarget == nil || *request.Marker.ReplayTarget != (idempotency.IdempotencyReplayTarget{
		Kind: idempotency.IdempotencyReplayTargetZone, ID: task.TargetID}) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Zone removal replay target differs from its resource")
	}
	fence, err := repository.loadEnvironmentBlueprintMutationFence(ctx, request.Project, request.Environment)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	previous, found, err := repository.getEnvironmentBlueprintProjectionAtRevision(ctx, task.Scope.EnvironmentID, fence.ReadRevision())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if !found || previous.Revision != request.Authorities.Desired.Revision ||
		!environmentchanges.SameServiceRemovalProjection(previous.Record, request.Authorities.Desired.Record) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Zone removal desired source changed before publication")
	}
	host, err := hostbindings.ReadLocal(ctx, repository.store, fence.ReadRevision())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if item.Placement.HostID != host.Binding.HostID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Zone removal target host changed")
	}
	root, err := repository.prepareMutationDesiredRoot(ctx, request.Claim, request.Projection, task,
		request.Marker, request.Authorities.Desired.Revision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ZeroMutationBytes(root.Mutations)
	identities, err := prepareEnvironmentDesiredIdentityPublication(ctx, repository.store, request.Projection,
		request.Claim.SourceKind, request.Authorities.Desired.Revision, fence.ReadRevision())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer identities.clear()
	checks, err := edgechecks.RequireZoneIdle(ctx, repository.store, task.TargetID, fence.ReadRevision())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	dns, err := dnsrecords.NewReader(repository.store).RequireDNSUnreferenced(ctx, "", task.TargetID, fence.ReadRevision())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	zoneRepository := &ZoneRepository{store: repository.store}
	pool, err := zoneRepository.getZonePoolRegistryAtRevision(ctx, task.Scope.EnvironmentID, fence.ReadRevision())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if pool.Revision <= 0 || pool.Record.Reservations[task.TargetID] != request.Zone.Record.Desired.Subnet {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Zone subnet reservation differs from the captured Zone")
	}
	addresses, err := networkreservations.GetComponentAddressRegistry(ctx, repository.store, request.Zone.Record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if len(addresses.Record.Reservations) != 0 {
		return IdempotencyTransactionResult{}, errs.New(errs.KindResourceInUse, "Zone has reserved Component addresses")
	}
	tombstoneKey := deletions.TombstoneKey(string(deletions.DeletionTargetZone), task.TargetID)
	tombstone, err := deletions.EncodeDeletionTombstone(deletions.DeletionTombstoneRecord{
		TargetKind: deletions.DeletionTargetZone, TargetID: task.TargetID, TargetRevision: request.Zone.Revision,
		TaskID: task.ID, Phase: deletions.DeletionPhaseHostEffects, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(tombstone)
	admission, err := resourcePublicationConditions(fence.TransactionConditions(), host.Conditions, root.Conditions, identities.conditions, dns,
		[]keyvalue.Condition{checks,
			{Key: tombstoneKey},
			{Key: networkreservations.ZonePoolRegistryKey(task.Scope.EnvironmentID), ModRevision: pool.Revision},
			{Key: networkreservations.ComponentAddressRegistryKey(task.TargetID), ModRevision: addresses.Revision},
			{Key: projectionrecord.EnvironmentComposeProjectionStorageKey(task.Scope.EnvironmentID), ModRevision: request.Authorities.Applied.Revision}})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	inputs, err := networkwork.NewInputRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	work, err := inputs.PreparePublication(ctx, task, item, request.Input, request.Marker, admission, request.Claim.RevisionID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ZeroMutationBytes(work.Mutations)
	work.Conditions = append(work.Conditions, keyvalue.Condition{Key: networkwork.RemovalAuthorityKey(item.ID)})
	work.Mutations = append(work.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
		Key: networkwork.RemovalAuthorityKey(item.ID), Value: append([]byte(nil), tombstone...)})
	ancestry, err := hierarchydeletion.BindMutationEpochs(ctx, repository.store, fence.ReadRevision(),
		hierarchydeletion.MutationScope{TenantID: task.Scope.TenantID, ProjectID: task.Scope.ProjectID}, nil, nil)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer ancestry.Clear()
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epoch.Value)
	conditions, err := resourcePublicationConditions(work.Conditions, ancestry.Conditions())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	mutations := append([]keyvalue.Mutation(nil), work.Mutations...)
	mutations = append(mutations, root.Mutations...)
	mutations = append(mutations, identities.mutations...)
	mutations = append(mutations, ancestry.Mutations()...)
	mutations = append(mutations, epoch, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: tombstoneKey, Value: tombstone})
	plan, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask, conditions, mutations, resourceWorkPublicationConflict(conditions))
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replay, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return replay.applyEnvironmentBlueprint(ctx, request.Marker, plan, repository.transactions)
}
