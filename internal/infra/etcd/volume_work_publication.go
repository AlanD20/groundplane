package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumework"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type VolumeWorkPublication struct {
	Project              keyvalue.Versioned[hierarchyrecord.ProjectRecord]
	Environment          keyvalue.Versioned[hierarchyrecord.EnvironmentRecord]
	ExpectedHeadRevision int64
	Claim                blueprints.EnvironmentBlueprintStageClaim
	Projection           projectionrecord.EnvironmentComposeProjection
	Policy               VolumeRemovalBackupPolicyPreparation
	Task                 workexecution.Task
	Item                 workexecution.Item
	Input                *agentpb.VolumeWorkInput
	Marker               idempotency.IdempotencyMarker
}

// PublishVolumeWork is the atomic native Volume admission boundary. Input chunk
// staging is not dispatchable; desired state, policy, ownership and work become
// visible together under the existing final-publication transaction envelope.
func (repository *EnvironmentBlueprintRepository) PublishVolumeWork(
	ctx context.Context,
	request VolumeWorkPublication,
) (IdempotencyTransactionResult, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if repository == nil || repository.HierarchyRepository == nil || repository.transactions == nil ||
		hierarchyrecord.ValidateProject(
			request.Project.Record,
		) != nil || hierarchyrecord.ValidateEnvironment(request.Environment.Record) != nil ||
		request.Project.Record.Kind != hierarchyrecord.ProjectKindTenant || request.Project.Revision <= 0 || request.Environment.Revision <= 0 ||
		request.Environment.Record.ProjectID != request.Project.Record.ID ||
		request.Environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
		request.Item.ID != ids.DeriveAt(ids.KindWorkItem, request.Claim.CreatedAt, request.Claim.TaskID, "volume") ||
		request.Task.Scope.EnvironmentID != request.Environment.Record.ID || request.Task.Scope.ProjectID != request.Project.Record.ID ||
		request.Task.Scope.TenantID != request.Project.Record.TenantID || request.Task.Actor != workexecution.OperatorActor {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Volume publication requires its ready owning Environment",
		)
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, request.Marker); err != nil ||
		found {
		return existing, err
	}
	fence, err := repository.loadEnvironmentBlueprintMutationFence(ctx, request.Project, request.Environment)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	host, err := hostbindings.ReadLocal(ctx, repository.store, fence.ReadRevision())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if request.Item.Placement.HostID != host.Binding.HostID || request.Item.Placement.Executor != workexecution.Agent {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"Volume target host changed before publication",
		)
	}
	references, err := repository.prepareVolumeDesiredReferences(
		ctx,
		request.Task,
		request.Projection,
		request.ExpectedHeadRevision,
		fence.ReadRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	root, err := repository.prepareMutationDesiredRoot(
		ctx,
		request.Claim,
		request.Projection,
		request.Task,
		request.Marker,
		request.ExpectedHeadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ZeroMutationBytes(root.Mutations)
	identities, err := prepareEnvironmentDesiredIdentityPublication(ctx, repository.store, request.Projection,
		request.Claim.SourceKind, request.ExpectedHeadRevision, fence.ReadRevision())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer identities.clear()
	policy, err := prepareVolumeWorkPolicy(
		request.Policy,
		request.Claim,
		request.Projection,
		request.Task,
		request.Marker,
		time.Now().UTC(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ZeroMutationBytes(policy.Mutations)
	inputs, err := volumework.NewInputRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	admission := append(fence.TransactionConditions(), root.Conditions...)
	admission = append(admission, host.Conditions...)
	admission = append(admission, references...)
	work, err := inputs.PreparePublication(
		ctx,
		request.Task,
		request.Item,
		request.Input,
		request.Marker,
		admission,
		request.Claim.RevisionID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ZeroMutationBytes(work.Mutations)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epoch.Value)
	ancestry, err := hierarchydeletion.BindMutationEpochs(
		ctx,
		repository.store,
		fence.ReadRevision(),
		hierarchydeletion.MutationScope{
			TenantID:  request.Project.Record.TenantID,
			ProjectID: request.Project.Record.ID,
		},
		nil,
		nil,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer ancestry.Clear()
	conditions, err := resourcePublicationConditions(
		work.Conditions,
		root.Conditions,
		identities.conditions,
		policy.Conditions,
		ancestry.Conditions(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	mutations := append([]keyvalue.Mutation(nil), work.Mutations...)
	mutations = append(mutations, root.Mutations...)
	mutations = append(mutations, identities.mutations...)
	mutations = append(mutations, policy.Mutations...)
	mutations = append(mutations, ancestry.Mutations()...)
	mutations = append(mutations, epoch)
	tenant, err := loadConnectorTaskInitiationTenantAtRevision(
		ctx,
		repository.store,
		request.Project,
		fence.ReadRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(
		tenant,
		request.Project,
		request.Environment,
		taskjournal.TaskActorOperator,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions, classify, err := prepareTaskInitiationFences(
		initiation,
		conditions,
		resourceWorkPublicationConflict(conditions),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask, conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replay, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return replay.applyEnvironmentBlueprint(ctx, request.Marker, plan, repository.transactions)
}
