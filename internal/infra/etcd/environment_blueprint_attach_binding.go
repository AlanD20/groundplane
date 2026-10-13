package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *EnvironmentBlueprintRepository) SettleBlueprintAttachBinding(
	ctx context.Context, parent TaskRecord, unit blueprintunits.Unit,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if validateBlueprintParentClaimTask(parent) != nil || parent.Status != taskjournal.TaskStatusRunning ||
		unit.Target.Kind != ids.KindAttach || unit.Removal {
		return errs.New(errs.KindValidationFailed, "Attach binding settlement authority is invalid")
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return err
	}
	snapshot, err := ledger.Load(ctx, parent.Owner.EnvironmentID)
	if err != nil {
		return err
	}
	if snapshot.HeadTaskID != parent.ID {
		return errs.New(errs.KindStateConflict, "Attach binding Apply has been superseded")
	}
	keys := []string{
		taskjournal.TaskStorageKey(parent.ID), taskjournal.BlueprintParentClaimKey(parent.ID),
		attachinputs.Key(parent.ID, unit.Target.ID),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: snapshot.ReadRevision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != snapshot.ReadRevision || len(read.Values) != len(keys) {
		return errs.New(errs.KindInternal, "Attach binding settlement capture is incomplete")
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		return errs.New(errs.KindStateConflict, "Attach binding settlement source is unavailable")
	}
	stored, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil {
		return err
	}
	claimID, err := idempotency.DecodeTaskReference(read.Values[1].Value)
	if err != nil {
		return err
	}
	generation, err := attachinputs.Decode(read.Values[2].Value)
	if err != nil {
		return err
	}
	defer attachinputs.Clear(&generation)
	if validateBlueprintParentClaimTask(stored) != nil || stored.ID != parent.ID ||
		stored.RenderGeneration != parent.RenderGeneration ||
		stored.Status != taskjournal.TaskStatusRunning || stored.Owner != parent.Owner ||
		stored.PlanID != parent.PlanID || stored.PlanHash != parent.PlanHash || claimID != parent.ID ||
		generation.ParentTaskID != parent.ID || generation.EnvironmentID != parent.Owner.EnvironmentID ||
		generation.AttachID != unit.Target.ID || generation.OwnerKind != attachinputs.OwnerParent ||
		generation.OwnerTaskID != parent.ID ||
		!generation.SettlesInController() {
		return errs.New(errs.KindStateConflict, "Attach binding settlement ownership changed")
	}
	mutation, err := blueprintunits.PrepareControllerAttachBinding(
		snapshot, unit, generation.OperationID, read.Values[2].ModRevision,
	)
	if err != nil {
		return err
	}
	defer mutation.Clear()
	conditions := mutation.Conditions()
	mutations := mutation.Mutations()
	for index, key := range keys {
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: read.Values[index].ModRevision})
	}
	conditions = append(conditions, etcdstore.Condition{Key: taskjournal.BlueprintParentAbortKey(parent.ID)})
	if generation.Binding != nil {
		conditions = append(conditions, generation.Binding.Conditions()...)
	} else {
		connectionConditions, connectionMutations, err := repository.prepareBlueprintAttachConnection(ctx, parent, generation, snapshot.ReadRevision)
		if err != nil {
			return err
		}
		defer etcdstore.ClearMutationValues(connectionMutations)
		conditions, mutations = append(conditions, connectionConditions...), append(mutations, connectionMutations...)
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	etcdstore.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Attach binding changed before settlement")
	}
	return nil
}
