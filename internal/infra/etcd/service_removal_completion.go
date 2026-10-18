package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareServiceRemovalCompletion joins exact host proof, desired publication,
// runtime retirement and applied receipt. The report owner joins work completion
// and authenticated Agent guards in the same transaction.
func (repository *ServiceRepository) PrepareServiceRemovalCompletion(ctx context.Context, view workqueue.ResumeView,
	delivery *agentpb.ServiceLifecycleWorkInput, at time.Time,
) (workqueue.Change, error) {
	task, item, attempt := view.Work.Task.Task, view.Work.Item.Item, view.Attempt.Attempt
	if task.Action != workexecution.ActionRemove && task.Action != workexecution.ActionApply || task.Stop != nil || item.ForwardRevoked ||
		attempt.Direction != workexecution.Forward || attempt.Executor.Executor != workexecution.Agent {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Service removal cannot publish a stopped or replaced desired revision")
	}
	inputs, err := serviceremovalwork.NewRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, err
	}
	input, proof, err := inputs.PrepareHostProof(ctx, view, delivery, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	if task.Action == workexecution.ActionApply {
		ledger, err := blueprintunits.NewRepository(repository.store)
		if err != nil {
			keyvalue.ClearMutationValues(proof.Mutations)
			return workqueue.Change{}, err
		}
		receipt, err := ledger.PrepareServiceRemovalReceipt(ctx, view)
		if err != nil {
			keyvalue.ClearMutationValues(proof.Mutations)
			return workqueue.Change{}, err
		}
		settlement, err := repository.prepareBlueprintServiceRemovalState(ctx, inputs, input, view.Work)
		if err != nil {
			keyvalue.ClearMutationValues(proof.Mutations)
			keyvalue.ClearMutationValues(receipt.Mutations)
			return workqueue.Change{}, err
		}
		proof.Conditions = append(proof.Conditions, receipt.Conditions...)
		proof.Mutations = append(proof.Mutations, receipt.Mutations...)
		proof.Conditions = append(proof.Conditions, settlement.Conditions...)
		proof.Mutations = append(proof.Mutations, settlement.Mutations...)
		return proof, nil
	}
	settlement, err := repository.prepareDirectServiceRemovalState(ctx, inputs, input, view.Work.ReadRevision)
	if err != nil {
		keyvalue.ClearMutationValues(proof.Mutations)
		return workqueue.Change{}, err
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		keyvalue.ClearMutationValues(proof.Mutations)
		keyvalue.ClearMutationValues(settlement.Mutations)
		return workqueue.Change{}, err
	}
	snapshot, err := ledger.LoadAtRevision(ctx, input.Scope.EnvironmentID, view.Work.ReadRevision)
	if err != nil {
		keyvalue.ClearMutationValues(proof.Mutations)
		keyvalue.ClearMutationValues(settlement.Mutations)
		return workqueue.Change{}, err
	}
	receipt, err := blueprintunits.PrepareNativeRemovalReceipt(snapshot, blueprintunits.NativeRemoval{
		Target:      blueprintunits.ResourceKey{Kind: ids.KindService, ID: input.ServiceID},
		OperationID: ids.DeriveAt(ids.KindOperation, input.CreatedAt, input.TaskID, input.WorkItemID),
		Execution: blueprintunits.AppliedExecution{TaskID: task.ID, WorkItemID: item.ID, AttemptID: attempt.ID,
			Executor: attempt.Executor, Input: attempt.Input, Result: view.Checkpoint.Record.Evidence},
	})
	if err != nil {
		keyvalue.ClearMutationValues(proof.Mutations)
		keyvalue.ClearMutationValues(settlement.Mutations)
		return workqueue.Change{}, err
	}
	proof.Conditions = append(proof.Conditions, settlement.Conditions...)
	proof.Conditions = append(proof.Conditions, receipt.Conditions()...)
	proof.Mutations = append(proof.Mutations, settlement.Mutations...)
	proof.Mutations = append(proof.Mutations, receipt.Mutations()...)
	return proof, nil
}

func (repository *ServiceRepository) prepareDirectServiceRemovalState(ctx context.Context, inputs *serviceremovalwork.Repository,
	input serviceremovalwork.Input, revision int64,
) (workqueue.Change, error) {
	conditions, err := inputs.RuntimeGuards(ctx, input, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	desired, err := inputs.DesiredGuard(ctx, input, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	baseline, found, err := blueprints.ReadCurrentProjection(ctx, repository.store, input.Scope.EnvironmentID, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !found || baseline.Revision != input.HeadRevision || baseline.Record.RevisionID != input.BaseRevisionID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Service removal baseline was replaced before finalization")
	}
	keys := []string{
		blueprints.EnvironmentBlueprintEffectiveProjectionKey(input.Scope.EnvironmentID, input.DesiredRevisionID),
		blueprints.EnvironmentBlueprintOwnedIdentitiesKey(input.Scope.EnvironmentID, input.DesiredRevisionID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Service removal prepared revision is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != revision || len(read.Values) != len(keys) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Service removal prepared revision read is incomplete")
	}
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > revision {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Service removal prepared revision is unavailable")
		}
		conditions = append(conditions, keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision})
	}
	candidate, err := environmentprojection.DecodeEnvironmentComposeProjectionStorage(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if err := serviceremovalwork.ValidateCandidate(input, baseline.Record, candidate); err != nil {
		return workqueue.Change{}, err
	}
	dns, err := dnsrecords.NewReader(repository.store).RequireDNSUnreferenced(ctx, input.ServiceID, "", revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	scripts, err := scriptsourceevidence.PrepareServiceScriptAbsence(ctx, repository.store, input.ServiceID, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	addresses, err := networkreservations.PrepareProxyAddressRelease(ctx, repository.store, input.ServiceID, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, input.Scope.EnvironmentID, revision)
	if err != nil {
		keyvalue.ClearMutationValues(addresses.Mutations())
		return workqueue.Change{}, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		keyvalue.ClearMutationValues(addresses.Mutations())
		return workqueue.Change{}, err
	}
	head, err := idempotency.EncodeTaskReference(input.DesiredRevisionID)
	if err != nil {
		keyvalue.ClearMutationValues(addresses.Mutations())
		clear(epoch.Value)
		return workqueue.Change{}, err
	}
	conditions = append(conditions, desired, keyvalue.Condition{
		Key: blueprints.EnvironmentBlueprintHeadKey(input.Scope.EnvironmentID), ModRevision: input.HeadRevision})
	conditions = append(conditions, dns...)
	conditions = append(conditions, scripts...)
	conditions = append(conditions, addresses.Conditions()...)
	conditions = append(conditions, fence.TransactionConditions()...)
	mutations := append(addresses.Mutations(), epoch,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: blueprints.EnvironmentBlueprintHeadKey(input.Scope.EnvironmentID), Value: head},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: services.ServiceRuntimeKey(input.ServiceID)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: serviceruntimerecord.Key(input.ServiceID)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: deletions.TombstoneKey(string(deletions.DeletionTargetService), input.ServiceID)})
	return workqueue.Change{Conditions: conditions, Mutations: mutations}, nil
}
