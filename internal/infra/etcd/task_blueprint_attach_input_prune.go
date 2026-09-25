package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A terminal parent can discard only generations it never transferred to a
// child. A transferred generation remains child authority even after the
// parent Task and its journal expire.
func (repository *TaskRepository) pruneParentOwnedBlueprintAttachInput(
	ctx context.Context,
	task TaskRecord,
	taskRevision int64,
	retention etcdstore.KeyValue,
	readRevision int64,
) (bool, error) {
	if task.Executor != taskjournal.TaskExecutorBlueprint {
		return false, nil
	}
	const maximumGenerations = 512
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: attachinputs.ParentPrefix(task.ID), Limit: maximumGenerations + 1,
		Revision: readRevision,
	})
	if err != nil {
		return true, err
	}
	if page == nil || page.ReadRevision != readRevision || page.More || len(page.Values) > maximumGenerations {
		if page != nil {
			etcdstore.ClearRangeValues(page.Values)
		}
		return true, errs.New(errs.KindStateConflict, "Blueprint Attach input generation scan is incomplete")
	}
	defer etcdstore.ClearRangeValues(page.Values)
	for _, value := range page.Values {
		generation, err := attachinputs.Decode(value.Value)
		if err != nil {
			return true, err
		}
		if value.Key != attachinputs.Key(task.ID, generation.AttachID) || generation.ParentTaskID != task.ID ||
			generation.EnvironmentID != task.Owner.EnvironmentID {
			attachinputs.Clear(&generation)
			return true, errs.New(errs.KindStateConflict, "Blueprint Attach input generation identity changed")
		}
		if generation.OwnerKind == attachinputs.OwnerChild {
			attachinputs.Clear(&generation)
			continue
		}
		if generation.OwnerKind != attachinputs.OwnerParent || generation.OwnerTaskID != task.ID ||
			generation.Transfer != nil {
			attachinputs.Clear(&generation)
			return true, errs.New(errs.KindStateConflict, "Blueprint Attach input generation ownership changed")
		}
		conditions := []etcdstore.Condition{
			{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskRevision},
			{Key: retention.Key, ModRevision: retention.ModRevision},
			{Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
			{Key: value.Key, ModRevision: value.ModRevision},
		}
		mutations := []etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: value.Key}}
		if generation.SecretPins != nil {
			pins, err := tasksecretpins.NewEtcdRepository(repository.store, generation.BackingProjectID)
			if err != nil {
				attachinputs.Clear(&generation)
				return true, err
			}
			root, found, err := pins.LoadActive(ctx, generation.OperationID)
			if err != nil {
				attachinputs.Clear(&generation)
				return true, err
			}
			if found {
				if root.TaskID() != task.ID || root.AttemptID() != task.ID ||
					root.MembershipCount() != generation.SecretPins.Count ||
					root.MembershipSHA256() != generation.SecretPins.SHA256 {
					attachinputs.Clear(&generation)
					return true, errs.New(errs.KindStateConflict, "Blueprint Attach input Secret pin ownership changed")
				}
				if root.Phase() == tasksecretpins.RootPhaseReleasing {
					attachinputs.Clear(&generation)
					_, err := pins.ResumeRelease(ctx)
					return true, err
				}
				if root.Phase() != tasksecretpins.RootPhaseActive {
					attachinputs.Clear(&generation)
					return true, errs.New(errs.KindStateConflict, "Blueprint Attach input Secret pins are not active")
				}
				fragment, err := tasksecretpins.BeginRelease(root)
				if err != nil {
					attachinputs.Clear(&generation)
					return true, err
				}
				pinConditions, pinMutations, err := tasksecretpins.EtcdFragment(fragment)
				if err != nil {
					fragment.Clear()
					attachinputs.Clear(&generation)
					return true, err
				}
				conditions = append(conditions, pinConditions...)
				mutations = append(mutations, pinMutations...)
			}
		}
		attachinputs.Clear(&generation)
		commit, err := repository.store.Transact(ctx, conditions, mutations)
		etcdstore.ClearValues(commit.FailureReads)
		etcdstore.ClearMutationValues(mutations)
		if err != nil {
			return true, err
		}
		if !commit.Succeeded {
			return true, errs.New(errs.KindStateConflict, "Blueprint Attach input prune raced ownership")
		}
		return true, nil
	}
	return false, nil
}

// Once a hidden child has no execution claim, assignment or recovery owner,
// its encrypted input generation is no longer needed to reconstruct a plan.
// Output facts have a separate retention owner and are not removed here.
func (repository *TaskRepository) pruneChildOwnedBlueprintAttachInput(
	ctx context.Context,
	task TaskRecord,
	taskRevision int64,
	retention etcdstore.KeyValue,
	readRevision int64,
) (bool, error) {
	parentID := task.Params[taskjournal.TaskBlueprintParentParam]
	attachID := task.Params[attachinputs.TaskAttachIDParam]
	if parentID == "" || attachID == "" {
		return false, nil
	}
	if !taskjournal.IsBlueprintChild(task.Params) ||
		task.Executor != taskjournal.TaskExecutorAgent || task.Type != taskjournal.TaskUpdate ||
		task.Target != task.Owner.EnvironmentID {
		return true, errs.New(errs.KindStateConflict, "Blueprint Attach input prune Task shape changed")
	}
	key := attachinputs.Key(parentID, attachID)
	keys := []string{
		key,
		taskjournal.TaskActiveOperationKey(task.OperationID),
		taskjournal.TaskAssignmentIndexKey(task.ID),
		taskjournal.TaskRecoveryProofRequiredKey(task.ID),
		blueprintunits.AppliedKey(task.Owner.EnvironmentID, blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: attachID}),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: readRevision})
	if err != nil {
		return true, err
	}
	if read == nil || read.ReadRevision != readRevision || len(read.Values) != len(keys) {
		if read != nil {
			etcdstore.ClearValues(read.Values)
		}
		return true, errs.New(errs.KindInternal, "Blueprint Attach child input prune read is incomplete")
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] == nil {
		return false, nil
	}
	generation, err := attachinputs.Decode(read.Values[0].Value)
	if err != nil {
		return true, err
	}
	defer attachinputs.Clear(&generation)
	if generation.ParentTaskID != parentID || generation.AttachID != attachID ||
		generation.EnvironmentID != task.Owner.EnvironmentID ||
		generation.OperationID != task.OperationID ||
		generation.OwnerKind != attachinputs.OwnerChild || generation.OwnerTaskID != task.ID ||
		generation.Transfer == nil || generation.Transfer.ParentTaskID != parentID ||
		generation.Transfer.ChildTaskID != task.ID {
		return true, errs.New(errs.KindStateConflict, "Blueprint Attach child input ownership changed")
	}
	if read.Values[1] != nil || read.Values[2] != nil || read.Values[3] != nil {
		return true, nil
	}
	if read.Values[4] != nil {
		applied, err := blueprintunits.DecodeApplied(read.Values[4].Value)
		if err != nil || applied.EnvironmentID != task.Owner.EnvironmentID ||
			applied.Target != (blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: attachID}) {
			return true, errs.New(errs.KindStateConflict, "Blueprint Attach applied owner changed")
		}
		if applied.SourceTaskID == task.ID {
			return true, nil
		}
	}
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskRevision},
		{Key: retention.Key, ModRevision: retention.ModRevision},
		{Key: key, ModRevision: read.Values[0].ModRevision},
	}
	for _, absentKey := range keys[1:4] {
		conditions = append(conditions, etcdstore.Condition{Key: absentKey})
	}
	conditions = append(conditions, etcdstore.Condition{
		Key: keys[4], ModRevision: etcdstore.RevisionOf(read.Values[4]),
	})
	commit, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{{
		Type: etcdstore.MutationDelete, Key: key,
	}})
	etcdstore.ClearValues(commit.FailureReads)
	if err != nil {
		return true, err
	}
	if !commit.Succeeded {
		return true, errs.New(errs.KindStateConflict, "Blueprint Attach child input prune raced ownership")
	}
	return true, nil
}
