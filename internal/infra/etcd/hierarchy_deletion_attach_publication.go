package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionattach"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type hierarchyAttachPublication struct {
	task       TaskRecord
	conditions []keyvalue.Condition
	mutations  []keyvalue.Mutation
	values     [][]byte
	input      hierarchydeletionattach.Publication
	hook       backingHookTaskPublication
}

func (publication *hierarchyAttachPublication) clear() {
	keyvalue.ClearByteSlices(publication.values)
	publication.hook.clear()
	publication.input.Clear()
}

func (repository *HierarchyDeletionRepository) prepareHierarchyAttachPublication(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation, action hierarchydeletion.HierarchyDeletionAction,
	task TaskRecord, previous *hierarchydeletion.HierarchyDeletionChildEntry,
) (_ hierarchyAttachPublication, returnErr error) {
	if repository.attachPlanBuilder == nil || task.Type != taskjournal.TaskRemove ||
		action.ActionKind != hierarchydeletion.HierarchyDeletionAttachDeprovision {
		return hierarchyAttachPublication{}, errs.New(
			errs.KindInternal,
			"hierarchy Attach cleanup plan builder is unavailable",
		)
	}
	frozenKey := hierarchydeletionattach.FrozenKey(operation.Tombstone.OperationID, action.TargetID)
	input, frozenRevision, err := hierarchydeletionattach.Read(ctx, repository.store, frozenKey)
	if err != nil {
		return hierarchyAttachPublication{}, err
	}
	defer hierarchydeletionattach.Clear(&input)
	base := input
	base.HookInputs, base.HookInputSet, base.HookOperationID = nil, nil, ""
	digest, err := hierarchydeletionattach.Digest(base)
	if err != nil || input.ParentOperationID != operation.Tombstone.OperationID ||
		input.SnapshotRevision != operation.Tombstone.SnapshotRevision ||
		input.Attach.ID != action.TargetID ||
		input.AttachRevision != action.TargetRevision ||
		digest != action.AgentProcedure.InputDigest ||
		!hierarchydeletionattach.NeedsAgent(input) {
		return hierarchyAttachPublication{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	conditions, err := hierarchydeletionattach.SourceConditions(ctx, repository.store, input)
	if err != nil {
		return hierarchyAttachPublication{}, err
	}
	input.PlanID, input.TaskID, input.ActionOrdinal = task.PlanID, task.ID, action.Ordinal
	draft := task
	draft.PlanHash, draft.Steps = "", nil
	prepared, err := repository.attachPlanBuilder.BuildHierarchyDeletionAttachPlan(ctx, draft, input)
	if err != nil {
		return hierarchyAttachPublication{}, err
	}
	if !hierarchydeletion.ValidHierarchyDeletionDigest(prepared.PlanHash) || len(prepared.Steps) == 0 ||
		len(prepared.Steps) > attachrecord.MaximumAttachGrants+1 {
		if prepared.HookInputs != nil {
			clear(prepared.HookInputs.Ciphertext)
		}
		return hierarchyAttachPublication{}, errs.New(errs.KindInternal, "hierarchy Attach sealed plan is invalid")
	}
	for _, step := range prepared.Steps {
		if step.Kind != taskjournal.TaskStepOperation || ids.Validate(ids.KindStep, step.ID) != nil {
			return hierarchyAttachPublication{}, errs.New(
				errs.KindInternal,
				"hierarchy Attach sealed plan steps are invalid",
			)
		}
	}
	task.PlanHash, task.Steps = prepared.PlanHash, prepared.Steps
	task.Configuration = taskconfiguration.CloneTaskConfiguration(prepared.Configuration)
	publication := hierarchyAttachPublication{task: task, conditions: conditions}
	defer func() {
		if returnErr != nil {
			returnErr = publication.hook.finish(ctx, repository.store, returnErr)
			publication.clear()
		}
	}()
	publication.conditions = append(
		publication.conditions,
		keyvalue.Condition{Key: frozenKey, ModRevision: frozenRevision},
	)
	if previous == nil {
		publication.hook, err = prepareBackingHookTaskPublication(ctx, repository.store, task, prepared.HookInputs)
		if err != nil {
			return hierarchyAttachPublication{}, err
		}
		publication.task = publication.hook.task
		publication.conditions = append(publication.conditions, publication.hook.pins.conditions...)
		publication.mutations = append(publication.mutations, publication.hook.pins.mutations...)
	} else if err := repository.prepareHierarchyAttachRetry(ctx, &publication, previous); err != nil {
		return hierarchyAttachPublication{}, err
	}
	input.HookInputs = prepared.HookInputs
	if publication.task.Configuration != nil {
		input.HookInputSet = publication.task.Configuration.BackingHookInputs
	}
	publication.input, err = hierarchydeletionattach.PrepareInputPublication(
		ctx, repository.store, input, task.OperationID, previous != nil,
	)
	if err != nil {
		return hierarchyAttachPublication{}, err
	}
	publication.conditions = append(publication.conditions, publication.input.Conditions...)
	publication.mutations = append(publication.mutations, publication.input.Mutations...)
	return publication, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyAttachRetry(ctx context.Context,
	publication *hierarchyAttachPublication, previous *hierarchydeletion.HierarchyDeletionChildEntry,
) error {
	read, err := repository.store.Get(ctx, taskjournal.TaskStorageKey(previous.CurrentTaskID))
	if err != nil {
		return err
	}
	if read == nil || read.Entry == nil {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer clear(read.Entry.Value)
	source, err := DecodeTaskRecord(read.Entry.Value)
	if err != nil || source.ID != previous.CurrentTaskID || source.OperationID != publication.task.OperationID {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	if source.Configuration == nil || source.Configuration.SecretPins == nil {
		return nil
	}
	if publication.task.Configuration == nil ||
		!taskconfiguration.SameTaskBackingHookInputSet(
			source.Configuration.BackingHookInputs,
			publication.task.Configuration.BackingHookInputs,
		) {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, taskSecretPinProjectID(source))
	if err != nil {
		return err
	}
	root, found, err := pins.LoadActive(ctx, source.OperationID)
	if err != nil {
		return err
	}
	binding := source.Configuration.SecretPins
	if !found || root.AttemptID() != source.ID || root.TaskID() != binding.TaskID ||
		root.MembershipCount() != binding.Count ||
		root.MembershipSHA256() != binding.SHA256 {
		return errs.New(errs.KindStateConflict, "hierarchy Attach Retry lost its Secret source authority")
	}
	fragment, err := tasksecretpins.RetainForRetry(root, publication.task.ID)
	if err != nil {
		return err
	}
	compares, writes, err := tasksecretpins.EtcdFragment(fragment)
	if err != nil {
		fragment.Clear()
		return err
	}
	publication.task.Configuration.SecretPins = taskconfiguration.CloneTaskConfiguration(
		source.Configuration,
	).SecretPins
	publication.conditions = append(publication.conditions, compares...)
	publication.mutations = append(publication.mutations, writes...)
	for _, write := range writes {
		publication.values = append(publication.values, write.Value)
	}
	return nil
}
