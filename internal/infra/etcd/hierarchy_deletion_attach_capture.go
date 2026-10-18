package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type HierarchyAttachCapture struct {
	Source     HierarchyAttachInput
	Capture    attachremoval.WorkCapture
	Revision   int64
	Conditions []keyvalue.Condition
}

func HierarchyAttachOperation(view workqueue.View) string {
	return ids.DeriveAt(ids.KindOperation, view.Task.Task.CreatedAt, view.Item.Item.ID, "attach-removal-input")
}

func (capture *HierarchyAttachCapture) Clear() {
	if capture != nil {
		attachremoval.Clear(&capture.Source.Input)
		capture.Capture.Clear()
	}
}

// LoadHierarchyAttachCapture distinguishes unprepared work from an invalid or
// changed source. Absence is useful only to preparation, never to execution.
func (repository *HierarchyDeletionRepository) LoadHierarchyAttachCapture(ctx context.Context, view workqueue.View) (HierarchyAttachCapture, error) {
	source, err := repository.LoadHierarchyAttachInput(ctx, view)
	if err != nil {
		return HierarchyAttachCapture{}, err
	}
	result := HierarchyAttachCapture{Source: source, Conditions: append([]keyvalue.Condition(nil), source.Conditions...)}
	key := attachremoval.WorkCaptureKey(view.Item.Item.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		result.Clear()
		return HierarchyAttachCapture{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 {
		result.Clear()
		return HierarchyAttachCapture{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if read.Values[0] == nil {
		result.Conditions = append(result.Conditions, keyvalue.Condition{Key: key})
		return result, nil
	}
	if read.Values[0].Key != key || read.Values[0].ModRevision <= 0 {
		result.Clear()
		return HierarchyAttachCapture{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	capture, err := attachremoval.DecodeWorkCapture(read.Values[0].Value)
	if err != nil {
		result.Clear()
		return HierarchyAttachCapture{}, err
	}
	result.Capture, result.Revision = capture, read.Values[0].ModRevision
	if !matchesHierarchyAttachCapture(view, source.Input, capture) {
		result.Clear()
		return HierarchyAttachCapture{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	capture.ApplyTo(&result.Source.Input)
	if attachremoval.Validate(result.Source.Input) != nil {
		result.Clear()
		return HierarchyAttachCapture{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	result.Conditions = append(result.Conditions, keyvalue.Condition{Key: key, ModRevision: result.Revision})
	return result, nil
}

func matchesHierarchyAttachCapture(view workqueue.View, source attachremoval.Input, capture attachremoval.WorkCapture) bool {
	requiresHook := false
	if source.Backing.Desired.Adapter == "custom" && source.Backing.Desired.Hooks != nil {
		for _, input := range source.Backing.Desired.Hooks.Inputs {
			requiresHook = requiresHook || input.Generate == ""
		}
	}
	if requiresHook != (capture.Hook != nil) {
		return false
	}
	digest, err := attachremoval.Digest(source)
	return err == nil && capture.Validate() == nil && view.Item.Item.Input != nil && capture.TaskID == view.Task.Task.ID &&
		capture.WorkItemID == view.Item.Item.ID && capture.OperationID == HierarchyAttachOperation(view) &&
		capture.Action == *view.Item.Item.Input && capture.FrozenSHA256 == digest && capture.BackingProjectID == source.Attach.BackingProjectID
}

// PrepareHierarchyAttachCapture atomically publishes the encrypted capture and
// activates exact Secret retention. Preparing pins alone grants no execution.
func (repository *HierarchyDeletionRepository) PrepareHierarchyAttachCapture(ctx context.Context, view workqueue.View,
	capture attachremoval.WorkCapture, prepared tasksecretpins.Prepared,
) (workqueue.Change, error) {
	item, task := view.Item.Item, view.Task.Task
	if !hierarchyCaptureMayActivate(view) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Attach input preparation requires unstarted removal work")
	}
	current, err := repository.LoadHierarchyAttachCapture(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer current.Clear()
	if current.Revision != 0 || !matchesHierarchyAttachCapture(view, current.Source.Input, capture) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Attach removal input is already captured or changed")
	}
	if (capture.Pins == nil) != prepared.IsZero() {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	envelope, err := repository.hierarchyPreparedCaptureGuard(ctx, view, capture)
	if err != nil {
		return workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: append(current.Conditions,
		envelope,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision})}
	if capture.Pins != nil {
		binding, err := prepared.Binding()
		if err != nil || binding != *capture.Pins {
			return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		fragment, err := tasksecretpins.Activation(prepared)
		if err != nil {
			return workqueue.Change{}, err
		}
		conditions, mutations, err := tasksecretpins.EtcdFragment(fragment)
		if err != nil {
			fragment.Clear()
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, conditions...)
		change.Mutations = append(change.Mutations, mutations...)
	}
	raw, err := attachremoval.EncodeWorkCapture(capture)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
		Key: attachremoval.WorkCaptureKey(item.ID), Value: raw},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: envelope.Key})
	return change, nil
}

// HierarchyAttachAdmission guards the published capture and its live Secret
// ownership in the same assignment or Start transaction as the shared work CAS.
func (repository *HierarchyDeletionRepository) HierarchyAttachAdmission(ctx context.Context, view workqueue.View) (workqueue.Change, error) {
	capture, err := repository.LoadHierarchyAttachCapture(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer capture.Clear()
	if capture.Revision == 0 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Attach removal inputs have not been captured")
	}
	change := workqueue.Change{Conditions: capture.Conditions}
	sourceConditions, err := attachremoval.SourceConditions(ctx, repository.store, capture.Source.Input)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, sourceConditions...)
	if capture.Capture.Pins != nil {
		pins, err := tasksecretpins.NewEtcdRepository(repository.store, capture.Capture.BackingProjectID)
		if err != nil {
			return workqueue.Change{}, err
		}
		root, found, err := pins.LoadActive(ctx, capture.Capture.OperationID, view.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		if !found {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Attach removal lost its captured Secret retention")
		}
		condition, err := root.AdmissionCondition(*capture.Capture.Pins)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: condition.Key, ModRevision: condition.ModRevision})
	}
	return change, nil
}
