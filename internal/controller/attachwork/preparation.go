package attachwork

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Preparation struct {
	inputs *Inputs
	queue  *workqueue.Repository
	units  *blueprintunits.Repository
}

func NewPreparation(
	inputs *Inputs,
	queue *workqueue.Repository,
	units *blueprintunits.Repository,
) (*Preparation, error) {
	if inputs == nil || queue == nil || units == nil {
		return nil, errs.New(errs.KindInternal, "Attach preparation requires captured inputs and Apply ownership")
	}
	return &Preparation{inputs: inputs, queue: queue, units: units}, nil
}

func (owner *Preparation) Prepare(ctx context.Context, taskID, workID string) error {
	work, err := owner.queue.ReadWork(ctx, taskID, workID, 0)
	if err != nil {
		return err
	}
	source, err := owner.inputs.repository.LoadWaitingCapture(ctx, work)
	if err != nil {
		return err
	}
	defer source.Clear()
	task, item := work.Task.Task, work.Item.Item
	snapshot, err := owner.units.LoadAtRevision(ctx, task.Scope.EnvironmentID, work.ReadRevision)
	if err != nil {
		return err
	}
	if snapshot.HeadTaskID != task.ID || snapshot.Desired == nil ||
		snapshot.Desired.Record.ParentTaskID != task.ID || !snapshot.Desired.Record.Complete {
		return errs.New(errs.KindStateConflict, "Attach preparation no longer belongs to the current Apply")
	}
	graph, err := owner.units.LoadCapturedGraph(ctx, task, work.ReadRevision)
	if err != nil {
		return err
	}
	matched := false
	for index, captured := range graph.Record.Items {
		if captured.ID != item.ID {
			continue
		}
		unit := graph.Record.Units[index]
		if captured.TaskID != item.TaskID || captured.Operation != item.Operation ||
			captured.TargetID != item.TargetID ||
			captured.Placement != item.Placement ||
			captured.Input != nil ||
			!slices.Equal(captured.Dependencies, item.Dependencies) ||
			!slices.Equal(captured.Claims, item.Claims) ||
			!captured.CreatedAt.Equal(item.CreatedAt) ||
			!captured.Deadline.Equal(item.Deadline) ||
			unit.Removal ||
			unit.Target.Kind != ids.KindAttach ||
			unit.Target.ID != source.Generation.Record.AttachID {
			return errs.New(errs.KindStateConflict, "Attach preparation differs from its accepted resource graph")
		}
		matched = true
		break
	}
	if !matched {
		return errs.New(errs.KindStateConflict, "Apply has no captured Attach work with this identity")
	}
	dependencies := make([]workqueue.ItemSnapshot, len(item.Dependencies))
	for index, id := range item.Dependencies {
		dependency, err := owner.queue.ReadWork(ctx, task.ID, id, work.ReadRevision)
		if err != nil {
			return err
		}
		dependencies[index] = dependency.Item
	}
	guards, err := workqueue.DependencyConditions(item, dependencies)
	if err != nil {
		return err
	}
	reference, err := blueprintattachinputs.WorkInput(source.Generation.Record)
	if err != nil {
		return err
	}
	if !source.Generation.Record.SettlesInController() {
		// Resolve once before readiness so corrupt encrypted input cannot become
		// an executable assignment. No plaintext is persisted by preparation.
		prepared := item
		prepared.Input = &reference
		payload, err := owner.inputs.planner.BuildCaptured(ctx, source.Generation.Record, task, prepared)
		if err != nil {
			return err
		}
		attachexecution.ClearProvision(payload)
	}
	at := time.Now().UTC()
	if !at.Before(task.Deadline) || !at.Before(item.Deadline) {
		return errs.New(errs.KindStateConflict, "Attach input preparation exceeded its deadline")
	}
	change, err := workqueue.PrepareInput(work.Task, work.Item, reference, at)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	var binding workqueue.Change
	if !source.Generation.Record.SettlesInController() {
		binding, err = owner.inputs.repository.PreparePendingBinding(ctx, work)
		if err != nil {
			return err
		}
		defer keyvalue.ZeroMutationBytes(binding.Mutations)
	}
	change.Conditions = append(change.Conditions, source.Conditions...)
	change.Conditions = append(change.Conditions, guards...)
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{
			Key:         blueprintunits.CapturedGraphKey(task.Scope.EnvironmentID, task.ID),
			ModRevision: graph.Revision,
		},
		keyvalue.Condition{
			Key:         blueprints.EnvironmentBlueprintHeadKey(task.Scope.EnvironmentID),
			ModRevision: snapshot.HeadRevision,
		},
		keyvalue.Condition{Key: blueprintunits.EpochKey(task.Scope.EnvironmentID), ModRevision: snapshot.EpochRevision},
		keyvalue.Condition{
			Key:         blueprintunits.DesiredPlanKey(task.Scope.EnvironmentID),
			ModRevision: snapshot.Desired.Revision,
		},
	)
	result, err := owner.queue.Commit(ctx, change, binding)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Attach authority changed while preparing its input")
	}
	return nil
}
