package releasegroups

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RestorationSource struct {
	Capture       keyvalue.Versioned[ExecutionCapture]
	Preparation   RestorationPreparation
	Source        workqueue.View
	SourceAttempt workqueue.AttemptSnapshot
	Failed        workqueue.ItemSnapshot
	Authority     releaseexecution.GroupRestorationAuthority
	Conditions    []keyvalue.Condition
}

// LoadRestorationSource follows an EXPLICIT failed member selected by policy or
// retained executable input. It never chooses a newer failure on reconnect. The
// source input/runtime remain owned by the successful forward work until the
// whole Group settles and collection can begin.
func (repository *ExecutionRepository) LoadRestorationSource(ctx context.Context, work workqueue.View, failedWorkID string) (RestorationSource, error) {
	item, task := work.Item.Item, work.Task.Task
	if item.Operation != workexecution.ReleaseRestoration || item.TaskID != task.ID || work.Item.Revision <= 0 ||
		work.Task.Revision <= 0 || work.ReadRevision < work.Item.Revision || work.ReadRevision < work.Task.Revision {
		return RestorationSource{}, errs.New(errs.KindStateConflict, "Group restoration requires its fixed native work snapshot")
	}
	capture, err := repository.Load(ctx, work.Task, work.ReadRevision)
	if err != nil {
		return RestorationSource{}, err
	}
	policy := capture.Record.Policy
	index := slices.IndexFunc(policy.Members, func(member domain.GroupWorkMember) bool { return member.RestoreID == item.ID })
	failedIndex := slices.IndexFunc(policy.Members, func(member domain.GroupWorkMember) bool { return member.ForwardID == failedWorkID })
	if index < 0 || failedIndex <= index || policy.Manifest.FailurePolicy != domain.OnFailureSwitchBack ||
		item.TargetID != policy.Members[index].Member.ServiceID || item.Placement != capture.Record.Placement {
		return RestorationSource{}, errs.New(errs.KindStateConflict, "Group restoration is not an earlier member of this captured switch-back")
	}
	preparation, err := capture.Record.RestorationPreparation(index)
	if err != nil {
		return RestorationSource{}, err
	}
	source, err := repository.queue.ReadWork(ctx, task.ID, preparation.SourceWorkID, work.ReadRevision)
	if err != nil {
		return RestorationSource{}, err
	}
	if source.Item.Item.Phase != workexecution.Settled || source.Item.Item.Outcome != workexecution.Succeeded ||
		source.Item.Item.Input == nil || source.Item.Item.Result == nil || source.Item.Item.AttemptNumber == 0 ||
		source.Item.Item.ActiveAttemptID != "" || source.Item.Item.Operation != workexecution.Release ||
		source.Item.Item.TargetID != item.TargetID || source.Item.Item.Placement != item.Placement {
		return RestorationSource{}, errs.New(errs.KindStateConflict, "Group restoration source has not proven a successful settled Release")
	}
	_, attempt, err := repository.queue.ReadAttempt(ctx, task.ID, preparation.SourceWorkID, source.Item.Item.AttemptNumber, work.ReadRevision)
	if err != nil {
		return RestorationSource{}, err
	}
	terminal := attempt.Attempt
	if terminal.Direction != workexecution.Forward || terminal.Outcome != workexecution.Succeeded || terminal.FinishedAt == nil ||
		terminal.StartedAt == nil || terminal.Result == nil || *terminal.Result != *source.Item.Item.Result ||
		terminal.Input != *source.Item.Item.Input || terminal.Executor.HostID != item.Placement.HostID {
		return RestorationSource{}, errs.New(errs.KindStateConflict, "Group restoration source differs from its successful attempt receipt")
	}
	failed, err := repository.queue.ReadWork(ctx, task.ID, failedWorkID, work.ReadRevision)
	if err != nil {
		return RestorationSource{}, err
	}
	if failed.Item.Item.Operation != workexecution.Release || failed.Item.Item.Phase != workexecution.Settled ||
		failed.Item.Item.Outcome != workexecution.Failed || failed.Item.Item.Result == nil || failed.Item.Item.ActiveAttemptID != "" ||
		failed.Item.Item.TargetID != policy.Members[failedIndex].Member.ServiceID || failed.Item.Item.Placement != item.Placement ||
		failed.Item.Item.UpdatedAt.Before(*terminal.FinishedAt) {
		return RestorationSource{}, errs.New(errs.KindStateConflict, "Group restoration requires the captured later member's settled failure")
	}
	authority := releaseexecution.GroupRestorationAuthority{GroupID: task.TargetID, GroupOperationID: policy.Manifest.OperationID,
		GroupInput: task.Intent, MemberOrdinal: policy.Members[index].Member.Ordinal, FailedOrdinal: policy.Members[failedIndex].Member.Ordinal,
		FailedWorkID: failedWorkID, Failure: *failed.Item.Item.Result, FailedAt: failed.Item.Item.UpdatedAt, SourceInput: terminal.Input}
	return RestorationSource{Capture: capture, Preparation: preparation, Source: source, SourceAttempt: attempt, Failed: failed.Item,
		Authority: authority, Conditions: []keyvalue.Condition{
			{Key: ExecutionCaptureKey(task.ID), ModRevision: capture.Revision},
			{Key: workqueue.TaskKey(task.ID), ModRevision: work.Task.Revision},
			{Key: workqueue.ItemKey(item.ID), ModRevision: work.Item.Revision},
			{Key: workqueue.ItemKey(source.Item.Item.ID), ModRevision: source.Item.Revision},
			{Key: workqueue.AttemptKey(terminal.ID), ModRevision: attempt.Revision},
			{Key: workqueue.ItemKey(failedWorkID), ModRevision: failed.Item.Revision},
		}}, nil
}
