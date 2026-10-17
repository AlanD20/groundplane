package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadCompletion captures assignment, slot and resource ownership at one MVCC
// revision. Recorded means the same attempt already settled; it is an
// acknowledgement, not permission to execute it again.
func (repository *Repository) ReadCompletion(ctx context.Context, taskID, workID, attemptID string,
	identity workexecution.Identity,
) (request Completion, recorded bool, err error) {
	view, err := repository.ReadResume(ctx, taskID, workID, attemptID, identity)
	if err != nil {
		return Completion{}, false, err
	}
	if view.Attempt.Attempt.FinishedAt != nil {
		return Completion{}, true, nil
	}
	request, err = repository.ReadCompletionAt(ctx, view)
	return request, false, err
}

// ReadCompletionAt keeps operation proof, worker ownership and resource claims
// on the caller's fixed snapshot. It grants no execution or independent commit.
func (repository *Repository) ReadCompletionAt(ctx context.Context, view ResumeView) (Completion, error) {
	if !view.ActiveAttempt || !view.CurrentExecutor || view.Work.ReadRevision <= 0 ||
		view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 ||
		view.Attempt.Attempt.FinishedAt != nil || workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return Completion{}, errs.New(errs.KindStateConflict, "execution attempt no longer owns this work")
	}
	return repository.readCompletionOwnership(ctx, view)
}

// ReadReleaseBeforeEffectsAt is restricted to a replaced forward Release with
// no checkpoint grant. Release execution requires that grant before any effect;
// other operation owners must supply their own proof, not reuse this shortcut.
func (repository *Repository) ReadReleaseBeforeEffectsAt(ctx context.Context, view ResumeView) (Completion, error) {
	if !view.ActiveAttempt || view.CurrentExecutor || view.ExecutorRevision <= 0 || view.Work.ReadRevision <= 0 ||
		view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 ||
		view.Work.Item.Item.Operation != workexecution.Release || view.Attempt.Attempt.FinishedAt != nil ||
		view.Attempt.Attempt.Direction != workexecution.Forward || view.Checkpoint.Record != nil || view.Checkpoint.Revision != 0 ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return Completion{}, errs.New(errs.KindStateConflict, "interrupted Release may already have authorized effects")
	}
	return repository.readCompletionOwnership(ctx, view)
}

// ReadInterruptedReleaseAt captures ownership for recording an interruption.
// It proves no external cleanup; callers must retain all unresolved claims.
func (repository *Repository) ReadInterruptedReleaseAt(ctx context.Context, view ResumeView) (Completion, error) {
	if !view.ActiveAttempt || view.CurrentExecutor || view.ExecutorRevision <= 0 || view.Work.ReadRevision <= 0 ||
		view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 ||
		view.Work.Item.Item.Operation != workexecution.Release || view.Attempt.Attempt.FinishedAt != nil ||
		view.Attempt.Attempt.Direction != workexecution.Forward || view.Checkpoint.Record == nil ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return Completion{}, errs.New(errs.KindStateConflict, "Release interruption requires a replaced forward attempt with retained effects")
	}
	return repository.readCompletionOwnership(ctx, view)
}

// ReadInterruptedRecoveryAt captures ownership only. The caller must verify
// every restoration proof before releasing these claims after replacement.
func (repository *Repository) ReadInterruptedRecoveryAt(ctx context.Context, view ResumeView) (Completion, error) {
	if !view.ActiveAttempt || view.CurrentExecutor || view.ExecutorRevision <= 0 || view.Work.ReadRevision <= 0 ||
		view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 ||
		view.Work.Item.Item.Operation != workexecution.Release || view.Attempt.Attempt.FinishedAt != nil ||
		view.Attempt.Attempt.Direction != workexecution.Recovery || (view.Checkpoint.Record == nil) != (view.Checkpoint.Revision == 0) ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return Completion{}, errs.New(errs.KindStateConflict, "Recovery reconciliation requires its replaced attempt and retained claims")
	}
	return repository.readCompletionOwnership(ctx, view)
}

// ReadInterruptedScriptAt captures retained ownership without granting effects.
// The Script owner must prove cleanup or absence of any command authorization.
func (repository *Repository) ReadInterruptedScriptAt(ctx context.Context, view ResumeView) (Completion, error) {
	if !view.ActiveAttempt || view.CurrentExecutor || view.ExecutorRevision <= 0 || view.Work.ReadRevision <= 0 ||
		view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 ||
		view.Work.Item.Item.Operation != workexecution.Script || view.Attempt.Attempt.FinishedAt != nil ||
		view.Attempt.Attempt.Direction != workexecution.Forward ||
		(view.Checkpoint.Record == nil) != (view.Checkpoint.Revision == 0) ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return Completion{}, errs.New(errs.KindStateConflict, "Script reconciliation requires its replaced attempt and retained claims")
	}
	return repository.readCompletionOwnership(ctx, view)
}

// Interrupted Script cleanup never proves absence. Its owner can finish the
// attempt with retained claims, allowing an explicit cleanup retry.
func (repository *Repository) ReadInterruptedScriptRecoveryAt(ctx context.Context, view ResumeView) (Completion, error) {
	if !view.ActiveAttempt || view.CurrentExecutor || view.ExecutorRevision <= 0 || view.Work.ReadRevision <= 0 ||
		view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 ||
		view.Work.Item.Item.Operation != workexecution.Script || view.Attempt.Attempt.FinishedAt != nil ||
		view.Attempt.Attempt.Direction != workexecution.Recovery || view.Checkpoint.Record != nil || view.Checkpoint.Revision != 0 ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return Completion{}, errs.New(errs.KindStateConflict, "Script cleanup interruption requires its replaced attempt and retained claims")
	}
	return repository.readCompletionOwnership(ctx, view)
}

// ReadInterruptedHierarchyAt captures the replaced executor's slot and claims
// for operation-owned reconciliation. It cannot authorize another host effect.
func (repository *Repository) ReadInterruptedHierarchyAt(ctx context.Context, view ResumeView) (Completion, error) {
	if !view.ActiveAttempt || view.CurrentExecutor || view.ExecutorRevision <= 0 || view.Work.ReadRevision <= 0 ||
		view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 ||
		view.Work.Item.Item.Operation != workexecution.Hierarchy || view.Attempt.Attempt.FinishedAt != nil ||
		(view.Checkpoint.Record == nil) != (view.Checkpoint.Revision == 0) ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return Completion{}, errs.New(errs.KindStateConflict, "removal reconciliation requires its replaced attempt and retained claims")
	}
	return repository.readCompletionOwnership(ctx, view)
}

// ReadInterruptedVolumeAt captures a replaced attempt's retained ownership. The
// Volume owner must supply completion proof or preserve unknown-effect claims.
func (repository *Repository) ReadInterruptedVolumeAt(ctx context.Context, view ResumeView) (Completion, error) {
	if !view.ActiveAttempt || view.CurrentExecutor || view.ExecutorRevision <= 0 || view.Work.ReadRevision <= 0 ||
		view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 ||
		view.Work.Item.Item.Operation != workexecution.Volume || view.Attempt.Attempt.FinishedAt != nil ||
		(view.Checkpoint.Record == nil) != (view.Checkpoint.Revision == 0) ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return Completion{}, errs.New(errs.KindStateConflict, "Volume reconciliation requires its replaced attempt and retained claims")
	}
	return repository.readCompletionOwnership(ctx, view)
}

func (repository *Repository) readCompletionOwnership(ctx context.Context, view ResumeView) (Completion, error) {
	request := Completion{Task: view.Work.Task, Item: view.Work.Item, Attempt: view.Attempt}
	taskID, workID, attemptID := request.Task.Task.ID, request.Item.Item.ID, request.Attempt.Attempt.ID
	identity := request.Attempt.Attempt.Executor
	item := request.Item.Item
	keys := make([]string, 0, maximumExecutorCapacity+len(item.Claims))
	for index := uint16(0); index < maximumExecutorCapacity; index++ {
		keys = append(keys, SlotKey(identity, index))
	}
	for _, claim := range item.Claims {
		keys = append(keys, ClaimKey(claim))
	}
	read, err := repository.read(ctx, keys, view.Work.ReadRevision)
	if err != nil {
		return Completion{}, err
	}
	for index, value := range read.Values[:maximumExecutorCapacity] {
		if value == nil {
			continue
		}
		slot, err := recordcodec.Decode[Slot](value.Value, "execution_slot")
		if err != nil {
			return Completion{}, err
		}
		if slot.WorkItemID != workID {
			continue
		}
		if slot.TaskID != taskID || slot.AttemptID != attemptID || request.Slot.Revision != 0 {
			return Completion{}, corrupt()
		}
		request.Slot = SlotSnapshot{Index: uint16(index), Record: slot, Revision: value.ModRevision}
	}
	if request.Slot.Revision == 0 {
		return Completion{}, errs.New(errs.KindStateConflict, "execution worker reservation is missing")
	}
	for index, claim := range item.Claims {
		value := read.Values[maximumExecutorCapacity+index]
		if value == nil {
			return Completion{}, errs.New(errs.KindStateConflict, "execution resource protection is missing")
		}
		owner, err := recordcodec.Decode[ClaimOwner](value.Value, "execution_claim")
		if err != nil || owner.TaskID != taskID || owner.WorkItemID != workID {
			return Completion{}, corrupt()
		}
		request.Claims = append(request.Claims, ClaimSnapshot{Claim: claim, Owner: &owner, Revision: value.ModRevision})
	}
	return request, nil
}
