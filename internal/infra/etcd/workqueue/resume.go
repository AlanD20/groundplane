package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResumeView exposes saved execution state, not permission to repeat effects.
// Reports from a replaced process may still account for already-authorized
// effects, but cannot authorize another stage or move the assignment to a host.
type ResumeView struct {
	Work             View
	Attempt          AttemptSnapshot
	Checkpoint       CheckpointSnapshot
	CurrentExecutor  bool
	ExecutorRevision int64
	ActiveAttempt    bool
}

func (repository *Repository) ReadResume(ctx context.Context, taskID, workID, attemptID string,
	identity workexecution.Identity,
) (ResumeView, error) {
	return repository.ReadResumeAt(ctx, taskID, workID, attemptID, identity, 0)
}

// ReadResumeAt joins operation-owned ledgers to execution state at one MVCC
// revision. Zero selects a fresh snapshot, as for ordinary channel reports.
func (repository *Repository) ReadResumeAt(ctx context.Context, taskID, workID, attemptID string,
	identity workexecution.Identity, revision int64,
) (ResumeView, error) {
	if err := identity.Validate(); err != nil {
		return ResumeView{}, err
	}
	if ids.Validate(ids.KindAttempt, attemptID) != nil {
		return ResumeView{}, invalid("execution resume identity is invalid")
	}
	work, err := repository.ReadWork(ctx, taskID, workID, revision)
	if err != nil {
		return ResumeView{}, err
	}
	read, err := repository.read(ctx, []string{AttemptKey(attemptID), CheckpointKey(attemptID), ExecutorKey(identity)}, work.ReadRevision)
	if err != nil {
		return ResumeView{}, err
	}
	if read.Values[0] == nil {
		return ResumeView{}, errs.New(errs.KindStateConflict, "execution attempt was not found")
	}
	attempt, err := DecodeAttempt(read.Values[0].Value)
	if err != nil {
		return ResumeView{}, err
	}
	if attempt.ID != attemptID || attempt.TaskID != taskID || attempt.WorkItemID != workID {
		return ResumeView{}, corrupt()
	}
	if attempt.Executor != identity {
		return ResumeView{}, errs.New(errs.KindStateConflict, "execution attempt belongs to a different executor")
	}
	view := ResumeView{
		Work:          work,
		Attempt:       AttemptSnapshot{Attempt: attempt, Revision: read.Values[0].ModRevision},
		ActiveAttempt: work.Item.Item.ActiveAttemptID == attemptID,
	}
	if view.ActiveAttempt && workexecution.ValidateDelivery(work.Item.Item, attempt) != nil {
		return ResumeView{}, corrupt()
	}
	if read.Values[1] != nil {
		checkpoint, err := DecodeCheckpoint(read.Values[1].Value)
		if err != nil {
			return ResumeView{}, err
		}
		if checkpoint.AttemptID != attemptID || attempt.StartedAt == nil || checkpoint.RecordedAt.Before(*attempt.StartedAt) ||
			attempt.FinishedAt != nil && checkpoint.RecordedAt.After(*attempt.FinishedAt) {
			return ResumeView{}, corrupt()
		}
		view.Checkpoint = CheckpointSnapshot{Record: &checkpoint, Revision: read.Values[1].ModRevision}
	}
	if read.Values[2] != nil {
		executor, err := DecodeExecutor(read.Values[2].Value)
		if err != nil {
			return ResumeView{}, err
		}
		if executor.Identity.Executor != identity.Executor || executor.Identity.HostID != identity.HostID {
			return ResumeView{}, corrupt()
		}
		view.CurrentExecutor = executor.Identity == identity
		view.ExecutorRevision = read.Values[2].ModRevision
	}
	return view, nil
}
