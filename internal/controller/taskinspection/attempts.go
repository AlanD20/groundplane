package taskinspection

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Attempt struct {
	ID                    string
	Number                uint64
	Executor              workexecution.Executor
	HostID, AgentID       string
	Generation            uint64
	Direction             workexecution.Direction
	Outcome               workexecution.Outcome
	AssignedAt, Deadline  time.Time
	StartedAt, FinishedAt *time.Time
}

type AttemptPage struct {
	Attempts []Attempt
	More     bool
	After    uint64
	Revision int64
}

// Attempts exposes explicit execution identity/timing without copying captured
// executable input, secret material or arbitrary output into the read model.
func (service *Service) Attempts(ctx context.Context, taskID, workID string, after uint64, limit int64, revision int64) (AttemptPage, error) {
	page, err := service.queue.ListAttempts(ctx, taskID, workID, after, limit, revision)
	if err != nil {
		return AttemptPage{}, err
	}
	result := AttemptPage{Attempts: []Attempt{}, More: page.More, After: page.LastNumber, Revision: page.ReadRevision}
	for _, snapshot := range page.Attempts {
		attempt := snapshot.Attempt
		result.Attempts = append(result.Attempts, inspectAttempt(attempt))
	}
	return result, nil
}

func inspectAttempt(attempt workexecution.Attempt) Attempt {
	return Attempt{ID: attempt.ID, Number: attempt.Number,
		Executor: attempt.Executor.Executor, HostID: attempt.Executor.HostID, AgentID: attempt.Executor.AgentID,
		Generation: attempt.Executor.Generation, Direction: attempt.Direction, Outcome: attempt.Outcome,
		AssignedAt: attempt.AssignedAt, Deadline: attempt.Deadline, StartedAt: attempt.StartedAt, FinishedAt: attempt.FinishedAt}
}

func (service *Service) originalFailureAttempt(ctx context.Context, taskID string, item workexecution.Item, revision int64) (*Attempt, error) {
	if item.FirstFailureAttempt == 0 {
		return nil, nil
	}
	_, captured, err := service.queue.ReadAttempt(ctx, taskID, item.ID, item.FirstFailureAttempt, revision)
	if err != nil {
		return nil, err
	}
	attempt := captured.Attempt
	if attempt.Direction != workexecution.Forward || attempt.Outcome != workexecution.Failed ||
		attempt.Result == nil || item.FirstFailure == nil || *attempt.Result != *item.FirstFailure {
		return nil, errs.New(errs.KindStateConflict, "original failure differs from its captured execution attempt")
	}
	result := inspectAttempt(attempt)
	return &result, nil
}
