package taskinspection

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
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
		result.Attempts = append(result.Attempts, Attempt{ID: attempt.ID, Number: attempt.Number,
			Executor: attempt.Executor.Executor, HostID: attempt.Executor.HostID, AgentID: attempt.Executor.AgentID,
			Generation: attempt.Executor.Generation, Direction: attempt.Direction, Outcome: attempt.Outcome,
			AssignedAt: attempt.AssignedAt, Deadline: attempt.Deadline, StartedAt: attempt.StartedAt, FinishedAt: attempt.FinishedAt})
	}
	return result, nil
}
