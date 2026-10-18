package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"
)

func (repository *TaskRepository) prepareRemovalTaskAcknowledgement(
	ctx context.Context, task TaskRecord, terminalStatus taskjournal.TaskStatus, terminalAt time.Time, revision int64,
	assignment taskassignments.TaskAssignmentRecord,
) (removalTaskChange, error) {
	return repository.prepareServiceRemovalTaskAcknowledgement(
		ctx,
		task,
		terminalStatus,
		terminalAt,
		revision,
		assignment,
	)
}

func (repository *TaskRepository) validateRemovalTaskAcknowledgementReplay(
	ctx context.Context, task TaskRecord, terminalStatus taskjournal.TaskStatus, revision int64,
) error {
	return repository.validateServiceRemovalTaskAcknowledgementReplay(ctx, task, terminalStatus, revision)
}
