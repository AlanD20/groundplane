package app

import (
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func validTaskRecord(now time.Time) etcd.TaskRecord {
	// Generic journal fixtures carry no Script execution or source authority.
	// Script tests opt into TaskScript and publish those required companions.
	task := etcd.TaskRecord{
		ID: ids.NewAt(ids.KindTask, now, 1), OperationID: ids.NewAt(ids.KindOperation, now, 2),
		Owner: testtaskjournal.PlatformTaskOwner(), Actor: testtaskjournal.TaskActorOperator,
		Executor: testtaskjournal.TaskExecutorAgent, Type: testtaskjournal.TaskUpdate,
		Target: ids.NewAt(ids.KindService, now, 4), TimeoutSeconds: 120,
		Status: testtaskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	task.IdempotencyKey = "0123456789abcdef"
	task.PlanID = ids.NewAt(ids.KindPlan, now, 3)
	task.PlanHash = strings.Repeat("a", 64)
	task.RenderGeneration = 1
	task.Params = map[string]string{"name": "migrate"}
	task.Steps = []testtaskjournal.TaskStepRecord{{Kind: testtaskjournal.TaskStepOperation, ID: taskJournalStepID()}}
	return task
}
