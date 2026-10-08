package softwarepreparation

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
)

const Target = etcd.SoftwarePreparationTarget

func newTask(now time.Time, idempotencyKey string, input preparation.Input) (etcd.TaskRecord, string, error) {
	canonical, hash, err := preparation.EncodeInput(input)
	if err != nil {
		return etcd.TaskRecord{}, "", err
	}
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: input.OperationID, PlanID: ids.New(ids.KindPlan),
		Owner: taskjournal.PlatformTaskOwner(), Actor: taskjournal.TaskActorOperator,
		Executor: taskjournal.TaskExecutorController, IdempotencyKey: idempotencyKey,
		PlanHash: hash, RenderGeneration: 1, Type: taskjournal.TaskPrepare, Target: Target,
		Params: map[string]string{
			taskjournal.TaskResourceKindParam:             taskjournal.TaskResourceSoftwarePreparation,
			taskjournal.TaskSoftwarePreparationInputParam: canonical,
		},
		TimeoutSeconds: preparation.TaskTimeoutSeconds, Status: taskjournal.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	return task, hash, etcd.ValidateTaskRecord(task)
}

func DecodeTask(task etcd.TaskRecord) (preparation.Input, error) {
	if err := etcd.ValidateTaskRecord(task); err != nil {
		return preparation.Input{}, err
	}
	return preparation.DecodeInput(
		task.Params[taskjournal.TaskSoftwarePreparationInputParam], task.PlanHash,
	)
}
