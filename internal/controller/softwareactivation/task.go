package softwareactivation

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	activationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

const Target = etcd.SoftwareActivationTarget

func newTask(now time.Time, idempotencyKey string, input activationrecord.Input) (etcd.TaskRecord, string, error) {
	canonical, hash, err := activationrecord.EncodeInput(input)
	if err != nil {
		return etcd.TaskRecord{}, "", err
	}
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: input.OperationID, PlanID: ids.New(ids.KindPlan),
		Owner: taskjournal.PlatformTaskOwner(), Actor: taskjournal.TaskActorOperator,
		Executor: taskjournal.TaskExecutorSoftware, IdempotencyKey: idempotencyKey,
		PlanHash: hash, RenderGeneration: 1, Type: taskjournal.TaskApply, Target: Target,
		Params: map[string]string{
			taskjournal.TaskResourceKindParam:            taskjournal.TaskResourceSoftware,
			taskjournal.TaskSoftwareActivationInputParam: canonical,
		},
		TimeoutSeconds: activationrecord.TaskTimeoutSeconds, Status: taskjournal.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	return task, hash, etcd.ValidateTaskRecord(task)
}

func DecodeTask(task etcd.TaskRecord) (activationrecord.Input, error) {
	if err := etcd.ValidateTaskRecord(task); err != nil {
		return activationrecord.Input{}, err
	}
	return activationrecord.DecodeInput(task.Params[taskjournal.TaskSoftwareActivationInputParam], task.PlanHash)
}
