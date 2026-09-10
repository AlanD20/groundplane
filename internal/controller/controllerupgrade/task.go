package controllerupgrade

import (
	"encoding/json"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	Target     = "controller"
	InputParam = "controller_update_input"
)

// Input is the closed immutable native Task payload. No host paths or commands
// can be selected here; the release store and predecessor procedure own those.
type Input struct {
	Release            upgrade.Digest            `json:"release"`
	Manifest           upgrade.Manifest          `json:"manifest"`
	PreviousController upgrade.Digest            `json:"previous_controller"`
	Agent              *upgrade.AgentPredecessor `json:"agent"`
}

func (input Input) Validate() error {
	if !input.Release.Valid() || !input.PreviousController.Valid() ||
		input.Manifest.ControllerSHA256 == input.PreviousController {
		return errs.New(errs.KindValidationFailed, "controller update identities are invalid or unchanged")
	}
	return (upgrade.Release{Release: input.Release, Manifest: input.Manifest}).Validate()
}

func NewTask(now time.Time, idempotencyKey string, input Input) (etcd.TaskRecord, error) {
	if err := input.Validate(); err != nil {
		return etcd.TaskRecord{}, err
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return etcd.TaskRecord{}, errs.Wrap(errs.KindInternal, err)
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		Owner: etcd.PlatformTaskOwner(), Actor: etcd.TaskActorOperator,
		IdempotencyKey: idempotencyKey, Executor: etcd.TaskExecutorController,
		PlanID: ids.New(ids.KindPlan), PlanHash: string(upgrade.Hash(canonical))[7:], RenderGeneration: 1,
		Type: etcd.TaskUpdate, Target: Target, Params: map[string]string{
			etcd.TaskResourceKindParam: etcd.TaskResourceController, InputParam: string(canonical),
		},
		TimeoutSeconds: upgrade.TaskTimeoutSeconds, Status: etcd.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	if _, err := DecodeTask(task, now, now.Add(upgrade.TaskTimeoutSeconds*time.Second)); err != nil {
		return etcd.TaskRecord{}, err
	}
	return task, nil
}

func DecodeTask(task etcd.TaskRecord, started, deadline time.Time) (upgrade.Journal, error) {
	if task.Executor != etcd.TaskExecutorController || task.Type != etcd.TaskUpdate || task.Target != Target ||
		task.Owner != etcd.PlatformTaskOwner() || task.Actor != etcd.TaskActorOperator || ids.Validate(ids.KindTask, task.ID) != nil ||
		ids.Validate(ids.KindOperation, task.OperationID) != nil || ids.Validate(ids.KindPlan, task.PlanID) != nil ||
		task.TimeoutSeconds != upgrade.TaskTimeoutSeconds || task.RenderGeneration != 1 || task.RetryOf != "" || len(task.Steps) != 0 ||
		len(task.Materializations) != 0 ||
		len(task.Params) != 2 || task.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceController ||
		deadline.Sub(started) != upgrade.TaskTimeoutSeconds*time.Second {
		return upgrade.Journal{}, errs.New(errs.KindValidationFailed, "controller update Task authority is invalid")
	}
	raw := []byte(task.Params[InputParam])
	if len(raw) == 0 || len(raw) > 8192 || string(upgrade.Hash(raw))[7:] != task.PlanHash {
		return upgrade.Journal{}, errs.New(errs.KindValidationFailed, "controller update Task input hash is invalid")
	}
	input, err := jcs.Decode[Input](raw)
	if err != nil {
		return upgrade.Journal{}, err
	}
	if err := input.Validate(); err != nil {
		return upgrade.Journal{}, err
	}
	journal := upgrade.Journal{
		Schema: 1, TaskID: task.ID, Release: input.Release, Manifest: input.Manifest,
		PreviousController: input.PreviousController, Agent: input.Agent,
		StartedAt: started, Deadline: deadline, Phase: upgrade.PhasePrepared,
	}
	if err := journal.Validate(); err != nil {
		return upgrade.Journal{}, err
	}
	return journal, nil
}
