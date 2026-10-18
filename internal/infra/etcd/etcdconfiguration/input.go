// Package etcdconfiguration owns captured etcd activation input and receipts.
package etcdconfiguration

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const Timeout = 120 * time.Second

func TargetID() string {
	return ids.DeriveAt(ids.KindComponent, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), "groundplane-platform", "etcd-configuration")
}

type Input struct {
	TaskID           string    `json:"task_id"`
	WorkItemID       string    `json:"work_item_id"`
	PlanID           string    `json:"plan_id"`
	SelectedRevision string    `json:"selected_revision"`
	Candidate        string    `json:"candidate"`
	Previous         string    `json:"previous"`
	CreatedAt        time.Time `json:"created_at"`
}

func InputKey(workID string) string { return "/v1/execution/etcd-configuration/inputs/" + workID }
func ResultKey(attemptID string) string {
	return "/v1/execution/etcd-configuration/results/" + attemptID
}

func (input Input) Validate() error {
	if ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindWorkItem, input.WorkItemID) != nil ||
		ids.Validate(ids.KindPlan, input.PlanID) != nil || !recordcodec.IsCanonicalUTC(input.CreatedAt) ||
		len(input.SelectedRevision) == 0 || len(input.SelectedRevision) > 256 ||
		len(input.Candidate) == 0 || len(input.Candidate) > 8192 || len(input.Previous) == 0 || len(input.Previous) > 8192 {
		return errs.New(errs.KindValidationFailed, "etcd configuration capture is invalid")
	}
	return nil
}

func Encode(input Input) ([]byte, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	return recordcodec.Encode("etcd_configuration_input", input)
}

func Decode(value []byte) (Input, error) {
	input, err := recordcodec.Decode[Input](value, "etcd_configuration_input")
	if err != nil || input.Validate() != nil {
		return Input{}, errs.New(errs.KindInternal, "etcd configuration input is corrupt")
	}
	return input, nil
}

func (input Input) Reference() (workexecution.Input, error) {
	raw, err := Encode(input)
	if err != nil {
		return workexecution.Input{}, err
	}
	defer clear(raw)
	digest := sha256.Sum256(raw)
	return workexecution.Input{ID: input.PlanID, SHA256: hex.EncodeToString(digest[:])}, nil
}

func (input Input) Match(item workexecution.Item) error {
	reference, err := input.Reference()
	if err != nil {
		return err
	}
	if item.Operation != workexecution.EtcdConfiguration || item.TaskID != input.TaskID || item.ID != input.WorkItemID ||
		item.TargetID != TargetID() || item.Placement != (workexecution.Placement{Executor: workexecution.Controller}) ||
		item.Input == nil || *item.Input != reference || !item.CreatedAt.Equal(input.CreatedAt) ||
		len(item.Claims) != 1 || item.Claims[0] != (workexecution.Claim{ScopeID: TargetID()}) {
		return errs.New(errs.KindStateConflict, "etcd activation differs from its captured work")
	}
	return nil
}

func (input Input) Work() (workexecution.Task, workexecution.Item, error) {
	reference, err := input.Reference()
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, err
	}
	task := workexecution.Task{ID: input.TaskID, Actor: workexecution.OperatorActor, Action: workexecution.ActionApply,
		TargetID: TargetID(), TargetName: "etcd configuration", Intent: reference, ExpectedItems: 1,
		CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt, Deadline: input.CreatedAt.Add(Timeout)}
	item := workexecution.Item{ID: input.WorkItemID, TaskID: input.TaskID, Operation: workexecution.EtcdConfiguration,
		TargetID: task.TargetID, Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims: []workexecution.Claim{{ScopeID: task.TargetID}}, Input: &reference, Phase: workexecution.Ready,
		Cleanup: workexecution.CleanupNone, CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt, Deadline: task.Deadline}
	if err := workexecution.ValidateGraph(task, []workexecution.Item{item}); err != nil {
		return workexecution.Task{}, workexecution.Item{}, err
	}
	return task, item, nil
}

func PreparePublication(input Input) (workexecution.Task, workexecution.Item, []keyvalue.Condition, []keyvalue.Mutation, error) {
	task, item, err := input.Work()
	if err != nil {
		return task, item, nil, nil, err
	}
	raw, err := Encode(input)
	if err != nil {
		return task, item, nil, nil, err
	}
	return task, item, []keyvalue.Condition{{Key: InputKey(item.ID)}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: InputKey(item.ID), Value: raw}}, nil
}
