package releasegroups

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RemovalInput captures a definition-only change. It contains no executable
// command and never acquires Agent capacity or a host-effect claim.
type RemovalInput struct {
	TaskID        string              `json:"task_id"`
	WorkItemID    string              `json:"work_item_id"`
	PlanID        string              `json:"plan_id"`
	Scope         workexecution.Scope `json:"scope"`
	GroupID       string              `json:"group_id"`
	GroupName     string              `json:"group_name"`
	GroupRevision int64               `json:"group_revision"`
	CreatedAt     time.Time           `json:"created_at"`
}

func RemovalInputKey(workID string) string {
	return "/v1/execution/release-group-removal/inputs/" + workID
}

func (input RemovalInput) Validate() error {
	if ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindWorkItem, input.WorkItemID) != nil ||
		ids.Validate(ids.KindPlan, input.PlanID) != nil || ids.Validate(ids.KindReleaseGroup, input.GroupID) != nil ||
		input.Scope.Validate() != nil || input.Scope.EnvironmentID == "" || input.GroupName == "" || len(input.GroupName) > 256 ||
		input.GroupRevision <= 0 || !recordcodec.IsCanonicalUTC(input.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Release Group removal capture is invalid")
	}
	return nil
}

func EncodeRemovalInput(input RemovalInput) ([]byte, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	return recordcodec.Encode("release_group_removal_input", input)
}

func DecodeRemovalInput(value []byte) (RemovalInput, error) {
	if len(value) > 4096 {
		return RemovalInput{}, errs.New(errs.KindInternal, "Release Group removal input exceeds its storage bound")
	}
	input, err := recordcodec.Decode[RemovalInput](value, "release_group_removal_input")
	if err != nil || input.Validate() != nil {
		return RemovalInput{}, errs.New(errs.KindInternal, "Release Group removal input is corrupt")
	}
	return input, nil
}

func (input RemovalInput) Reference() (workexecution.Input, error) {
	value, err := EncodeRemovalInput(input)
	if err != nil {
		return workexecution.Input{}, err
	}
	digest := sha256.Sum256(value)
	return workexecution.Input{ID: input.PlanID, SHA256: hex.EncodeToString(digest[:])}, nil
}

func (input RemovalInput) Work() (workexecution.Task, workexecution.Item, error) {
	reference, err := input.Reference()
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, err
	}
	deadline := input.CreatedAt.Add(30 * time.Second)
	task := workexecution.Task{ID: input.TaskID, Actor: workexecution.OperatorActor, Scope: input.Scope,
		Action: workexecution.ActionRemove, TargetID: input.GroupID, TargetName: input.GroupName, Intent: reference,
		CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt, Deadline: deadline, ExpectedItems: 1}
	item := workexecution.Item{ID: input.WorkItemID, TaskID: input.TaskID, Operation: workexecution.ReleaseGroup,
		TargetID: input.GroupID, Placement: workexecution.Placement{Executor: workexecution.Controller},
		Input: &reference, Claims: []workexecution.Claim{{ScopeID: input.GroupID}}, Phase: workexecution.Ready,
		Cleanup: workexecution.CleanupNone, CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt, Deadline: deadline}
	return task, item, nil
}

func (input RemovalInput) Match(task workexecution.Task, item workexecution.Item) error {
	expectedTask, expectedItem, err := input.Work()
	if err != nil {
		return err
	}
	if task.ID != input.TaskID || task.Action != expectedTask.Action || task.Scope != input.Scope ||
		task.TargetID != input.GroupID || task.TargetName != input.GroupName || task.Intent != expectedTask.Intent ||
		task.ExpectedItems != 1 || !task.CreatedAt.Equal(input.CreatedAt) || !task.Deadline.Equal(expectedTask.Deadline) ||
		item.ID != input.WorkItemID || item.TaskID != input.TaskID || item.Operation != expectedItem.Operation ||
		item.TargetID != input.GroupID || item.Placement != expectedItem.Placement || item.Input == nil ||
		*item.Input != expectedTask.Intent || len(item.Dependencies) != 0 || len(item.Claims) != 1 || item.Claims[0] != expectedItem.Claims[0] ||
		!item.CreatedAt.Equal(input.CreatedAt) || !item.Deadline.Equal(expectedItem.Deadline) {
		return errs.New(errs.KindStateConflict, "Release Group removal differs from its captured input")
	}
	return nil
}
