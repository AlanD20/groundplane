// Package environmentprovision owns captured Environment directory provisioning.
// Desired resource state remains in hierarchy, independently of Task retention.
package environmentprovision

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Input struct {
	TaskID      string                      `json:"task_id"`
	WorkItemID  string                      `json:"work_item_id"`
	PlanID      string                      `json:"plan_id"`
	Scope       workexecution.Scope         `json:"scope"`
	HostID      string                      `json:"host_id"`
	VolumeRoot  string                      `json:"volume_root"`
	Environment hierarchy.EnvironmentRecord `json:"environment"`
	CreatedAt   time.Time                   `json:"created_at"`
}

func InputKey(workID string) string { return "/v1/execution/environment-provision/inputs/" + workID }

func (input Input) Validate() error {
	if ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindWorkItem, input.WorkItemID) != nil ||
		ids.Validate(ids.KindPlan, input.PlanID) != nil || input.Scope.Validate() != nil || input.Scope.EnvironmentID == "" ||
		input.Scope.TenantID == "" || hierarchy.ValidateEnvironment(input.Environment) != nil ||
		input.Environment.ID != input.Scope.EnvironmentID || input.Environment.ProjectID != input.Scope.ProjectID ||
		input.Environment.CreateTaskID != input.TaskID || input.Environment.ProvisioningState != hierarchy.EnvironmentProvisioningProvisioning ||
		!recordcodec.IsCanonicalUTC(input.CreatedAt) || !input.CreatedAt.Equal(input.Environment.CreatedAt) ||
		ids.Validate(ids.KindHost, input.HostID) != nil {
		return errs.New(errs.KindValidationFailed, "Environment provisioning capture is invalid")
	}
	scope, err := environmentpath.Parse(input.VolumeRoot, input.Environment.VolumeDir)
	if err != nil {
		return err
	}
	if scope.ProjectKind != environmentpath.ProjectKindTenant || scope.TenantID != input.Scope.TenantID ||
		scope.ProjectID != input.Scope.ProjectID || scope.EnvironmentID != input.Scope.EnvironmentID {
		return errs.New(errs.KindValidationFailed, "Environment provisioning directory does not match its captured ownership")
	}
	return nil
}

func Encode(input Input) ([]byte, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	raw, err := recordcodec.Encode("environment_provision_input", input)
	if err != nil {
		return nil, err
	}
	if len(raw) > 8192 {
		return nil, errs.New(errs.KindValidationFailed, "Environment provisioning capture exceeds its storage bound")
	}
	return raw, nil
}

func Decode(raw []byte) (Input, error) {
	if len(raw) > 8192 {
		return Input{}, errs.New(errs.KindInternal, "Environment provisioning capture exceeds its storage bound")
	}
	input, err := recordcodec.Decode[Input](raw, "environment_provision_input")
	if err != nil || input.Validate() != nil {
		return Input{}, errs.New(errs.KindInternal, "Environment provisioning capture is corrupt")
	}
	return input, nil
}

func (input Input) Reference() (workexecution.Input, error) {
	raw, err := Encode(input)
	if err != nil {
		return workexecution.Input{}, err
	}
	digest := sha256.Sum256(raw)
	return workexecution.Input{ID: input.PlanID, SHA256: hex.EncodeToString(digest[:])}, nil
}

func (input Input) Work() (workexecution.Task, workexecution.Item, error) {
	reference, err := input.Reference()
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, err
	}
	deadline := input.CreatedAt.Add(120 * time.Second)
	task := workexecution.Task{ID: input.TaskID, Actor: workexecution.OperatorActor, Scope: input.Scope,
		Action: workexecution.ActionCreate, TargetID: input.Environment.ID, TargetName: input.Environment.Name,
		Intent: reference, ExpectedItems: 1, CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt, Deadline: deadline}
	item := workexecution.Item{ID: input.WorkItemID, TaskID: input.TaskID, Operation: workexecution.Environment,
		TargetID: input.Environment.ID, Placement: workexecution.Placement{Executor: workexecution.Agent, HostID: input.HostID},
		Input: &reference, Claims: []workexecution.Claim{{ScopeID: input.Environment.ID}},
		Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone, CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt, Deadline: deadline}
	return task, item, nil
}

func (input Input) Match(task workexecution.Task, item workexecution.Item) error {
	expectedTask, expectedItem, err := input.Work()
	if err != nil {
		return err
	}
	if task.ID != input.TaskID || task.Action != expectedTask.Action || task.Actor != expectedTask.Actor ||
		task.Scope != input.Scope || task.TargetID != expectedTask.TargetID || task.TargetName != expectedTask.TargetName ||
		task.Intent != expectedTask.Intent || task.ExpectedItems != 1 || !task.CreatedAt.Equal(input.CreatedAt) ||
		item.ID != input.WorkItemID || item.TaskID != input.TaskID || item.Operation != expectedItem.Operation || item.TargetID != input.Environment.ID ||
		item.Placement != expectedItem.Placement || item.Input == nil || *item.Input != expectedTask.Intent ||
		len(item.Dependencies) != 0 || len(item.Claims) != 1 || item.Claims[0] != expectedItem.Claims[0] || !item.CreatedAt.Equal(input.CreatedAt) {
		return errs.New(errs.KindStateConflict, "Environment provisioning work differs from its captured input")
	}
	return nil
}
