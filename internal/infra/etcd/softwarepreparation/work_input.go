package softwarepreparation

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Capture struct {
	TaskID     string            `json:"task_id"`
	WorkItemID string            `json:"work_item_id"`
	PlanID     string            `json:"plan_id"`
	Input      preparation.Input `json:"input"`
	CreatedAt  time.Time         `json:"created_at"`
}

func InputKey(workID string) string { return "/v1/execution/software-preparation/inputs/" + workID }
func ResultKey(attemptID string) string {
	return "/v1/execution/software-preparation/results/" + attemptID
}
func TargetID() string {
	return ids.DeriveAt(
		ids.KindComponent,
		time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		"groundplane-platform",
		"software-preparation",
	)
}

func (capture Capture) Reference() (workexecution.Input, error) {
	if ids.Validate(ids.KindTask, capture.TaskID) != nil || ids.Validate(ids.KindWorkItem, capture.WorkItemID) != nil ||
		ids.Validate(ids.KindPlan, capture.PlanID) != nil || !recordcodec.IsCanonicalUTC(capture.CreatedAt) {
		return workexecution.Input{}, errs.New(
			errs.KindValidationFailed,
			"software preparation capture identity is invalid",
		)
	}
	_, digest, err := preparation.EncodeInput(capture.Input)
	if err != nil {
		return workexecution.Input{}, err
	}
	return workexecution.Input{ID: capture.PlanID, SHA256: digest}, nil
}

func EncodeCapture(capture Capture) ([]byte, error) {
	if _, err := capture.Reference(); err != nil {
		return nil, err
	}
	return recordcodec.Encode("software_preparation_capture", capture)
}

func (capture Capture) Match(item workexecution.Item, record Record) error {
	reference, err := capture.Reference()
	if err != nil {
		return err
	}
	if item.Operation != workexecution.SoftwarePreparation || item.ID != capture.WorkItemID ||
		item.TaskID != capture.TaskID ||
		item.TargetID != TargetID() ||
		item.Input == nil ||
		*item.Input != reference ||
		item.Placement != (workexecution.Placement{Executor: workexecution.Controller}) ||
		len(item.Claims) != 1 ||
		item.Claims[0] != (workexecution.Claim{ScopeID: TargetID()}) ||
		record.TaskID != capture.TaskID ||
		record.OperationID != capture.Input.OperationID ||
		record.InputSHA256 != reference.SHA256 ||
		!record.Source.Equal(capture.Input.Source) ||
		!record.CreatedAt.Equal(capture.CreatedAt) {
		return errs.New(errs.KindStateConflict, "software preparation work differs from its captured input")
	}
	return nil
}

func (capture Capture) Work() (workexecution.Task, workexecution.Item, error) {
	reference, err := capture.Reference()
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, err
	}
	task := workexecution.Task{
		ID:            capture.TaskID,
		Actor:         workexecution.OperatorActor,
		Action:        workexecution.ActionPrepare,
		TargetID:      TargetID(),
		TargetName:    "Groundplane software",
		Intent:        reference,
		ExpectedItems: 1,
		CreatedAt:     capture.CreatedAt,
		UpdatedAt:     capture.CreatedAt,
		Deadline:      capture.CreatedAt.Add(time.Duration(preparation.TaskTimeoutSeconds) * time.Second),
	}
	item := workexecution.Item{
		ID:        capture.WorkItemID,
		TaskID:    task.ID,
		Operation: workexecution.SoftwarePreparation,
		TargetID:  task.TargetID,
		Placement: workexecution.Placement{
			Executor: workexecution.Controller,
		},
		Claims:    []workexecution.Claim{{ScopeID: task.TargetID}},
		Input:     &reference,
		Phase:     workexecution.Ready,
		Cleanup:   workexecution.CleanupNone,
		CreatedAt: task.CreatedAt,
		UpdatedAt: task.CreatedAt,
		Deadline:  task.Deadline,
	}
	return task, item, workexecution.ValidateGraph(task, []workexecution.Item{item})
}
