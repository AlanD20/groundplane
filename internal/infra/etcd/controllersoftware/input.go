// Package controllersoftware owns captured native Controller activation input
// and receipts. Installed software and local handoff journals have other owners.
package controllersoftware

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Capture struct {
	TaskID     string        `json:"task_id"`
	WorkItemID string        `json:"work_item_id"`
	PlanID     string        `json:"plan_id"`
	Input      upgrade.Input `json:"input"`
	CreatedAt  time.Time     `json:"created_at"`
}

const LatestKey = "/v1/platform/controller-software/latest"

func InputKey(workID string) string { return "/v1/execution/controller-software/inputs/" + workID }
func ResultKey(attemptID string) string {
	return "/v1/execution/controller-software/results/" + attemptID
}
func TargetID() string {
	return ids.DeriveAt(ids.KindComponent, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), "groundplane-platform", "controller-software")
}

func EncodeCapture(capture Capture) ([]byte, error) {
	if ids.Validate(ids.KindTask, capture.TaskID) != nil || ids.Validate(ids.KindWorkItem, capture.WorkItemID) != nil ||
		ids.Validate(ids.KindPlan, capture.PlanID) != nil || !recordcodec.IsCanonicalUTC(capture.CreatedAt) {
		return nil, errs.New(errs.KindValidationFailed, "controller software capture identity is invalid")
	}
	if err := capture.Input.ValidateRecord(); err != nil {
		return nil, err
	}
	return recordcodec.Encode("controller_software_input", capture)
}

func (capture Capture) Reference() (workexecution.Input, error) {
	raw, err := EncodeCapture(capture)
	if err != nil {
		return workexecution.Input{}, err
	}
	defer clear(raw)
	digest := sha256.Sum256(raw)
	return workexecution.Input{ID: capture.PlanID, SHA256: hex.EncodeToString(digest[:])}, nil
}

func (capture Capture) Journal(start, deadline time.Time) (upgrade.Journal, error) {
	input := capture.Input
	journal := upgrade.Journal{Schema: 2, TaskID: capture.TaskID, Release: input.Release, Manifest: input.Manifest,
		PreviousController: input.PreviousController, Agent: input.Agent, StartedAt: start, Deadline: deadline, Phase: upgrade.PhasePrepared}
	return journal, journal.Validate()
}

func (capture Capture) Match(item workexecution.Item) error {
	reference, err := capture.Reference()
	if err != nil {
		return err
	}
	if item.Operation != workexecution.ControllerSoftware || item.ID != capture.WorkItemID || item.TaskID != capture.TaskID ||
		item.TargetID != TargetID() || item.Input == nil || *item.Input != reference ||
		item.Placement != (workexecution.Placement{Executor: workexecution.Controller}) ||
		len(item.Claims) != 1 || item.Claims[0] != (workexecution.Claim{ScopeID: TargetID()}) || !item.CreatedAt.Equal(capture.CreatedAt) {
		return errs.New(errs.KindStateConflict, "controller activation differs from its captured work")
	}
	return nil
}

func (capture Capture) Work() (workexecution.Task, workexecution.Item, error) {
	reference, err := capture.Reference()
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, err
	}
	if err := capture.Input.Validate(); err != nil {
		return workexecution.Task{}, workexecution.Item{}, err
	}
	task := workexecution.Task{ID: capture.TaskID, Actor: workexecution.OperatorActor, Action: workexecution.ActionUpdate,
		TargetID: TargetID(), TargetName: "Controller", Intent: reference, ExpectedItems: 1, CreatedAt: capture.CreatedAt,
		UpdatedAt: capture.CreatedAt, Deadline: capture.CreatedAt.Add(upgrade.TaskTimeoutSeconds * time.Second)}
	item := workexecution.Item{ID: capture.WorkItemID, TaskID: task.ID, Operation: workexecution.ControllerSoftware, TargetID: task.TargetID,
		Placement: workexecution.Placement{Executor: workexecution.Controller}, Claims: []workexecution.Claim{{ScopeID: task.TargetID}},
		Input: &reference, Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline}
	return task, item, workexecution.ValidateGraph(task, []workexecution.Item{item})
}
