// Package agentoperations owns captured Controller-managed Agent lifecycle
// work and its settlement receipts, not the Agent's installed resource record.
package agentoperations

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Enrollment struct {
	PullIntervalSeconds int32             `json:"pull_interval_seconds"`
	MaxConcurrentTasks  int32             `json:"max_concurrent_tasks"`
	Labels              map[string]string `json:"labels,omitempty"`
}

type Capture struct {
	TaskID             string               `json:"task_id"`
	WorkItemID         string               `json:"work_item_id"`
	PlanID             string               `json:"plan_id"`
	AgentID            string               `json:"agent_id"`
	Action             workexecution.Action `json:"action"`
	Image              string               `json:"image"`
	PreviousImage      string               `json:"previous_image,omitempty"`
	StartingGeneration uint64               `json:"starting_generation,omitempty"`
	Enrollment         *Enrollment          `json:"enrollment,omitempty"`
	CreatedAt          time.Time            `json:"created_at"`
}

func InputKey(workID string) string     { return "/v1/execution/agent-management/inputs/" + workID }
func ResultKey(attemptID string) string { return "/v1/execution/agent-management/results/" + attemptID }

// A singleton claim serializes enrollment, replacement and removal, including
// enrollment attempts with different newly allocated Agent IDs.
func ClaimID() string {
	return ids.DeriveAt(
		ids.KindComponent,
		time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		"groundplane-platform",
		"agent-lifecycle",
	)
}

func EncodeCapture(capture Capture) ([]byte, error) {
	if ids.Validate(ids.KindTask, capture.TaskID) != nil || ids.Validate(ids.KindWorkItem, capture.WorkItemID) != nil ||
		ids.Validate(ids.KindPlan, capture.PlanID) != nil || ids.Validate(ids.KindAgent, capture.AgentID) != nil ||
		!recordcodec.IsCanonicalUTC(capture.CreatedAt) || !imageref.IsDigestPinned(capture.Image) {
		return nil, errs.New(errs.KindValidationFailed, "Agent lifecycle captured identity or image is invalid")
	}
	switch capture.Action {
	case workexecution.ActionCreate:
		if capture.Enrollment == nil || capture.StartingGeneration != 0 || capture.PreviousImage != "" {
			return nil, errs.New(errs.KindValidationFailed, "Agent enrollment capture is invalid")
		}
		config := capture.Enrollment
		if err := localagents.ValidateLocalAgentConfig(localagents.LocalAgentConfig{PullIntervalSeconds: config.PullIntervalSeconds,
			MaxConcurrentTasks: config.MaxConcurrentTasks, Labels: config.Labels}); err != nil {
			return nil, err
		}
	case workexecution.ActionUpdate:
		if capture.Enrollment != nil || capture.StartingGeneration == 0 || capture.StartingGeneration > ^uint64(0)-2 ||
			!imageref.IsDigestPinned(capture.PreviousImage) || capture.PreviousImage == capture.Image {
			return nil, errs.New(errs.KindValidationFailed, "Agent replacement capture is invalid")
		}
	case workexecution.ActionRemove:
		if capture.Enrollment != nil || capture.StartingGeneration == 0 || capture.PreviousImage != "" {
			return nil, errs.New(errs.KindValidationFailed, "Agent removal capture is invalid")
		}
	default:
		return nil, errs.New(errs.KindValidationFailed, "Agent lifecycle action is invalid")
	}
	return recordcodec.Encode("agent_lifecycle_input", capture)
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

func (capture Capture) Operation() workexecution.Operation {
	if capture.Action == workexecution.ActionUpdate {
		return workexecution.AgentSoftware
	}
	return workexecution.AgentLifecycle
}

func (capture Capture) Match(item workexecution.Item) error {
	reference, err := capture.Reference()
	if err != nil {
		return err
	}
	if item.Operation != capture.Operation() || item.ID != capture.WorkItemID || item.TaskID != capture.TaskID ||
		item.TargetID != capture.AgentID || item.Input == nil || *item.Input != reference ||
		item.Placement != (workexecution.Placement{Executor: workexecution.Controller}) || len(item.Claims) != 1 ||
		item.Claims[0] != (workexecution.Claim{ScopeID: ClaimID()}) || !item.CreatedAt.Equal(capture.CreatedAt) {
		return errs.New(errs.KindStateConflict, "Agent lifecycle work differs from its captured input")
	}
	return nil
}

func (capture Capture) Work() (workexecution.Task, workexecution.Item, error) {
	reference, err := capture.Reference()
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, err
	}
	timeout := 120 * time.Second
	if capture.Action == workexecution.ActionUpdate {
		timeout = 300 * time.Second
	}
	task := workexecution.Task{ID: capture.TaskID, Actor: workexecution.OperatorActor, Action: capture.Action,
		TargetID: capture.AgentID, TargetName: "Agent", Intent: reference, ExpectedItems: 1, CreatedAt: capture.CreatedAt,
		UpdatedAt: capture.CreatedAt, Deadline: capture.CreatedAt.Add(timeout)}
	item := workexecution.Item{
		ID:        capture.WorkItemID,
		TaskID:    task.ID,
		Operation: capture.Operation(),
		TargetID:  capture.AgentID,
		Placement: workexecution.Placement{
			Executor: workexecution.Controller,
		},
		Claims:    []workexecution.Claim{{ScopeID: ClaimID()}},
		Input:     &reference,
		Phase:     workexecution.Ready,
		Cleanup:   workexecution.CleanupNone,
		CreatedAt: task.CreatedAt,
		UpdatedAt: task.CreatedAt,
		Deadline:  task.Deadline,
	}
	return task, item, workexecution.ValidateGraph(task, []workexecution.Item{item})
}
