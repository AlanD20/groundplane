package softwareactivation

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TargetID() string {
	return ids.DeriveAt(ids.KindComponent, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), "groundplane-platform", "software-activation")
}

func (record Record) Graph() (workexecution.Task, []workexecution.Item, error) {
	if err := ValidateRecord(record); err != nil {
		return workexecution.Task{}, nil, err
	}
	reference, err := record.Reference()
	if err != nil {
		return workexecution.Task{}, nil, err
	}
	task := workexecution.Task{ID: record.TaskID, Actor: workexecution.OperatorActor, Action: workexecution.ActionActivate, TargetID: TargetID(),
		TargetName: "Groundplane software", Intent: reference, CreatedAt: record.CreatedAt, UpdatedAt: record.CreatedAt,
		Deadline: record.CreatedAt.Add(time.Duration(TaskTimeoutSeconds) * time.Second), ExpectedItems: 1}
	stage := workexecution.Item{ID: record.WorkItemID, TaskID: task.ID, Operation: workexecution.SoftwareStaging, TargetID: TargetID(),
		Placement: workexecution.Placement{Executor: workexecution.Controller}, Claims: []workexecution.Claim{{ScopeID: TargetID()}},
		Input: &reference, Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline}
	items := []workexecution.Item{stage}
	dependency := stage.ID
	if record.ControllerWorkItemID != "" {
		item := stage
		item.ID, item.Operation, item.TargetID = record.ControllerWorkItemID, workexecution.ControllerSoftware, controllersoftware.TargetID()
		item.Claims, item.Input, item.Phase, item.Wait = controllersoftware.Claims(), nil, workexecution.Waiting, &workexecution.Wait{Reason: workexecution.WaitInput}
		item.Dependencies = []string{stage.ID}
		items = append(items, item)
		dependency = item.ID
	}
	if record.AgentWorkItemID != "" {
		item := stage
		item.ID, item.Operation, item.TargetID = record.AgentWorkItemID, workexecution.AgentSoftware, record.Input.Agent.ID
		item.Claims, item.Input, item.Phase, item.Wait = []workexecution.Claim{{ScopeID: agentoperations.ClaimID()}}, nil, workexecution.Waiting, &workexecution.Wait{Reason: workexecution.WaitInput}
		item.Dependencies = []string{dependency}
		items = append(items, item)
	}
	task.ExpectedItems = uint32(len(items))
	return task, items, workexecution.ValidateGraph(task, items)
}

func (record Record) MatchStage(item workexecution.Item) error {
	_, items, err := record.Graph()
	if err != nil {
		return err
	}
	expected := items[0]
	if item.ID != expected.ID || item.TaskID != expected.TaskID || item.Operation != expected.Operation || item.TargetID != expected.TargetID ||
		item.Placement != expected.Placement || len(item.Claims) != 1 || item.Claims[0] != expected.Claims[0] || len(item.Dependencies) != 0 ||
		item.Input == nil || *item.Input != *expected.Input || !item.CreatedAt.Equal(expected.CreatedAt) {
		return errs.New(errs.KindStateConflict, "software staging work differs from the captured activation")
	}
	return nil
}
