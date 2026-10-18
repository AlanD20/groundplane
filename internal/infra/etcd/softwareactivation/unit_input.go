package softwareactivation

import (
	"context"
	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func unitInputChange(view workqueue.View, record keyvalue.Versioned[Record], reference workexecution.Input, key string, raw []byte, dependencies []workqueue.ItemSnapshot) (workqueue.Change, error) {
	if record.Record.TaskID != view.Task.Task.ID || record.Revision <= 0 || record.ReadRevision != view.ReadRevision ||
		view.Task.Task.Action != workexecution.ActionActivate || view.Task.Task.TargetID != TargetID() || view.Task.Task.Stop != nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "software activation input publication has no current captured graph")
	}
	guards, err := workqueue.DependencyConditions(view.Item.Item, dependencies)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareInput(view.Task, view.Item, reference, time.Now().UTC())
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: Key(record.Record.TaskID), ModRevision: record.Revision}, keyvalue.Condition{Key: key})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: append([]byte(nil), raw...)})
	return change, nil
}

func (repository *Repository) PrepareControllerInput(ctx context.Context, view workqueue.View, record keyvalue.Versioned[Record], capture controllersoftware.Capture, dependencies []workqueue.ItemSnapshot) (workqueue.Change, error) {
	if capture.WorkItemID != record.Record.ControllerWorkItemID || capture.TaskID != record.Record.TaskID || record.Record.Progress.StagedController == nil ||
		capture.Input.Release != record.Record.Progress.StagedController.Release || capture.Input.Manifest != record.Record.Progress.StagedController.Manifest ||
		capture.Input.PreviousController != record.Record.Input.PreviousController || !sameAgent(capture.Input.Agent, record.Record.Input.Agent) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Controller unit input differs from captured staging")
	}
	if err := capture.Input.Validate(); err != nil {
		return workqueue.Change{}, err
	}
	reference, err := capture.Reference()
	if err != nil {
		return workqueue.Change{}, err
	}
	expected := view.Item.Item
	expected.Input = &reference
	if err := capture.Match(expected); err != nil {
		return workqueue.Change{}, err
	}
	raw, err := controllersoftware.EncodeCapture(capture)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(raw)
	change, err := unitInputChange(view, record, reference, controllersoftware.InputKey(capture.WorkItemID), raw, dependencies)
	if err != nil {
		return workqueue.Change{}, err
	}
	currentRecords, err := controllersoftware.New(repository.store)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	current, found, err := currentRecords.Current(ctx, view.ReadRevision)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: controllersoftware.LatestKey, ModRevision: current.Revision})
	if !found || !current.Record.Capture.CreatedAt.After(capture.CreatedAt) {
		value, err := controllersoftware.EncodeCurrent(controllersoftware.Current{Capture: capture})
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, err
		}
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: controllersoftware.LatestKey, Value: value})
	}
	return change, nil
}

func sameAgent(left, right *upgrade.AgentPredecessor) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func (repository *Repository) PrepareAgentInput(view workqueue.View, record keyvalue.Versioned[Record], capture agentoperations.Capture, dependencies []workqueue.ItemSnapshot) (workqueue.Change, error) {
	predecessor := record.Record.Input.Agent
	if predecessor == nil || record.Record.Input.Preparation.Progress.Result.Agent == nil || capture.WorkItemID != record.Record.AgentWorkItemID ||
		capture.TaskID != record.Record.TaskID || capture.Action != workexecution.ActionUpdate || capture.AgentID != predecessor.ID || capture.PreviousImage != predecessor.Image ||
		capture.StartingGeneration != predecessor.Generation || capture.Image != record.Record.Input.Preparation.Progress.Result.Agent.Artifact.Reference {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Agent unit input differs from captured staging")
	}
	reference, err := capture.Reference()
	if err != nil {
		return workqueue.Change{}, err
	}
	expected := view.Item.Item
	expected.Input = &reference
	if err := capture.Match(expected); err != nil {
		return workqueue.Change{}, err
	}
	raw, err := agentoperations.EncodeCapture(capture)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(raw)
	return unitInputChange(view, record, reference, agentoperations.InputKey(capture.WorkItemID), raw, dependencies)
}
