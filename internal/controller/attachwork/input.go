// Package attachwork owns native Attach provisioning under its visible Task.
package attachwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Inputs struct {
	repository *attachprovision.InputRepository
	planner    *attachplan.ProvisionPlanner
}

func NewInputs(repository *attachprovision.InputRepository, planner *attachplan.ProvisionPlanner) (*Inputs, error) {
	if repository == nil || planner == nil {
		return nil, errs.New(errs.KindInternal, "Attach execution requires captured inputs and provisioning resolution")
	}
	return &Inputs{repository: repository, planner: planner}, nil
}

// Forward reconstructs the same protected payload for delivery or effect
// inspection. Reading it does not grant a command start or revive revoked work.
func (owner *Inputs) Forward(ctx context.Context, view workqueue.ResumeView) (attachexecution.ProvisionInput, error) {
	attempt, item := view.Attempt.Attempt, view.Work.Item.Item
	if owner == nil || owner.repository == nil || owner.planner == nil ||
		item.Operation != workexecution.Attach || attempt.Direction != workexecution.Forward ||
		attempt.Validate() != nil || attempt.TaskID != view.Work.Task.Task.ID ||
		attempt.WorkItemID != item.ID || item.Input == nil || attempt.Input != *item.Input ||
		attempt.Executor.Executor != workexecution.Agent || attempt.Executor.HostID != item.Placement.HostID {
		return attachexecution.ProvisionInput{}, errs.New(
			errs.KindStateConflict,
			"Attach attempt does not own this captured input",
		)
	}
	source, err := owner.repository.LoadCaptured(ctx, view.Work)
	if err != nil {
		return attachexecution.ProvisionInput{}, err
	}
	defer source.Clear()
	payload, err := owner.planner.BuildCaptured(ctx, source.Generation.Record, view.Work.Task.Task, item)
	if err != nil {
		return attachexecution.ProvisionInput{}, err
	}
	input := attachexecution.ProvisionInput{Attempt: attempt, Provision: payload}
	if err := input.Validate(); err != nil {
		attachexecution.ClearProvision(payload)
		return attachexecution.ProvisionInput{}, err
	}
	return input, nil
}

func (owner *Inputs) LoadWorkAssignment(
	ctx context.Context,
	view workqueue.ResumeView,
) (*agentpb.AttachProvisionAssignment, error) {
	task := view.Work.Task.Task
	if !view.CurrentExecutor || !view.ActiveAttempt || task.FinishedAt != nil ||
		task.CollectionStartedAt != nil || !task.PublicationComplete ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return nil, errs.New(errs.KindStateConflict, "Attach delivery no longer owns active execution")
	}
	input, err := owner.Forward(ctx, view)
	if err != nil {
		return nil, err
	}
	defer attachexecution.ClearProvision(input.Provision)
	return attachexecution.EncodeProvisionAssignment(input)
}
