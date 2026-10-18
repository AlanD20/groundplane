package agentmanagement

import (
	"context"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type assignmentAdmission interface {
	PauseAssignments(context.Context, string, uint64) (func(), error)
}

type activeLifecycle struct {
	attemptID string
	cancel    context.CancelFunc
	stopped   bool
}

// Work owns one complete typed lifecycle. Durable resource records describe
// actual installation; captured native work owns the decision and its recovery.
type Work struct {
	queue         *workqueue.Repository
	inputs        *agentoperations.Repository
	registrations *agentregistration.Repository
	agents        *localagent.Manager
	sessions      assignmentAdmission
	diagnostics   *executiondiagnostics.Repository
	mu            sync.Mutex
	active        map[string]*activeLifecycle
}

func NewWork(queue *workqueue.Repository, inputs *agentoperations.Repository, registrations *agentregistration.Repository,
	agents *localagent.Manager, sessions assignmentAdmission, diagnostics *executiondiagnostics.Repository,
) (*Work, error) {
	if queue == nil || inputs == nil || registrations == nil || agents == nil || sessions == nil || diagnostics == nil {
		return nil, errs.New(errs.KindInternal, "Agent lifecycle work dependencies are incomplete")
	}
	return &Work{queue: queue, inputs: inputs, registrations: registrations, agents: agents, sessions: sessions,
		diagnostics: diagnostics, active: make(map[string]*activeLifecycle)}, nil
}

func inputGuard(input keyvalue.Versioned[agentoperations.Capture]) workqueue.Change {
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: agentoperations.InputKey(input.Record.WorkItemID), ModRevision: input.Revision}}}
}

func (owner *Work) loadDelivery(ctx context.Context, delivery workqueue.Delivery) (keyvalue.Versioned[agentoperations.Capture], workqueue.ResumeView, error) {
	attempt := delivery.Attempt.Attempt
	if workexecution.ValidateDelivery(delivery.Item.Item, attempt) != nil || attempt.Executor.Executor != workexecution.Controller {
		return keyvalue.Versioned[agentoperations.Capture]{}, workqueue.ResumeView{}, errs.New(errs.KindStateConflict, "Agent lifecycle delivery is invalid")
	}
	view, err := owner.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return keyvalue.Versioned[agentoperations.Capture]{}, view, err
	}
	if !view.ActiveAttempt || view.Attempt.Attempt.FinishedAt != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
		return keyvalue.Versioned[agentoperations.Capture]{}, view, errs.New(errs.KindStateConflict, "Agent lifecycle attempt no longer owns execution")
	}
	input, err := owner.inputs.Load(ctx, view.Work.Item.Item, view.Work.ReadRevision)
	return input, view, err
}

func (owner *Work) AuthorizeStart(ctx context.Context, delivery workqueue.Delivery) (workqueue.Change, error) {
	input, _, err := owner.loadDelivery(ctx, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	return inputGuard(input), nil
}

func (owner *Work) RejectUnstarted(ctx context.Context, delivery workqueue.Delivery) error {
	_, view, err := owner.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	attempt := view.Attempt.Attempt
	if attempt.StartedAt != nil {
		return errs.New(errs.KindStateConflict, "started Agent lifecycle work needs effect reconciliation")
	}
	observed := agentoperations.NotStarted
	if attempt.Direction == workexecution.Recovery {
		observed = agentoperations.Unresolved
	}
	return owner.inputs.SaveResult(ctx, owner.queue, attempt, agentoperations.Result{Outcome: workexecution.Failed, Observed: observed}, time.Now().UTC())
}

func (owner *Work) Execute(ctx context.Context, delivery workqueue.Delivery) error {
	return owner.run(ctx, delivery, delivery.Attempt.Attempt.Direction == workexecution.Forward)
}

func (owner *Work) Reconcile(ctx context.Context, delivery workqueue.Delivery) error {
	return owner.run(ctx, delivery, false)
}

func enrollment(capture agentoperations.Capture) localagent.EnrollRequest {
	config := capture.Enrollment
	return localagent.EnrollRequest{AgentID: capture.AgentID, EnrollmentTaskID: capture.TaskID, Image: capture.Image,
		Config: localagent.Config{PullIntervalSeconds: config.PullIntervalSeconds, MaxConcurrentTasks: config.MaxConcurrentTasks, Labels: config.Labels}}
}

func replacement(capture agentoperations.Capture) localagent.UpdateRequest {
	return localagent.UpdateRequest{AgentID: capture.AgentID, PreviousImage: capture.PreviousImage, DesiredImage: capture.Image, StartingGeneration: capture.StartingGeneration}
}
