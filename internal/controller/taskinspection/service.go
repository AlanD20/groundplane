// Package taskinspection reads the execution model without interpreting logs or
// granting mutation authority. Operation detail providers supply safe evidence.
package taskinspection

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/etcdconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	activationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Service struct {
	environments       *environmentprovision.Repository
	activations        *activationrecord.Repository
	agentLifecycle     *agentoperations.Repository
	controllerSoftware *controllersoftware.Repository
	preparations       *preparationrecord.Repository
	etcd               *etcdconfiguration.Repository
	runners            *runnerwork.Repository
	components         *componentwork.InputRepository
	queue              *workqueue.Repository
	images             *imageoperations.Repository
	diagnostics        *executiondiagnostics.Repository
	retries            *workdispatch.Retries
	hierarchy          *etcd.HierarchyDeletionRepository
}

func New(queue *workqueue.Repository, images *imageoperations.Repository, diagnostics *executiondiagnostics.Repository, retries *workdispatch.Retries, hierarchy *etcd.HierarchyDeletionRepository, components *componentwork.InputRepository, runners *runnerwork.Repository, etcdConfig *etcdconfiguration.Repository, preparations *preparationrecord.Repository, controllerSoftware *controllersoftware.Repository, agentLifecycle *agentoperations.Repository, activations *activationrecord.Repository, environments *environmentprovision.Repository) (*Service, error) {
	if environments == nil || queue == nil || images == nil || diagnostics == nil || retries == nil || hierarchy == nil || components == nil || runners == nil || etcdConfig == nil || preparations == nil || controllerSoftware == nil || agentLifecycle == nil || activations == nil {
		return nil, errs.New(errs.KindInternal, "Task inspection requires execution storage")
	}
	return &Service{environments: environments, queue: queue, images: images, diagnostics: diagnostics, retries: retries, hierarchy: hierarchy, components: components, runners: runners, etcd: etcdConfig, preparations: preparations, controllerSoftware: controllerSoftware, agentLifecycle: agentLifecycle, activations: activations}, nil
}

type Summary struct {
	ID                             string
	Action                         workexecution.Action
	TargetID, TargetName           string
	ResourceKind                   string
	Scope                          workexecution.Scope
	Actor                          workexecution.Actor
	Status                         string
	Counts                         workexecution.Progress
	CreatedAt, UpdatedAt, Deadline time.Time
	FinishedAt                     *time.Time
	HistoryExpired                 bool
	StopRequested                  bool
	Cancel                         ActionAvailability
}

type Work struct {
	Preparation                    *WorkPreparation
	Retry                          ActionAvailability
	ID, TargetID                   string
	Operation                      workexecution.Operation
	Placement                      workexecution.Placement
	Phase                          workexecution.Phase
	Outcome                        workexecution.Outcome
	Wait                           *workexecution.Wait
	Cleanup                        workexecution.Cleanup
	AttemptCount                   uint64
	ActiveAttemptID                string
	CreatedAt, UpdatedAt, Deadline time.Time
}

type WorkPreparation struct {
	Summary      string
	DependencyID string
	FinishedAt   time.Time
}

type Page struct {
	Preparation *Preparation
	Task        Summary
	Work        []Work
	More        bool
	After       string
	Revision    int64
}

// WorkPage uses the Task read revision for membership and state. Later pages
// must retain that revision; a compacted snapshot is an error, not a mixed view.
func (service *Service) WorkPage(ctx context.Context, taskID, after string, limit int64, revision int64) (Page, error) {
	if limit < 1 || limit > 100 {
		return Page{}, errs.New(errs.KindValidationFailed, "Task work page size must be between 1 and 100")
	}
	task, readRevision, err := service.queue.ReadTask(ctx, taskID, revision)
	if err != nil {
		return Page{}, err
	}
	result := Page{Task: summarize(task.Task), Revision: readRevision, Work: []Work{}}
	if result.Task.HistoryExpired {
		return result, nil
	}
	result.Preparation, err = service.preparation(ctx, task, readRevision)
	if err != nil {
		return Page{}, err
	}
	page, err := service.queue.ListWork(ctx, taskID, after, limit, readRevision)
	if err != nil {
		return Page{}, err
	}
	for _, snapshot := range page.Items {
		item := workexecution.Clone(snapshot.Item)
		settlement, err := service.queue.ReadInputSettlement(ctx, workqueue.View{Task: task, Item: snapshot, ReadRevision: readRevision})
		if err != nil {
			return Page{}, err
		}
		var preparation *WorkPreparation
		if settlement != nil {
			preparation = &WorkPreparation{Summary: settlement.Reason, DependencyID: settlement.DependencyID, FinishedAt: settlement.At}
		}
		retry := ActionAvailability{Reason: "This work has no retryable failure"}
		if preparation != nil {
			retry.Reason = "No executable input was prepared. Submit a new action after resolving the reported problem."
			if item.Outcome == workexecution.Succeeded {
				retry.Reason = "The captured operation policy required no host execution for this work."
			}
		} else if (item.Phase == workexecution.Attention || item.Phase == workexecution.Settled && item.Outcome == workexecution.Failed) && item.ActiveAttemptID == "" {
			allowed, reason, err := service.retries.Availability(ctx, taskID, item.ID, item.AttemptNumber, readRevision, time.Now().UTC())
			if err != nil {
				return Page{}, err
			}
			retry = ActionAvailability{Allowed: allowed, Reason: reason}
		}
		result.Work = append(result.Work, Work{ID: item.ID, TargetID: item.TargetID, Operation: item.Operation, Preparation: preparation,
			Placement: item.Placement, Phase: item.Phase, Outcome: item.Outcome, Wait: item.Wait, Cleanup: item.Cleanup,
			AttemptCount: item.AttemptNumber, ActiveAttemptID: item.ActiveAttemptID, Retry: retry,
			CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt, Deadline: item.Deadline})
	}
	result.More, result.After = page.More, page.LastID
	return result, nil
}

func summarize(task workexecution.Task) Summary {
	status := "queued"
	switch {
	case task.FinishedAt != nil:
		status = string(task.Outcome)
	case task.Progress.Attention > 0:
		status = "attention_required"
	case task.Stop != nil:
		status = "stopping"
	case task.Progress.Running > 0 || task.Progress.Assigned > 0:
		status = "running"
	case task.Progress.Waiting > 0:
		status = "waiting"
	case !task.PublicationComplete:
		status = "preparing"
	case task.Progress.Unsettled() == 0:
		status = "finishing"
	}
	return Summary{ID: task.ID, Action: task.Action, TargetID: task.TargetID, TargetName: task.TargetName,
		Scope: task.Scope, Actor: task.Actor, Status: status, Counts: task.Progress, ResourceKind: resourceKind(task.TargetID),
		CreatedAt: task.CreatedAt, UpdatedAt: task.UpdatedAt, Deadline: task.Deadline, FinishedAt: task.FinishedAt,
		HistoryExpired: task.CollectionStartedAt != nil, StopRequested: task.Stop != nil, Cancel: cancellation(task)}
}
