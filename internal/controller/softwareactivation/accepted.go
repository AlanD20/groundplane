package softwareactivation

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	activationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type activationRequest struct {
	PreparationTaskID string `json:"preparation_task_id"`
}

func (service *Service) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{acceptedaction.Registration(service.submitter, "software-activate",
		workexecution.ActionActivate, ids.KindComponent, service.prepareAccepted)}
}

func (service *Service) prepareAccepted(ctx context.Context, accepted workqueue.TaskSnapshot, _ int64,
	at time.Time, request activationRequest,
) (taskdependencies.Prepared, error) {
	prepared, err := service.preparations.Get(ctx, request.PreparationTaskID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if prepared.Record.Progress.Phase != preparation.PhaseVerified {
		return taskdependencies.Prepared{}, errs.New(errs.KindStateConflict, "software preparation is not verified")
	}
	agent, err := service.freezeAgent(ctx, prepared.Record.Source.Selection)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	stagingImage := ""
	if prepared.Record.Source.Selection.IncludesController() {
		if prepared.Record.Source.Selection.IncludesAgent() {
			stagingImage = prepared.Record.Progress.Result.Agent.Artifact.Reference
		} else if agent != nil {
			stagingImage = agent.Image
		} else {
			stagingImage, err = service.controllerSource.DesiredAgentImage(ctx)
			if err != nil {
				return taskdependencies.Prepared{}, err
			}
			if !imageref.IsDigestPinned(stagingImage) {
				return taskdependencies.Prepared{}, errs.New(errs.KindStateConflict, "Controller activation has no valid default Agent image")
			}
		}
	}
	task := workexecution.CloneTask(accepted.Task)
	input := activationrecord.Input{
		OperationID:       ids.DeriveAt(ids.KindOperation, task.CreatedAt, task.ID, "software-activation"),
		Preparation:       prepared.Record,
		Agent:             agent,
		StagingAgentImage: stagingImage,
	}
	if prepared.Record.Source.Selection.IncludesController() {
		input.PreviousController, err = service.controllerSource.ActivationPredecessor(ctx)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
	}
	_, inputHash, err := activationrecord.EncodeInput(input)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	record, err := activationrecord.NewRecord(task.ID, inputHash, input, at)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	record.WorkItemID = ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "software-staging")
	if record.ControllerWorkItemID != "" {
		record.ControllerWorkItemID = ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "software-controller")
	}
	if record.AgentWorkItemID != "" {
		record.AgentWorkItemID = ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "software-agent")
	}
	captured, graph, err := record.Graph()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task.Intent, task.ExpectedItems, task.UpdatedAt = captured.Intent, uint32(len(graph)), at
	for index := range graph {
		graph[index].Deadline = acceptedaction.ExecutionDeadline(
			task,
			at,
			time.Duration(activationrecord.TaskTimeoutSeconds)*time.Second,
		)
	}
	authority, err := service.tasks.PrepareSoftwareActivationAuthority(ctx, record, prepared)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	return taskdependencies.Prepared{Task: task, Graph: graph, Authority: authority}, nil
}
