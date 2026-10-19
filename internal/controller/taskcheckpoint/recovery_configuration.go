package taskcheckpoint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	materialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type fileSelection struct {
	attempt    workexecution.Attempt
	stage      string
	reference  *agentpb.WorkConfigurationReference
	generation uint64
	files      []taskmaterialization.Record
}

func (service *ReleaseCheckpointService) activeFileSelection(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	identity *agentpb.WorkAttemptIdentity,
	stage string,
) (fileSelection, error) {
	if service == nil || ctx == nil || service.files == nil {
		return fileSelection{}, invalidConfigurationRequest()
	}
	if _, err := authorization.Conditions(); err != nil {
		return fileSelection{}, err
	}
	claimed, err := workexecution.DecodeAgentAttempt(identity, authorization.Identity())
	if err != nil {
		return fileSelection{}, err
	}
	view, err := authorization.ReadResult(ctx, service.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
	if err != nil {
		return fileSelection{}, err
	}
	if !service.restoring(view) {
		input, report, files, err := service.activeConfiguration(ctx, authorization, identity, stage)
		if err != nil {
			return fileSelection{}, err
		}
		return fileSelection{attempt: input.Attempt, stage: report.StageID, reference: input.Release.Configuration,
			generation: input.Release.Configuration.Generation, files: files}, nil
	}
	if !view.ActiveAttempt || !view.CurrentExecutor || !workexecution.SameAssignment(claimed, view.Attempt.Attempt) ||
		view.Attempt.Attempt.StartedAt == nil || view.Work.Task.Task.FinishedAt != nil || view.Work.Task.Task.CollectionStartedAt != nil {
		return fileSelection{}, invalidConfigurationRequest()
	}
	owner := service.restorations[view.Work.Item.Item.Operation]
	if owner == nil {
		return fileSelection{}, invalidConfigurationRequest()
	}
	input, report, files, err := owner.LoadActiveRecoveryConfiguration(ctx, view)
	if err != nil {
		return fileSelection{}, err
	}
	if !workexecution.SameAssignment(input.Input.Attempt, view.Attempt.Attempt) || report.StageID != stage ||
		len(
			files,
		) > 512 || len(files) != len(input.Input.Release.ConfigurationDestinations) || input.Input.Release.Configuration == nil {
		return fileSelection{}, invalidConfigurationRequest()
	}
	selection := fileSelection{
		attempt:    input.Input.Attempt,
		stage:      stage,
		reference:  input.Input.Release.PriorConfiguration,
		generation: input.Input.Release.Configuration.Generation,
		files:      files,
	}
	if selection.reference != nil {
		selection.generation = selection.reference.Generation
	}
	for index, file := range files {
		header, err := materialization.CapturedFileHeader(selection.attempt.TaskID, stage, selection.generation, file)
		if err != nil || releaseexecution.ValidateRecoveryFile(input, report, header) != nil ||
			file.Destination != input.Input.Release.ConfigurationDestinations[index] {
			return fileSelection{}, invalidConfigurationRequest()
		}
	}
	// Empty selections still require a valid acknowledged recovery phase.
	if len(files) == 0 {
		if _, err := releaseexecution.RecoveryConfigurationResult(input, report, nil); err != nil {
			return fileSelection{}, err
		}
	}
	return selection, nil
}
