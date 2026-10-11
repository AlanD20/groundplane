package runner

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerchecks"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *ConnectionCheckService) GetConnectionCheck(
	ctx context.Context,
	runnerID, taskID string,
) (apiTypes.RunnerCheckResult, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil || taskID != "" && ids.Validate(ids.KindTask, taskID) != nil {
		return apiTypes.RunnerCheckResult{}, errs.New(
			errs.KindValidationFailed,
			"Runner diagnostic selection is invalid",
		)
	}
	var record runnerchecks.Record
	if taskID == "" {
		loaded, err := service.records.Latest(ctx, runnerID)
		if err != nil {
			return apiTypes.RunnerCheckResult{}, err
		}
		record = loaded.Record
	} else {
		loaded, err := service.records.Load(ctx, taskID)
		if err != nil {
			return apiTypes.RunnerCheckResult{}, err
		}
		record = loaded.Record
	}
	if record.Source.RunnerID != runnerID {
		return apiTypes.RunnerCheckResult{}, errs.New(
			errs.KindRequestNotFound,
			"Connection check was not found for this Runner",
		)
	}
	stale, err := service.runners.RunnerCheckSourcesChanged(ctx, record.Source)
	if err != nil {
		return apiTypes.RunnerCheckResult{}, err
	}
	source := record.Source
	result := apiTypes.RunnerCheckResult{TaskID: record.Owner.TaskID, ExecutionLocation: "Registered Runner container",
		Configuration: apiTypes.RunnerCheckConfiguration{RunnerID: runnerID, RunnerRevision: source.RunnerRevision,
			RuntimeEpoch: source.Ownership.RuntimeEpoch, ImageReference: record.Plan.Container.ImageRef,
			GitHubURL: source.Desired.GitHubURL, ControllerEndpoint: record.Request.ControllerEndpoint},
		Stale: stale, Phase: string(record.Phase), Outcome: record.Outcome, Failure: record.Failure,
		Stages: []apiTypes.RunnerCheckStage{}, CreatedAt: record.CreatedAt.Format(time.RFC3339)}
	if record.StartedAt != nil {
		result.StartedAt = record.StartedAt.Format(time.RFC3339)
	}
	if record.FinishedAt != nil {
		result.FinishedAt = record.FinishedAt.Format(time.RFC3339)
	}
	if record.Result != nil {
		result.ObservedAt = record.Result.ObservedAt.Format(time.RFC3339)
		for _, stage := range record.Result.Stages {
			result.Stages = append(result.Stages, apiTypes.RunnerCheckStage{Name: stage.Name, Outcome: stage.Outcome,
				Failure: stage.Failure, HTTPStatus: stage.HTTPStatus, ElapsedMilliseconds: stage.ElapsedMilliseconds})
		}
	}
	return result, nil
}
