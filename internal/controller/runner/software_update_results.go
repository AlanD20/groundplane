package runner

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerupdates"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *SoftwareService) Versions(ctx context.Context, runnerID string) (apiTypes.RunnerVersions, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil {
		return apiTypes.RunnerVersions{}, errs.New(errs.KindValidationFailed, "Runner id is invalid")
	}
	if _, err := service.runners.GetRunner(ctx, runnerID); err != nil {
		return apiTypes.RunnerVersions{}, err
	}
	versions, err := service.catalog.List(ctx)
	if err != nil {
		return apiTypes.RunnerVersions{}, err
	}
	if len(versions) == 0 {
		return apiTypes.RunnerVersions{}, errs.New(errs.KindStorageUnavailable, "GitHub returned no checksummed Runner releases for this host architecture")
	}
	result := apiTypes.RunnerVersions{Items: []apiTypes.RunnerVersion{}}
	for _, release := range versions {
		result.Items = append(result.Items, apiTypes.RunnerVersion{Version: release.Version, Architecture: release.Architecture})
	}
	return result, nil
}
func (service *SoftwareService) GetUpdate(ctx context.Context, runnerID, taskID string) (apiTypes.RunnerUpdateResult, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil || taskID != "" && ids.Validate(ids.KindTask, taskID) != nil {
		return apiTypes.RunnerUpdateResult{}, errs.New(errs.KindValidationFailed, "Runner update selection is invalid")
	}
	var record runnerupdates.Record
	if taskID == "" {
		loaded, err := service.records.Latest(ctx, runnerID)
		if err != nil {
			return apiTypes.RunnerUpdateResult{}, err
		}
		record = loaded.Record
	} else {
		loaded, err := service.records.Load(ctx, taskID)
		if err != nil {
			return apiTypes.RunnerUpdateResult{}, err
		}
		record = loaded.Record
	}
	if record.Source.RunnerID != runnerID {
		return apiTypes.RunnerUpdateResult{}, errs.New(errs.KindRequestNotFound, "Update was not found for this Runner")
	}
	result := apiTypes.RunnerUpdateResult{TaskID: record.Owner.TaskID, Version: record.Request.Release.Version, Phase: string(record.Phase), Outcome: record.Outcome, Failure: record.Failure, Stages: []apiTypes.RunnerUpdateStage{}, CreatedAt: record.CreatedAt.Format(time.RFC3339Nano)}
	if record.FinishedAt != nil {
		result.FinishedAt = record.FinishedAt.Format(time.RFC3339Nano)
	}
	if record.Result != nil {
		for _, stage := range record.Result.Stages {
			result.Stages = append(result.Stages, apiTypes.RunnerUpdateStage{Name: stage.Name, Outcome: stage.Outcome, Failure: stage.Failure})
		}
	}
	return result, nil
}
