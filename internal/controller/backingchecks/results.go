package backingchecks

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) GetAccessCheck(
	ctx context.Context,
	environmentID, taskID string,
) (apiTypes.BackingCheckResult, error) {
	loaded, err := service.checks.Load(ctx, taskID)
	if err != nil {
		return apiTypes.BackingCheckResult{}, err
	}
	defer loaded.Record.Clear()
	if loaded.Record.Owner.EnvironmentID != environmentID {
		return apiTypes.BackingCheckResult{}, errs.New(
			errs.KindRequestNotFound,
			"access check was not found in this Environment",
		)
	}
	stale, err := service.attaches.BackingCheckSourcesChanged(ctx, loaded.Record.Source)
	if err != nil {
		return apiTypes.BackingCheckResult{}, err
	}
	return accessCheckResult(loaded.Record, stale), nil
}

func (service *Service) GetLatestAccessCheck(
	ctx context.Context,
	environmentID, attachID, grantID string,
) (apiTypes.BackingCheckResult, error) {
	consumer, err := service.attaches.GetAttach(ctx, attachID)
	if err != nil {
		return apiTypes.BackingCheckResult{}, err
	}
	if consumer.Record.EnvironmentID != environmentID {
		return apiTypes.BackingCheckResult{}, errs.New(
			errs.KindRequestNotFound,
			"Attach was not found in this Environment",
		)
	}
	loaded, err := service.checks.Latest(ctx, attachID, grantID)
	if err != nil {
		return apiTypes.BackingCheckResult{}, err
	}
	defer loaded.Record.Clear()
	if loaded.Record.Owner.EnvironmentID != environmentID {
		return apiTypes.BackingCheckResult{}, errs.New(
			errs.KindRequestNotFound,
			"access check was not found in this Environment",
		)
	}
	stale, err := service.attaches.BackingCheckSourcesChanged(ctx, loaded.Record.Source)
	if err != nil {
		return apiTypes.BackingCheckResult{}, err
	}
	return accessCheckResult(loaded.Record, stale), nil
}

func accessCheckResult(record checkrecord.Record, stale bool) apiTypes.BackingCheckResult {
	source, parameters := record.Source, record.Source.Parameters
	configuration := apiTypes.BackingCheckConfiguration{
		AttachID:           parameters.AttachID,
		CredentialAttachID: parameters.CredentialAttachID,
		ServiceID:          parameters.ServiceID,
		BackingServiceID:   parameters.BackingServiceID,
		GrantID:            source.GrantID,
		Family:             parameters.Family,
		Authentication:     parameters.Authentication,
		Host:               parameters.Host,
		Port:               parameters.Port,
		Database:           parameters.Database,
		Username:           parameters.Username,
		AttachRevision:     source.AttachRevision,
		CredentialRevision: source.CredentialRevision,
		FactsRevision:      source.FactsRevision,
		GrantRevision:      source.GrantRevision,
		AgentID:            source.AgentID,
		ContainerID:        source.ContainerID,
		RuntimeRevision:    source.RuntimeRevision,
	}
	result := apiTypes.BackingCheckResult{TaskID: record.Owner.TaskID, EnvironmentID: record.Owner.EnvironmentID,
		ExecutionLocation: "consumer workload network", Configuration: configuration, Stale: stale,
		Phase: string(record.Phase), Outcome: string(record.Outcome), Failure: record.Failure,
		Stages: []apiTypes.BackingCheckStage{}, CleanupRequired: record.Phase == checkrecord.CleanupRequired,
		CreatedAt: record.CreatedAt.Format(time.RFC3339)}
	if record.CleanupPending {
		result.HelperName = backingaccess.HelperName(record.HelperTaskID)
	}
	if record.Observation != nil {
		for _, stage := range record.Observation.Stages {
			result.Stages = append(result.Stages, apiTypes.BackingCheckStage{Name: stage.Name, Outcome: stage.Outcome,
				Failure: stage.Failure, ElapsedMilliseconds: stage.Elapsed.Milliseconds()})
		}
	}
	if record.StartedAt != nil {
		result.StartedAt = record.StartedAt.Format(time.RFC3339)
	}
	if record.FinishedAt != nil {
		result.FinishedAt = record.FinishedAt.Format(time.RFC3339)
	}
	return result
}
