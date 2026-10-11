package connectors

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *CheckService) GetConnectionCheck(ctx context.Context, environmentID, taskID string,
) (apiTypes.ConnectorCheckResult, error) {
	loaded, err := service.checks.Load(ctx, taskID)
	if err != nil {
		return apiTypes.ConnectorCheckResult{}, err
	}
	defer loaded.Record.Clear()
	if loaded.Record.Owner.EnvironmentID != environmentID {
		return apiTypes.ConnectorCheckResult{}, errs.New(
			errs.KindRequestNotFound,
			"connector check was not found in this Environment",
		)
	}
	stale, err := service.checkSourcesChanged(ctx, loaded.Record)
	if err != nil {
		return apiTypes.ConnectorCheckResult{}, err
	}
	return connectionCheckResult(loaded.Record, stale), nil
}

func (service *CheckService) GetLatestConnectionCheck(ctx context.Context, environmentID, connectorID string,
) (apiTypes.ConnectorCheckResult, error) {
	connector, err := service.connectors.GetConnector(ctx, connectorID)
	if err != nil {
		return apiTypes.ConnectorCheckResult{}, err
	}
	if connector.Record.Connector.EnvironmentID != environmentID {
		return apiTypes.ConnectorCheckResult{}, errs.New(
			errs.KindRequestNotFound,
			"connector was not found in this Environment",
		)
	}
	loaded, err := service.checks.LoadLatest(ctx, connectorID)
	if err != nil {
		return apiTypes.ConnectorCheckResult{}, err
	}
	defer loaded.Record.Clear()
	if loaded.Record.Owner.EnvironmentID != environmentID {
		return apiTypes.ConnectorCheckResult{}, errs.New(
			errs.KindRequestNotFound,
			"connector check was not found in this Environment",
		)
	}
	stale, err := service.checkSourcesChanged(ctx, loaded.Record)
	if err != nil {
		return apiTypes.ConnectorCheckResult{}, err
	}
	return connectionCheckResult(loaded.Record, stale), nil
}

func connectionCheckResult(record connectorchecks.Record, stale bool) apiTypes.ConnectorCheckResult {
	result := apiTypes.ConnectorCheckResult{TaskID: record.Owner.TaskID, EnvironmentID: record.Owner.EnvironmentID,
		ExecutionLocation: "Controller", Configuration: connectorResponse(record.Connector),
		ConnectorRevision: record.ConnectorRevision, Draft: record.Draft, Stale: stale,
		Phase: string(record.Phase), Outcome: string(record.Outcome), Failure: record.Failure,
		Stages:        make([]apiTypes.ConnectorCheckStage, len(record.Stages)),
		SecretSources: make([]apiTypes.ConnectorCheckSecretSource, len(record.SecretSources)),
		UploadIDs:     []string{}, CleanupRequired: record.Phase == connectorchecks.CleanupRequired,
		CleanupTaskID: record.TransferredTo, CreatedAt: record.CreatedAt.Format(time.RFC3339)}
	for index, stage := range record.Stages {
		result.Stages[index] = apiTypes.ConnectorCheckStage{
			Name:                stage.Name,
			ElapsedMilliseconds: stage.Elapsed.Milliseconds(),
			Succeeded:           stage.Succeeded,
			Failure:             stage.Failure,
		}
	}
	for index, source := range record.SecretSources {
		result.SecretSources[index] = apiTypes.ConnectorCheckSecretSource{Credential: source.Name,
			SecretID: source.Pin.SecretID, Revision: source.Pin.MetadataRevision}
	}
	if record.Phase != connectorchecks.Transferred {
		if record.Cleanup.ObjectPending {
			result.ObjectKey = record.Cleanup.ObjectKey
		}
		if record.Cleanup.MultipartPending {
			result.MultipartKey = record.Cleanup.MultipartKey
		}
		if len(record.Cleanup.UploadIDs) > 0 {
			result.UploadIDs = slices.Clone(record.Cleanup.UploadIDs)
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

// A passing probe is historical evidence, not a certificate for later settings
// or values. Draft consumers additionally compare their local request identity
// before using any asynchronous result to validate the form currently open.
func (service *CheckService) checkSourcesChanged(ctx context.Context, record connectorchecks.Record) (bool, error) {
	if record.ConnectorRevision > 0 {
		current, err := service.connectors.GetConnector(ctx, record.Connector.Connector.ID)
		if err != nil {
			if kind, ok := errs.KindOf(err); ok && kind == errs.KindConnectorNotFound {
				return true, nil
			}
			return false, err
		}
		if current.Revision != record.ConnectorRevision {
			return true, nil
		}
	}
	if len(record.SecretSources) == 0 {
		return false, nil
	}
	environment, err := service.hierarchy.GetEnvironment(ctx, record.Owner.EnvironmentID)
	if err != nil {
		if kind, ok := errs.KindOf(err); ok && kind == errs.KindEnvironmentNotFound {
			return true, nil
		}
		return false, err
	}
	for _, source := range record.SecretSources {
		current, err := service.secrets.ResolveSecret(ctx, environment.Record.ProjectID, source.Reference)
		if err != nil {
			if kind, ok := errs.KindOf(err); ok && kind == errs.KindSecretNotFound {
				return true, nil
			}
			return false, err
		}
		if current.Record.Secret.ID != source.Pin.SecretID || current.Revision != source.Pin.MetadataRevision {
			return true, nil
		}
		value, err := service.secrets.GetSecretValue(ctx, current)
		if err != nil {
			return false, err
		}
		changed := value.CiphertextSHA256 != source.Pin.CiphertextSHA256
		clear(value.Ciphertext)
		if changed {
			return true, nil
		}
	}
	return false, nil
}
