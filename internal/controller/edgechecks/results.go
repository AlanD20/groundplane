package edgechecks

import (
	"context"
	"slices"
	"time"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) GetConnectionCheck(
	ctx context.Context,
	environmentID, taskID string,
) (apiTypes.EdgeCheckResult, error) {
	loaded, err := service.checks.Load(ctx, taskID)
	if err != nil {
		return apiTypes.EdgeCheckResult{}, err
	}
	return service.result(ctx, environmentID, loaded.Record)
}

func (service *Service) GetLatestConnectionCheck(
	ctx context.Context,
	environmentID, routeID string,
) (apiTypes.EdgeCheckResult, error) {
	loaded, err := service.checks.Latest(ctx, routeID)
	if err != nil {
		return apiTypes.EdgeCheckResult{}, err
	}
	return service.result(ctx, environmentID, loaded.Record)
}

func (service *Service) result(
	ctx context.Context,
	environmentID string,
	record checkrecord.Record,
) (apiTypes.EdgeCheckResult, error) {
	if record.Owner.EnvironmentID != environmentID {
		return apiTypes.EdgeCheckResult{}, errs.New(
			errs.KindRequestNotFound,
			"Tunnel check was not found in this Environment",
		)
	}
	stale, err := service.routes.EdgeCheckSourcesChanged(ctx, record.Source)
	if err != nil {
		return apiTypes.EdgeCheckResult{}, err
	}
	return connectionCheckResult(record, stale), nil
}

func connectionCheckResult(record checkrecord.Record, stale bool) apiTypes.EdgeCheckResult {
	source, parameters := record.Source, record.Source.Request
	configuration := apiTypes.EdgeCheckConfiguration{RouteID: parameters.RouteID, RouteRevision: source.RouteRevision,
		BlueprintRevision: source.BlueprintRevision, OriginServiceID: parameters.OriginServiceID,
		OriginRuntimeRevision: source.OriginRuntimeRevision, OriginRuntimeSHA256: source.OriginRuntimeSHA256,
		RouteObservationRevision: source.RouteObservationRevision, ConnectorID: parameters.ComponentID, ConnectorServiceID: parameters.ServiceID,
		ConnectorRevision: source.ComponentRevision, ConnectorAppliedRevision: source.ComponentAppliedRevision,
		ConnectorContainerID: source.ConnectorContainerID, RouterID: source.RouterComponentID, RouterRevision: source.RouterRevision,
		RouterAppliedRevision: source.RouterAppliedRevision, OriginAddress: parameters.OriginAddress, Target: parameters.Target,
		RuntimeSHA256: parameters.RuntimeSHA256, TokenSecretID: source.TokenSecretID, TokenSecretRevision: source.TokenSecretRevision,
		TokenValueRevision: source.TokenValueRevision, AgentID: source.AgentID, AgentGeneration: source.AgentGeneration}
	result := apiTypes.EdgeCheckResult{
		TaskID:          record.Owner.TaskID,
		EnvironmentID:   record.Owner.EnvironmentID,
		Configuration:   configuration,
		Stale:           stale,
		Phase:           string(record.Phase),
		Outcome:         record.Outcome,
		Failure:         record.Failure,
		PrivateFailure:  record.PrivateFailure,
		PublicFailure:   record.PublicFailure,
		Stages:          []apiTypes.EdgeCheckStage{},
		Addresses:       []string{},
		CleanupRequired: record.Phase == checkrecord.CleanupRequired,
		CleanupTaskID:   record.TransferredTo,
		CreatedAt:       record.CreatedAt.Format(time.RFC3339),
	}
	if record.CleanupPending && record.Phase != checkrecord.Transferred {
		result.HelperName = contract.HelperName(record.HelperTaskID)
	}
	if record.Private != nil {
		result.Stages = appendCheckStages(result.Stages, record.Private.Stages, "Tunnel connector network")
	}
	if record.Public != nil {
		result.Stages = appendCheckStages(result.Stages, record.Public.Stages, "Controller host outbound network")
		result.Addresses, result.SelectedAddress, result.TLSVersion = slices.Clone(
			record.Public.Addresses,
		), record.Public.SelectedAddress, record.Public.TLSVersion
		if record.Public.CertificateExpiresAt != nil {
			result.CertificateExpiresAt = record.Public.CertificateExpiresAt.Format(time.RFC3339)
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

func appendCheckStages(
	output []apiTypes.EdgeCheckStage,
	stages []contract.Stage,
	location string,
) []apiTypes.EdgeCheckStage {
	for _, stage := range stages {
		output = append(
			output,
			apiTypes.EdgeCheckStage{Name: stage.Name, Outcome: stage.Outcome, Failure: stage.Failure,
				HTTPStatus: stage.HTTPStatus, ElapsedMilliseconds: stage.Elapsed.Milliseconds(), ExecutionLocation: location},
		)
	}
	return output
}
