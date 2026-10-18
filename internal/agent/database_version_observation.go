package agent

import (
	"context"
	"github.com/AlanD20/groundplane/internal/agent/backupruntime"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Version reads use the same attestation as Backup/Restore, never shell input
// from an operator and never a mutation-capable protocol operation.
type databaseVersionObserver struct{ ServiceObserver }

func (observer databaseVersionObserver) Observe(
	ctx context.Context,
	request *agentpb.ObserveServices,
) (*agentpb.ServiceObservationResult, error) {
	result, err := observer.ServiceObserver.Observe(ctx, request)
	if err != nil {
		return nil, err
	}
	for index, target := range request.Targets {
		if target.DatabaseProbe == nil {
			continue
		}
		if result == nil || index >= len(result.Observations) {
			return nil, invalidAgentStaging()
		}
		row := result.Observations[index]
		if row.GetUnavailable() {
			continue
		}
		if len(row.Containers) != 1 || row.Containers[0].State != "running" {
			return nil, invalidAgentStaging()
		}
		versions, err := backupruntime.ObserveDatabaseVersions(ctx, target.DatabaseProbe)
		if err != nil || versions.ContainerID != row.Containers[0].Id {
			return nil, invalidAgentStaging()
		}
		row.DatabaseVersions = versions.Wire()
	}
	return result, nil
}
