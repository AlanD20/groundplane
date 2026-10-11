package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
)

// EdgeCheckSourcesChanged reads current revisions at one MVCC view. Viewing a
// stored result never queries the provider, reads token plaintext or re-probes.
func (repository *RouteRepository) EdgeCheckSourcesChanged(
	ctx context.Context,
	source edgechecks.Source,
) (bool, error) {
	if err := source.Validate(); err != nil {
		return false, err
	}
	request := source.Request
	keys := []string{
		hierarchy.EnvironmentKey(request.EnvironmentID),
		blueprints.EnvironmentBlueprintHeadKey(request.EnvironmentID),
		components.RecordKey(request.ComponentID),
		componentplanning.RuntimeKey(request.ComponentID),
		secrets.RecordKey(source.TokenSecretID),
		secrets.ValueKey(source.TokenSecretID),
		localagents.LocalAgentPrimaryKey(source.AgentID),
		localagents.LocalAgentSingletonKey,
	}
	revisions := []int64{source.EnvironmentRevision, source.BlueprintRevision,
		source.ComponentRevision, source.ComponentAppliedRevision,
		source.TokenSecretRevision, source.TokenValueRevision, source.AgentRevision, source.AgentSingletonRevision}
	if request.RouteID != "" {
		keys = append(
			keys,
			routes.ObservationKey(request.RouteID),
			components.RecordKey(source.RouterComponentID),
			componentplanning.RuntimeKey(source.RouterComponentID),
		)
		revisions = append(
			revisions,
			source.RouteObservationRevision,
			source.RouterRevision,
			source.RouterAppliedRevision,
		)
	} else {
		keys = append(keys, serviceruntimerecord.Key(request.OriginServiceID), services.ServiceRuntimeKey(request.OriginServiceID))
		revisions = append(revisions, source.OriginRuntimeRevision, source.OriginIntentRevision)
	}
	read, err := repository.edgeCheckRead(ctx, keys, 0)
	if err != nil {
		return false, err
	}
	defer keyvalue.ClearValues(read.Values)
	for index, revision := range revisions {
		if keyvalue.RevisionOf(read.Values[index]) != revision {
			return true, nil
		}
	}
	return false, nil
}
