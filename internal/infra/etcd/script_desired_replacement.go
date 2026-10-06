package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptauthoring"
	scriptmutations "github.com/AlanD20/groundplane/internal/infra/etcd/scriptmutations"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

func (repository *ScriptRepository) ReplaceDesiredIdempotent(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	current etcdstore.Versioned[scriptrecord.Record],
	desired core.Script,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := scriptmutations.ValidateScriptMutationMarker(marker, current.Record.EnvironmentID); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replacement, conditions, mutations, classify, err := repository.PrepareScriptReplacement(
		ctx, environment, project, target, current, desired,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	publication, err := prepareDirectDesiredProjectionPublication(
		ctx, repository.store, replacement.EnvironmentID, marker,
		scriptauthoring.Replace(replacement),
	)
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		return IdempotencyTransactionResult{}, err
	}
	defer clearRouteHeadPublication(publication)
	conditions, mutations, classify, err = publication.bindDirectDesired(conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer etcdstore.ClearMutationValues(mutations)
	plan, err := NewIdempotencyMutationPlan(conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}
