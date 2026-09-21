package etcd

import (
	"context"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	localagentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// UpdateConfigIdempotent atomically replaces the generation-bound config
// singleton and commits the exact completed replay marker. The lifecycle
// primary and singleton pointer fence deletion or replacement of the Agent.
func (repository *LocalAgentRepository) UpdateConfigIdempotent(
	ctx context.Context,
	current etcdstore.Versioned[localagentrecord.LocalAgentRecord],
	config localagentrecord.LocalAgentConfig,
	marker idempotencyrecord.IdempotencyMarker,
) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, err
	}
	if err := localagentrecord.ValidateLocalAgentConfig(config); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision ||
		current.Record.ID == "" || marker.Kind != idempotencyrecord.IdempotencyMarkerDirect ||
		marker.State != idempotencyrecord.IdempotencyMarkerCompleted || marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopePlatform ||
		marker.Locator.ScopeID != "-" || marker.Locator.Method != "PUT" ||
		marker.Locator.Route != "/agents/{id}/config" {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"local Agent config mutation identity is invalid",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, err
	}
	evidence, err := repository.readSingleton(ctx)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, err
	}
	if evidence.record.ID != current.Record.ID || evidence.record.Generation != current.Record.Generation ||
		evidence.primaryRevision != current.Revision {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"local Agent generation or revision changed",
		)
	}
	if evidence.record.Phase == localagentrecord.LocalAgentPhaseDeleting {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"deleting local Agent config cannot be changed",
		)
	}
	replacement := localagentrecord.CloneLocalAgentRecord(evidence.record)
	replacement.Config = localagentrecord.CloneLocalAgentConfig(config)
	configValue, err := localagentrecord.EncodeLocalAgentConfig(replacement)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, err
	}
	defer clear(configValue)
	plan, err := newIdempotencyMutationPlan(
		[]etcdstore.Condition{
			{Key: localagentrecord.LocalAgentSingletonKey, ModRevision: evidence.singleton.ModRevision},
			{Key: localagentrecord.LocalAgentPrimaryKey(current.Record.ID), ModRevision: evidence.primary.ModRevision},
			{Key: localagentrecord.LocalAgentConfigKey(current.Record.ID), ModRevision: evidence.config.ModRevision},
		},
		[]etcdstore.Mutation{{
			Type: etcdstore.MutationPut, Key: localagentrecord.LocalAgentConfigKey(current.Record.ID), Value: configValue,
		}},
		classifyLocalAgentConfigConflict(current.Record.ID),
	)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, err
	}
	result, err := idempotency.Apply(ctx, marker, plan)
	return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
		Record: replacement, Revision: evidence.primaryRevision, ReadRevision: result.revision,
	}, result, err
}

func classifyLocalAgentConfigConflict(agentID string) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != 3 {
			return errs.New(errs.KindInternal, "local Agent config compare evidence is incomplete")
		}
		if values[0] == nil || values[1] == nil {
			return errs.New(errs.KindAgentNotFound, "local Agent was not found")
		}
		if values[2] == nil {
			return errs.New(errs.KindInternal, "local Agent config record is missing")
		}
		resolvedID, err := localagentrecord.DecodeLocalAgentReference(values[0].Value)
		if err != nil {
			return err
		}
		if resolvedID != agentID {
			return errs.New(errs.KindAgentNotFound, "local Agent was not found")
		}
		return errs.New(errs.KindStateConflict, "local Agent config changed")
	}
}
