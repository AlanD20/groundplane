package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// FenceReplacementAttempt resolves an unknown ReplaceGeneration transaction.
// If its primary revision remains unchanged, a same-value CAS advances that
// revision and makes every delayed transaction using the old evidence fail.
// A read alone cannot supply this proof. An already advanced record is returned
// unchanged so the lifecycle owner can recover the committed generation.
func (repository *LocalAgentRepository) FenceReplacementAttempt(
	ctx context.Context, agentID string, generation uint64, revision int64,
) (etcdstore.Versioned[LocalAgentRecord], error) {
	if err := validateContext(ctx); err != nil {
		return etcdstore.Versioned[LocalAgentRecord]{}, err
	}
	if ids.Validate(ids.KindAgent, agentID) != nil || generation == 0 || revision <= 0 {
		return etcdstore.Versioned[LocalAgentRecord]{}, errs.New(
			errs.KindValidationFailed,
			"local Agent replacement barrier is invalid",
		)
	}
	for range 3 {
		evidence, err := repository.readSingleton(ctx)
		if err != nil {
			return etcdstore.Versioned[LocalAgentRecord]{}, err
		}
		if evidence.record.ID != agentID || evidence.record.Generation < generation ||
			evidence.primaryRevision < revision {
			return etcdstore.Versioned[LocalAgentRecord]{}, errs.New(
				errs.KindStateConflict,
				"local Agent replacement authority changed",
			)
		}
		if evidence.primaryRevision > revision {
			return etcdstore.Versioned[LocalAgentRecord]{
				Record:       evidence.record,
				Revision:     evidence.primaryRevision,
				ReadRevision: evidence.readRevision,
			}, nil
		}
		if evidence.record.Generation != generation {
			return etcdstore.Versioned[LocalAgentRecord]{}, errs.New(
				errs.KindStateConflict,
				"local Agent replacement generation changed",
			)
		}
		result, err := repository.store.Transact(ctx, []etcdstore.Condition{
			{Key: localAgentSingletonKey, ModRevision: evidence.singleton.ModRevision},
			{Key: localAgentPrimaryKey(agentID), ModRevision: revision},
		}, []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: localAgentPrimaryKey(agentID), Value: evidence.primary.Value}})
		if err != nil {
			return etcdstore.Versioned[LocalAgentRecord]{}, err
		}
		clearKeyValues(result.FailureReads)
		if result.Succeeded {
			return etcdstore.Versioned[LocalAgentRecord]{
				Record:       evidence.record,
				Revision:     result.Revision,
				ReadRevision: result.Revision,
			}, nil
		}
	}
	return etcdstore.Versioned[LocalAgentRecord]{}, errs.New(
		errs.KindStateConflict,
		"local Agent replacement barrier raced another transition",
	)
}
