package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	localagentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *LocalAgentRepository) CreateSingleton(
	ctx context.Context,
	record localagentrecord.LocalAgentRecord,
) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	if record.Phase != localagentrecord.LocalAgentPhaseProvisioning {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindValidationFailed,
			"local Agent must be created in provisioning phase",
		)
	}
	if record.TokenUpdatedAt.IsZero() {
		record.TokenUpdatedAt = record.CreatedAt
	}
	if err := localagentrecord.ValidateLocalAgentRecord(record); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	primaryValue, configValue, tokenValue, reference, err := localagentrecord.EncodeLocalAgentValues(record)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	defer clear(primaryValue)
	defer clear(configValue)
	defer clear(tokenValue)
	defer clear(reference)
	keys := localagentrecord.LocalAgentKeys(record.ID, record.TokenDigest)
	conditions := make([]etcdstore.Condition, len(keys))
	mutations := make([]etcdstore.Mutation, len(keys))
	values := [][]byte{reference, primaryValue, reference, configValue, tokenValue, reference}
	for index, key := range keys {
		conditions[index] = etcdstore.Condition{Key: key}
		mutations[index] = etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: values[index]}
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	clearKeyValues(result.FailureReads)
	if !result.Succeeded {
		if len(result.FailureReads) == len(conditions) && result.FailureReads[0] != nil {
			return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(errs.KindStateConflict, "the local Agent already exists")
		}
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindInternal,
			"local Agent creation collided with durable state",
		)
	}
	return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
		Record: localagentrecord.CloneLocalAgentRecord(record), Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}
