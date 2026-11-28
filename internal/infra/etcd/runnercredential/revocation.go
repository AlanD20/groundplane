package runnercredential

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Revoke withdraws the named generation of a grant. If the grant was already
// withdrawn, retry succeeds; if a newer grant exists it remains untouched.
func (repository *Repository) Revoke(ctx context.Context, runnerID, requestID string, revision int64) error {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return err
	}
	if !validIdentity(runnerID, requestID) || revision <= 0 {
		return errs.New(errs.KindValidationFailed, "Runner credential revocation is invalid")
	}
	key := runnerKey(runnerID)
	read, err := repository.read(ctx, []string{key}, 0)
	if err != nil {
		return err
	}
	defer keyvalue.ClearValues(read.Values)
	indexed, err := repository.readSet(ctx, runnerID, read.Values[0], read.ReadRevision)
	if err != nil {
		return err
	}
	set := credentialSet{RunnerID: runnerID}
	var revoked indexedCredential
	hasOtherGeneration := false
	for _, item := range indexed {
		if item.record.RequestID == requestID && item.revision == revision {
			revoked = item
			continue
		}
		hasOtherGeneration = hasOtherGeneration || item.record.RequestID == requestID
		set.Digests = append(set.Digests, item.record.Digest)
	}
	if revoked.revision == 0 {
		if hasOtherGeneration {
			return credentialConflict()
		}
		return nil
	}
	change := keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key}
	if len(set.Digests) != 0 {
		value, err := recordcodec.Encode(credentialSetKind, set)
		if err != nil {
			return err
		}
		defer clear(value)
		change = keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value}
	}
	return repository.revoke(ctx,
		[]keyvalue.Condition{compare(key, read.Values[0]), {Key: revoked.key, ModRevision: revision}},
		[]keyvalue.Mutation{change, {Type: keyvalue.MutationDelete, Key: revoked.key}},
	)
}

// RevokeRetiredRunner collects the bounded credential records for a removed
// Runner. The durable registration must have settled and been removed first.
func (repository *Repository) RevokeRetiredRunner(ctx context.Context, runnerID string) error {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindRunner, runnerID); err != nil {
		return err
	}
	key := runnerKey(runnerID)
	desiredKey, lifecycleKey := runners.RunnerKey(runnerID), runners.RunnerLifecycleKey(runnerID)
	handoffKey := stageKey(runnerID)
	read, err := repository.read(ctx, []string{desiredKey, lifecycleKey, key, handoffKey}, 0)
	if err != nil {
		return err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] != nil || read.Values[1] != nil {
		return credentialConflict()
	}
	indexed, err := repository.readSet(ctx, runnerID, read.Values[2], read.ReadRevision)
	if err != nil || len(indexed) == 0 && read.Values[3] == nil {
		return err
	}
	conditions := []keyvalue.Condition{{Key: desiredKey}, {Key: lifecycleKey}, compare(key, read.Values[2])}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: key}}
	if handoff := read.Values[3]; handoff != nil {
		conditions = append(conditions, compare(handoffKey, handoff))
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: handoffKey})
	}
	for _, item := range indexed {
		conditions = append(conditions, keyvalue.Condition{Key: item.key, ModRevision: item.revision})
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: item.key})
	}
	return repository.revoke(ctx, conditions, mutations)
}

func (repository *Repository) revoke(
	ctx context.Context, conditions []keyvalue.Condition, mutations []keyvalue.Mutation,
) error {
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return credentialConflict()
	}
	return nil
}
