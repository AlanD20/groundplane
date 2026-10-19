package agentregistration

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResumeForward is only a pre-revocation recovery. A same-value primary write
// invalidates a delayed BeginDelete CAS before the admission fence is released.
// Once credentials are revoked, removal must finish; it cannot be undone here.
func (repository *Repository) ResumeForward(ctx context.Context, agentID string, generation uint64) error {
	binding, err := repository.MaintenanceBinding(ctx, agentID, generation)
	if err != nil {
		return err
	}
	guard, found, err := repository.readForwardFence(ctx, binding)
	if err != nil || !found {
		return err
	}
	key := localagents.LocalAgentPrimaryKey(agentID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: binding.ReadRevision},
	)
	if err != nil {
		return err
	}
	if read == nil {
		return errs.New(errs.KindStorageUnavailable, "Agent maintenance recovery snapshot is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != binding.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "Agent maintenance recovery identity is missing")
	}
	primary, err := localagents.DecodeLocalAgentPrimary(read.Values[0].Value)
	if err != nil {
		return err
	}
	if primary.ID != agentID || primary.Generation != generation ||
		primary.Phase == localagents.LocalAgentPhaseDeleting {
		return errs.New(errs.KindStateConflict, "revoked Agent removal cannot reopen forward work")
	}
	conditions := append([]keyvalue.Condition(nil), binding.Conditions...)
	conditions = append(conditions, guard)
	result, err := repository.store.Transact(ctx, conditions, []keyvalue.Mutation{
		{
			Type:  keyvalue.MutationPut,
			Key:   key,
			Value: read.Values[0].Value,
		}, {Type: keyvalue.MutationDelete, Key: guard.Key},
	})
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Agent changed before pre-removal recovery settled")
	}
	return nil
}
