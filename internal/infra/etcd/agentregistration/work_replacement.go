package agentregistration

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Replacement admission remains closed across all credential generations of
// one captured update, including Controller restart and failed restoration.
func (repository *Repository) PauseReplacement(ctx context.Context, agentID string, original, current uint64) error {
	binding, err := repository.replacementBinding(ctx, agentID, original, current, 0)
	if err != nil {
		return err
	}
	guard, found, err := repository.readForwardFence(ctx, binding)
	if err != nil || found {
		return err
	}
	raw, err := recordcodec.Encode(
		"agent_forward_fence",
		forwardFence{AgentID: agentID, CredentialGeneration: original},
	)
	if err != nil {
		return err
	}
	defer clear(raw)
	conditions := append([]keyvalue.Condition(nil), binding.Conditions...)
	conditions = append(conditions, guard)
	result, err := repository.store.Transact(
		ctx,
		conditions,
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: guard.Key, Value: raw}},
	)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Agent changed before replacement admission was fenced")
	}
	return nil
}

// PrepareResumeReplacement joins admission reopening to native settlement.
// A qualified candidate cannot accept new work between a late Abort and cleanup.
func (repository *Repository) PrepareResumeReplacement(
	ctx context.Context,
	agentID string,
	original, current uint64,
	revision int64,
) (workqueue.Change, error) {
	binding, err := repository.replacementBinding(ctx, agentID, original, current, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	guard, found, err := repository.readForwardFence(ctx, binding)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !found {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Agent replacement settlement has no retained forward fence",
		)
	}
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{
			Keys:     []string{localagents.LocalAgentPrimaryKey(agentID)},
			Revision: binding.ReadRevision,
		},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil {
		return workqueue.Change{}, errs.New(
			errs.KindStorageUnavailable,
			"Agent replacement readiness snapshot is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != binding.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Agent replacement readiness is missing")
	}
	primary, err := localagents.DecodeLocalAgentPrimary(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if primary.ID != agentID || primary.Generation != current || primary.Phase != localagents.LocalAgentPhaseReady {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Agent replacement has not settled at its qualified generation",
		)
	}
	conditions := append([]keyvalue.Condition(nil), binding.Conditions...)
	conditions = append(conditions, guard)
	return workqueue.Change{
		Conditions: conditions,
		Mutations:  []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: guard.Key}},
	}, nil
}

func (repository *Repository) replacementBinding(
	ctx context.Context,
	agentID string,
	original, current uint64,
	revision int64,
) (MaintenanceBinding, error) {
	if original == 0 || original > ^uint64(0)-2 || current < original || current > original+2 {
		return MaintenanceBinding{}, errs.New(
			errs.KindValidationFailed,
			"Agent replacement admission generations are invalid",
		)
	}
	binding, err := repository.maintenanceBindingAt(ctx, agentID, current, revision)
	if err != nil {
		return MaintenanceBinding{}, err
	}
	// The fence belongs to the original lifecycle operation; the primary
	// comparison still refers to the exact current credential generation.
	binding.Generation = original
	return binding, nil
}
