package agentregistration

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type MaintenanceBinding struct {
	HostID       string
	AgentID      string
	Generation   uint64
	Conditions   []keyvalue.Condition
	ReadRevision int64
}

type forwardFence struct {
	AgentID              string `json:"agent_id"`
	CredentialGeneration uint64 `json:"credential_generation"`
}

// MaintenanceBinding resolves the credential generation separately from the
// executor process generation. Restarting an Agent never changes host identity.
func (repository *Repository) MaintenanceBinding(
	ctx context.Context,
	agentID string,
	generation uint64,
) (MaintenanceBinding, error) {
	return repository.maintenanceBindingAt(ctx, agentID, generation, 0)
}

func (repository *Repository) maintenanceBindingAt(
	ctx context.Context,
	agentID string,
	generation uint64,
	revision int64,
) (MaintenanceBinding, error) {
	if ids.Validate(ids.KindAgent, agentID) != nil || generation == 0 {
		return MaintenanceBinding{}, errs.New(errs.KindValidationFailed, "Agent maintenance identity is invalid")
	}
	binding, err := hostbindings.Read(ctx, repository.store, agentID, revision)
	if err != nil {
		return MaintenanceBinding{}, err
	}
	key := localagents.LocalAgentPrimaryKey(agentID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: binding.ReadRevision},
	)
	if err != nil {
		return MaintenanceBinding{}, err
	}
	if read == nil {
		return MaintenanceBinding{}, errs.New(
			errs.KindStorageUnavailable,
			"Agent maintenance identity read is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != binding.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return MaintenanceBinding{}, errs.New(errs.KindStateConflict, "Agent maintenance identity is missing")
	}
	primary, err := localagents.DecodeLocalAgentPrimary(read.Values[0].Value)
	if err != nil {
		return MaintenanceBinding{}, err
	}
	if primary.ID != agentID || primary.Generation != generation {
		return MaintenanceBinding{}, errs.New(errs.KindStateConflict, "Agent maintenance generation changed")
	}
	conditions := append([]keyvalue.Condition(nil), binding.Conditions...)
	conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
	return MaintenanceBinding{
		HostID:       binding.Binding.HostID,
		AgentID:      agentID,
		Generation:   generation,
		Conditions:   conditions,
		ReadRevision: read.ReadRevision,
	}, nil
}

// PauseForward durably closes assignment/start for forward work only. Recovery
// remains dispatchable so Agent removal cannot strand its own cleanup.
func (repository *Repository) PauseForward(ctx context.Context, binding MaintenanceBinding) error {
	guard, found, err := repository.readForwardFence(ctx, binding)
	if err != nil || found {
		return err
	}
	raw, err := recordcodec.Encode(
		"agent_forward_fence",
		forwardFence{AgentID: binding.AgentID, CredentialGeneration: binding.Generation},
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
		return errs.New(errs.KindStateConflict, "Agent changed before forward work could be paused")
	}
	return nil
}

func (repository *Repository) readForwardFence(
	ctx context.Context,
	binding MaintenanceBinding,
) (keyvalue.Condition, bool, error) {
	key := workqueue.HostForwardFenceKey(binding.HostID)
	guard := keyvalue.Condition{Key: key}
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: binding.ReadRevision},
	)
	if err != nil {
		return guard, false, err
	}
	if read == nil || read.ReadRevision != binding.ReadRevision || len(read.Values) != 1 {
		return guard, false, errs.New(errs.KindStateConflict, "Agent forward fence read is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return guard, false, nil
	}
	fence, err := recordcodec.Decode[forwardFence](read.Values[0].Value, "agent_forward_fence")
	if err != nil {
		return guard, false, err
	}
	if fence.AgentID != binding.AgentID || fence.CredentialGeneration != binding.Generation {
		return guard, false, errs.New(
			errs.KindStateConflict,
			"Agent forward fence belongs to another lifecycle generation",
		)
	}
	guard.ModRevision = read.Values[0].ModRevision
	return guard, true, nil
}
