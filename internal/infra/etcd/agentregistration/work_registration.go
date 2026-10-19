package agentregistration

import (
	"context"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RegisterWorkProcess is called by the authenticated channel owner. New processes
// start unavailable: registration never releases old slots or proves old effects
// stopped. A receipt makes uncertain-response retry and reconnect idempotent,
// while a retired process cannot become current again.
func (repository *Repository) RegisterWorkProcess(ctx context.Context, agentID string,
	credentialGeneration uint64, process [16]byte, capacity uint16, operations []workexecution.Operation,
) (workexecution.Identity, error) {
	if credentialGeneration == 0 || process == [16]byte{} {
		return workexecution.Identity{}, agentCredentialNotFound()
	}
	binding, err := hostbindings.Read(ctx, repository.store, agentID, 0)
	if err != nil {
		return workexecution.Identity{}, err
	}
	identity := workexecution.Identity{Executor: workexecution.Agent, HostID: binding.Binding.HostID, AgentID: agentID}
	receiptKey := "/v1/execution/agent-processes/" + identity.HostID + "/" + hex.EncodeToString(process[:])
	keys := []string{localagents.LocalAgentPrimaryKey(agentID), workqueue.ExecutorKey(identity), receiptKey}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: binding.ReadRevision})
	if err != nil {
		return workexecution.Identity{}, err
	}
	if read == nil || read.ReadRevision != binding.ReadRevision || len(read.Values) != len(keys) {
		return workexecution.Identity{}, errs.New(errs.KindInternal, "Agent process registration read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := append([]keyvalue.Condition(nil), binding.Conditions...)
	for index, value := range read.Values {
		condition := keyvalue.Condition{Key: keys[index]}
		if value != nil {
			if value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
				return workexecution.Identity{}, errs.New(
					errs.KindInternal,
					"Agent process registration revision is invalid",
				)
			}
			condition.ModRevision = value.ModRevision
		}
		conditions = append(conditions, condition)
	}
	if read.Values[0] == nil {
		return workexecution.Identity{}, agentCredentialNotFound()
	}
	credential, err := localagents.DecodeLocalAgentPrimary(read.Values[0].Value)
	if err != nil {
		return workexecution.Identity{}, err
	}
	if credential.ID != agentID || credential.Generation != credentialGeneration ||
		credential.Phase == localagents.LocalAgentPhaseDeleting {
		return workexecution.Identity{}, agentCredentialNotFound()
	}
	var previous workqueue.ExecutorSnapshot
	if value := read.Values[1]; value != nil {
		previous.Record, err = workqueue.DecodeExecutor(value.Value)
		if err != nil {
			return workexecution.Identity{}, err
		}
		previous.Revision = value.ModRevision
	}
	if receipt := read.Values[2]; receipt != nil {
		saved, err := recordcodec.Decode[workexecution.Identity](receipt.Value, "agent-work-process")
		if err != nil || saved.Validate() != nil {
			return workexecution.Identity{}, errs.New(
				errs.KindInternal,
				"Agent process registration receipt is corrupt",
			)
		}
		if previous.Revision == 0 || previous.Record.Identity != saved ||
			previous.Record.ProcessGeneration != process ||
			saved.AgentID != agentID ||
			saved.HostID != binding.Binding.HostID {
			return workexecution.Identity{}, errs.New(
				errs.KindStateConflict,
				"Agent process has been replaced; it cannot reconnect as the current executor",
			)
		}
		if !slices.Equal(previous.Record.Operations, operations) {
			return workexecution.Identity{}, errs.New(
				errs.KindStateConflict,
				"Agent process operation catalog changed during reconnect",
			)
		}
		// A reconnect preserves the process generation, but admission waits for
		// the new channel to reconcile its durable deliveries before dispatch.
		change, err := workqueue.PrepareAvailability(
			previous,
			saved,
			false,
			capacity,
			previous.Record.UnavailableOperations,
		)
		if err != nil {
			return workexecution.Identity{}, err
		}
		defer keyvalue.ClearMutationValues(change.Mutations)
		result, err := repository.store.Transact(ctx, conditions, change.Mutations)
		keyvalue.ClearValues(result.FailureReads)
		if err != nil {
			return workexecution.Identity{}, err
		}
		if !result.Succeeded {
			return workexecution.Identity{}, errs.New(errs.KindStateConflict, "Agent reconnect ownership changed")
		}
		return saved, nil
	}
	change, registered, err := workqueue.PrepareRegistration(previous, workqueue.ExecutorRecord{
		Identity: identity, ProcessGeneration: process, Capacity: capacity, Operations: operations,
	})
	if err != nil {
		return workexecution.Identity{}, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	receipt, err := recordcodec.Encode("agent-work-process", registered.Identity)
	if err != nil {
		return workexecution.Identity{}, err
	}
	defer clear(receipt)
	mutations := append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: receiptKey, Value: receipt},
	)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return workexecution.Identity{}, err
	}
	if !result.Succeeded {
		return workexecution.Identity{}, errs.New(
			errs.KindStateConflict,
			"Agent process registration changed; reconnect to resolve its saved identity",
		)
	}
	return registered.Identity, nil
}
