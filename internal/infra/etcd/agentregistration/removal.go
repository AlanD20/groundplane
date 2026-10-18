package agentregistration

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	localagentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) BeginDelete(
	ctx context.Context,
	agentID string,
	generation uint64,
	revision int64,
) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	evidence, err := repository.readSingleton(ctx)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	if evidence.record.ID != agentID || evidence.record.Generation != generation ||
		evidence.primaryRevision != revision {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindStateConflict,
			"local Agent generation or revision changed",
		)
	}
	if evidence.record.Phase == localagentrecord.LocalAgentPhaseDeleting {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
			Record: evidence.record, Revision: evidence.primaryRevision,
			ReadRevision: evidence.readRevision,
		}, nil
	}
	if evidence.digest == nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindInternal,
			"active local Agent is missing its credential index",
		)
	}
	replacement := localagentrecord.CloneLocalAgentRecord(evidence.record)
	replacement.Phase = localagentrecord.LocalAgentPhaseDeleting
	replacement.TokenDigest = ""
	primaryValue, err := localagentrecord.EncodeLocalAgentPrimary(replacement)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	defer clear(primaryValue)
	unavailable, err := repository.prepareWorkUnavailable(ctx, agentID, evidence.readRevision)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	defer etcdstore.ClearMutationValues(unavailable.Mutations)
	conditions := append(unavailable.Conditions, []etcdstore.Condition{
		{Key: localagentrecord.LocalAgentPrimaryKey(agentID), ModRevision: evidence.primary.ModRevision},
		{Key: evidence.digest.Key, ModRevision: evidence.digest.ModRevision},
	}...)
	mutations := append(unavailable.Mutations, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: localagentrecord.LocalAgentPrimaryKey(agentID), Value: primaryValue},
		{Type: etcdstore.MutationDelete, Key: evidence.digest.Key},
	}...)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	etcdstore.ClearValues(result.FailureReads)
	if !result.Succeeded {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindStateConflict,
			"local Agent deletion state changed",
		)
	}
	return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *Repository) Delete(
	ctx context.Context,
	agentID string,
	generation uint64,
	revision int64,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	evidence, err := repository.readSingleton(ctx)
	if errorsIsAgentNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if evidence.record.ID != agentID || evidence.record.Generation != generation ||
		evidence.primaryRevision != revision || evidence.record.Phase != localagentrecord.LocalAgentPhaseDeleting ||
		evidence.digest != nil {
		return errs.New(errs.KindStateConflict, "local Agent is not at the deletable generation and revision")
	}
	conditions := []etcdstore.Condition{
		{Key: localagentrecord.LocalAgentSingletonKey, ModRevision: evidence.singleton.ModRevision},
		{Key: localagentrecord.LocalAgentPrimaryKey(agentID), ModRevision: evidence.primary.ModRevision},
		{Key: localagentrecord.LocalAgentOwnerKey(agentID), ModRevision: evidence.owner.ModRevision},
		{Key: localagentrecord.LocalAgentConfigKey(agentID), ModRevision: evidence.config.ModRevision},
		{Key: localagentrecord.LocalAgentTokenKey(agentID), ModRevision: evidence.token.ModRevision},
	}
	mutations := make([]etcdstore.Mutation, len(conditions))
	for index, condition := range conditions {
		mutations[index] = etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: condition.Key}
	}
	binding, err := hostbindings.Read(ctx, repository.store, agentID, evidence.readRevision)
	if err != nil {
		return err
	}
	conditions = append(conditions, binding.Conditions...)
	forwardGuard, found, err := repository.readForwardFence(ctx, MaintenanceBinding{HostID: binding.Binding.HostID,
		AgentID: agentID, Generation: generation, ReadRevision: evidence.readRevision})
	if err != nil {
		return err
	}
	if !found {
		return errs.New(errs.KindStateConflict, "Agent removal requires its durable forward-work fence")
	}
	conditions = append(conditions, forwardGuard)
	// Final removal follows settlement, not merely container termination. Host
	// reservations survive process loss and cannot be silently reassigned.
	conditions = append(conditions,
		etcdstore.Condition{Key: workqueue.SlotsPrefix(workexecution.Identity{Executor: workexecution.Agent, HostID: binding.Binding.HostID}), Prefix: true},
		etcdstore.Condition{Key: workqueue.HostClaimsPrefix(binding.Binding.HostID), Prefix: true},
		etcdstore.Condition{Key: workqueue.HostEffectsPrefix(binding.Binding.HostID), Prefix: true},
	)
	mutations = append(mutations,
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: hostbindings.AgentKey(agentID)},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: hostbindings.HostKey(binding.Binding.HostID)},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: workqueue.HostForwardFenceKey(binding.Binding.HostID)},
	)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return err
	}
	etcdstore.ClearValues(result.FailureReads)
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "local Agent deletion state changed")
	}
	return nil
}
