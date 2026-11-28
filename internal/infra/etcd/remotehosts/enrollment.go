package remotehosts

import (
	"context"
	"math"

	"github.com/AlanD20/groundplane/internal/common/remotehost"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) UpdateHost(
	ctx context.Context,
	expectedRevision int64,
	host remotehost.Host,
) (bool, error) {
	current, _, err := repository.readHost(ctx, host.ID)
	if err != nil || current.Revision != expectedRevision {
		return false, err
	}
	if !credentialUpdateAllowed(current.Host, host) {
		return false, invalidTransition()
	}
	return repository.write(ctx, expectedRevision, host, workqueue.Change{})
}

func credentialUpdateAllowed(previous, next remotehost.Host) bool {
	if previous.Name != next.Name {
		return false
	}
	if previous.Agent != nil {
		return next.Generation == previous.Generation && next.Pending == nil && next.Agent != nil &&
			previous.Agent.AgentID == next.Agent.AgentID && certificateUpdateAllowed(*previous.Agent, *next.Agent)
	}
	if next.Agent != nil {
		return false
	}
	if previous.Generation < math.MaxUint64 && next.Generation == previous.Generation+1 {
		return next.Pending != nil && next.Pending.Certificate == nil
	}
	if next.Generation != previous.Generation || previous.Pending == nil {
		return false
	}
	if next.Pending == nil {
		return true
	}
	return next.Pending.AgentID == previous.Pending.AgentID &&
		next.Pending.TokenDigest == previous.Pending.TokenDigest &&
		next.Pending.KeyDigest == previous.Pending.KeyDigest &&
		next.Pending.ExpiresAt.Equal(previous.Pending.ExpiresAt) &&
		next.Pending.Certificate != nil
}

func certificateUpdateAllowed(previous, next remotehost.AgentCredential) bool {
	if next.Renewal != nil {
		return sameCertificate(previous.Certificate, next.Certificate)
	}
	return previous.Renewal != nil && sameCertificate(*previous.Renewal, next.Certificate)
}

func sameCertificate(previous, next remotehost.CertificateCredential) bool {
	return previous.Digest == next.Digest && previous.ExpiresAt.Equal(next.ExpiresAt)
}

func (repository *Repository) AttachAgent(
	ctx context.Context,
	expectedRevision int64,
	identity remotehost.Identity,
	host remotehost.Host,
) (bool, error) {
	current, _, err := repository.readHost(ctx, host.ID)
	if err != nil || current.Revision != expectedRevision {
		return false, err
	}
	pending := current.Host.Pending
	if !identity.Valid() || current.Host.Identity(identity.AgentID) != identity || !sameHost(current.Host, host) ||
		pending == nil || pending.AgentID != identity.AgentID || pending.Certificate == nil ||
		host.Pending != nil || host.Agent == nil || host.Agent.AgentID != identity.AgentID || host.Agent.Renewal != nil ||
		!sameCertificate(*pending.Certificate, host.Agent.Certificate) {
		return false, invalidTransition()
	}
	binding, err := hostbindings.PrepareCreation(
		hostbindings.Binding{HostID: identity.HostID, AgentID: identity.AgentID},
	)
	if err != nil {
		return false, err
	}
	return repository.write(ctx, expectedRevision, host, binding)
}

func (repository *Repository) DetachAgent(
	ctx context.Context,
	expectedRevision int64,
	identity remotehost.Identity,
	host remotehost.Host,
) (bool, error) {
	return repository.detachAgent(ctx, expectedRevision, identity, host, workqueue.Change{})
}

func (repository *Repository) detachAgent(
	ctx context.Context,
	expectedRevision int64,
	identity remotehost.Identity,
	host remotehost.Host,
	publication workqueue.Change,
) (bool, error) {
	current, readRevision, err := repository.readHost(ctx, host.ID)
	if err != nil || current.Revision != expectedRevision {
		return false, err
	}
	if !identity.Valid() || current.Host.Agent == nil ||
		current.Host.Identity(current.Host.Agent.AgentID) != identity ||
		!sameHost(current.Host, host) ||
		host.Agent != nil ||
		host.Pending != nil {
		return false, invalidTransition()
	}
	binding, err := hostbindings.Read(ctx, repository.store, identity.AgentID, readRevision)
	if err != nil {
		return false, err
	}
	if binding.Binding.HostID != identity.HostID {
		return false, recordcodec.CorruptRecord()
	}
	executor := workexecution.Identity{Executor: workexecution.Agent, HostID: host.ID}
	change, err := repository.disableExecutor(ctx, executor, identity.AgentID, readRevision)
	if err != nil {
		return false, err
	}
	change.Conditions = append(change.Conditions, binding.Conditions...)
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: workqueue.SlotsPrefix(executor), Prefix: true},
		keyvalue.Condition{Key: workqueue.HostClaimsPrefix(host.ID), Prefix: true},
		keyvalue.Condition{Key: workqueue.HostEffectsPrefix(host.ID), Prefix: true},
		keyvalue.Condition{Key: workqueue.HostForwardFenceKey(host.ID)},
	)
	change.Mutations = append(change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: hostbindings.AgentKey(identity.AgentID)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: hostbindings.HostKey(identity.HostID)},
	)
	change.Conditions = append(change.Conditions, publication.Conditions...)
	change.Mutations = append(change.Mutations, publication.Mutations...)
	return repository.write(ctx, expectedRevision, host, change)
}

func (repository *Repository) disableExecutor(
	ctx context.Context,
	identity workexecution.Identity,
	agentID string,
	readRevision int64,
) (workqueue.Change, error) {
	key := workqueue.ExecutorKey(identity)
	value, _, err := repository.read(ctx, key, readRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if value == nil {
		return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key}}}, nil
	}
	defer clear(value.Value)
	record, err := workqueue.DecodeExecutor(value.Value)
	if err != nil || record.Identity.Executor != identity.Executor || record.Identity.HostID != identity.HostID {
		return workqueue.Change{}, recordcodec.CorruptRecord()
	}
	if !record.Ready {
		// Keep the last process generation so replacement cannot reuse it.
		return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: value.ModRevision}}}, nil
	}
	if record.Identity.AgentID != agentID {
		return workqueue.Change{}, recordcodec.CorruptRecord()
	}
	return workqueue.PrepareAvailability(workqueue.ExecutorSnapshot{Record: record, Revision: value.ModRevision},
		record.Identity, false, record.Capacity, record.UnavailableOperations)
}

func sameHost(previous, next remotehost.Host) bool {
	return previous.ID == next.ID && previous.Name == next.Name && previous.Generation == next.Generation
}

func invalidTransition() error {
	return errs.New(errs.KindStateConflict, "Remote Host enrollment transition is not allowed")
}
