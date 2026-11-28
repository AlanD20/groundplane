// Package hostplacement resolves the explicitly selected host for new work.
package hostplacement

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Snapshot ties stable placement and host platform to the Agent observed while
// preparing the work. Publication must compare Conditions atomically with its
// writes. Assignment and Start still require their own executor authorization.
type Snapshot struct {
	Placement         workexecution.Placement
	Identity          workexecution.Identity
	ProcessGeneration [16]byte
	Platform          hostbindings.Platform
	ReadRevision      int64
	Conditions        []keyvalue.Condition
}

type Resolver struct {
	reader hostbindings.Reader
}

func New(reader hostbindings.Reader) (*Resolver, error) {
	if reader == nil {
		return nil, errs.New(errs.KindValidationFailed, "host placement requires a snapshot reader")
	}
	return &Resolver{reader: reader}, nil
}

// Resolve requires the enclosing work plan's revision. It checks every required
// operation against the selected host's Agent at that revision.
func (resolver *Resolver) Resolve(
	ctx context.Context,
	hostID string,
	revision int64,
	operations ...workexecution.Operation,
) (Snapshot, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return Snapshot{}, err
	}
	if ids.Validate(ids.KindHost, hostID) != nil || revision <= 0 || len(operations) == 0 {
		return Snapshot{}, errs.New(
			errs.KindValidationFailed,
			"host placement requires a stable host identity, planning revision and work operations",
		)
	}
	for _, operation := range operations {
		if !workexecution.ValidOperation(operation) {
			return Snapshot{}, errs.New(errs.KindValidationFailed, "host placement requires recognized work operations")
		}
	}

	executorKey := workqueue.ExecutorKey(workexecution.Identity{Executor: workexecution.Agent, HostID: hostID})
	read, err := resolver.reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{executorKey}, Revision: revision})
	if err != nil {
		return Snapshot{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return Snapshot{}, errs.New(errs.KindInternal, "host executor read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return Snapshot{}, errs.New(errs.KindAgentReadinessChanged, "selected host has no registered executor")
	}
	if value.Key != executorKey || value.ModRevision <= 0 || value.ModRevision > revision {
		return Snapshot{}, errs.New(errs.KindInternal, "host executor revision is inconsistent")
	}
	executor, err := workqueue.DecodeExecutor(value.Value)
	if err != nil {
		return Snapshot{}, err
	}
	if executor.Identity.Executor != workexecution.Agent || executor.Identity.HostID != hostID {
		return Snapshot{}, errs.New(errs.KindInternal, "host executor identity is inconsistent")
	}
	binding, err := hostbindings.Read(ctx, resolver.reader, executor.Identity.AgentID, revision)
	if err != nil {
		return Snapshot{}, err
	}
	if binding.Binding.HostID != hostID {
		return Snapshot{}, errs.New(
			errs.KindStateConflict,
			"selected host does not match the executor's durable host binding",
		)
	}
	for _, operation := range operations {
		if !executor.Allows(operation) {
			return Snapshot{}, errs.New(
				errs.KindAgentReadinessChanged,
				"selected host is not ready for required work operation "+string(operation),
			)
		}
	}
	platform, err := hostbindings.ReadPlatform(ctx, resolver.reader, hostID, revision)
	if err != nil {
		return Snapshot{}, err
	}
	if platform.Revision == 0 {
		return Snapshot{}, errs.New(
			errs.KindAgentReadinessChanged,
			"selected host has not reported its execution platform",
		)
	}
	return Snapshot{
		Placement:         workexecution.Placement{Executor: workexecution.Agent, HostID: hostID},
		Identity:          executor.Identity,
		ProcessGeneration: executor.ProcessGeneration,
		Platform:          platform.Record,
		ReadRevision:      revision,
		Conditions: append(binding.Conditions,
			keyvalue.Condition{Key: executorKey, ModRevision: value.ModRevision},
			keyvalue.Condition{Key: hostbindings.PlatformKey(hostID), ModRevision: platform.Revision}),
	}, nil
}
