package agent

import (
	"context"
	"errors"
	"sync"

	"github.com/AlanD20/groundplane/internal/agent/attachruntime"
	"github.com/AlanD20/groundplane/internal/agent/backingruntime"
	"github.com/AlanD20/groundplane/internal/agent/backupruntime"
	"github.com/AlanD20/groundplane/internal/agent/componentruntime"
	"github.com/AlanD20/groundplane/internal/agent/entryruntime"
	"github.com/AlanD20/groundplane/internal/agent/environmentruntime"
	"github.com/AlanD20/groundplane/internal/agent/hierarchyruntime"
	"github.com/AlanD20/groundplane/internal/agent/lifecycleruntime"
	"github.com/AlanD20/groundplane/internal/agent/networkruntime"
	"github.com/AlanD20/groundplane/internal/agent/releaseruntime"
	"github.com/AlanD20/groundplane/internal/agent/removalruntime"
	"github.com/AlanD20/groundplane/internal/agent/resolverruntime"
	"github.com/AlanD20/groundplane/internal/agent/scriptruntime"
	"github.com/AlanD20/groundplane/internal/agent/volumeruntime"
	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type executionOutput struct {
	resolver             *workexecution.Attempt
	backing              *workexecution.Attempt
	environmentProvision *workexecution.Attempt
	backup               *workexecution.Attempt
	removalRecovery      *workexecution.Attempt
	lifecycle            *workexecution.Attempt
	provisionRecovery    *workexecution.Attempt
	provision            *attachruntime.Execution
	entry                *workexecution.Attempt
	component            *workexecution.Attempt
	network              *workexecution.Attempt
	volume               *workexecution.Attempt
	attachRecovery       *workexecution.Attempt
	attach               *attachexecution.RemovalProgress
	attachFailure        *workexecution.Attempt
	environmentFailure   *workexecution.Attempt
	environment          *hierarchyexecution.EnvironmentProgress
	scriptFailure        *agentpb.ScriptWorkFailureAck
	scriptRecovery       *agentpb.ScriptRecoveryCompletionAck
	attempt              workexecution.Attempt
	result               releaseruntime.ExecutionResult
	recovery             *releaseexecution.RecoveryReport
	err                  error
	script               *agentpb.ScriptWorkCompletionAck
}

type executionExchange interface {
	resolverruntime.Exchange
	backingruntime.Exchange
	environmentruntime.Exchange
	backupruntime.Exchange
	removalruntime.RecoveryExchange
	lifecycleruntime.Exchange
	attachruntime.Exchange
	entryruntime.Exchange
	componentruntime.Exchange
	networkruntime.Exchange
	volumeruntime.Exchange
	hierarchyruntime.AttachExchange
	hierarchyruntime.Exchange
	releaseruntime.Exchange
	releaseruntime.RecoveryExchange
	scriptruntime.WorkerExchange
}

type activeExecution struct {
	resolver             *agentpb.ResolverWorkAssignment
	backing              *agentpb.BackingProvisionAssignment
	environmentProvision *agentpb.EnvironmentProvisionAssignment
	ctx                  context.Context
	cancel               context.CancelFunc
	cancelRequested      bool
	backup               *agentpb.BackupWorkAssignment
	backupRecovery       *agentpb.BackupRecoveryAssignment
	removalRecovery      *agentpb.ServiceRemovalRecoveryAssignment
	lifecycle            *agentpb.ServiceLifecycleAssignment
	provisionRecovery    *agentpb.AttachProvisionRecoveryAssignment
	provision            *agentpb.AttachProvisionAssignment
	entry                *agentpb.EntryConfigurationWorkAssignment
	component            *agentpb.ComponentWorkAssignment
	network              *agentpb.NetworkWorkAssignment
	volume               *agentpb.VolumeWorkAssignment
	attachRecovery       *agentpb.AttachRecoveryAssignment
	attach               *agentpb.AttachRemovalAssignment
	environment          *agentpb.EnvironmentRemovalAssignment
	scriptRecovery       *agentpb.ScriptRecoveryAssignment
	attempt              workexecution.Attempt
	forward              *agentpb.ReleaseWorkAssignment
	recovery             *agentpb.ReleaseRecoveryAssignment
	script               bool
}

// executionWorkers owns shared in-process exclusivity and capacity. The Controller
// owns durable assignment, conflicts and retries; this executor never requeues
// a failed action. Closing a stream cancels and joins every local execution
// before another stream can begin reconciling the same attempts.
type executionWorkers struct {
	resolvers       *resolverruntime.Executor
	backings        *backingruntime.Executor
	environments    *environmentruntime.Executor
	backups         *backupruntime.Executor
	removalRecovery *removalruntime.Runtime
	lifecycles      *lifecycleruntime.Executor
	mu              sync.Mutex
	wait            sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
	identity        workexecution.Identity
	executor        *releaseruntime.Executor
	recovery        *releaseruntime.RecoveryExecutor
	scripts         *scriptruntime.Executor
	hierarchy       *hierarchyruntime.Executor
	networks        *networkruntime.Executor
	components      *componentruntime.Executor
	entries         *entryruntime.Executor
	volumes         *volumeruntime.Executor
	attachRemoval   *hierarchyruntime.AttachExecutor
	attachProvision *attachruntime.Executor
	exchange        executionExchange
	ownership       *agentownership.Lease
	limit           int
	closed          bool
	active          map[string]activeExecution
	outputs         chan executionOutput
}

func newExecutionWorkers(ctx context.Context, limit int, identity workexecution.Identity, executor *releaseruntime.Executor, recovery *releaseruntime.RecoveryExecutor, scripts *scriptruntime.Executor, hierarchy *hierarchyruntime.Executor, attachRemoval *hierarchyruntime.AttachExecutor, volumes *volumeruntime.Executor, networks *networkruntime.Executor, components *componentruntime.Executor, entries *entryruntime.Executor, attachProvision *attachruntime.Executor, lifecycles *lifecycleruntime.Executor, removalRecovery *removalruntime.Runtime, backups *backupruntime.Executor, environments *environmentruntime.Executor, backings *backingruntime.Executor, resolvers *resolverruntime.Executor, exchange executionExchange, ownership *agentownership.Lease) (*executionWorkers, error) {
	if resolvers == nil {
		return nil, errs.New(errs.KindInternal, "Agent DNS resolver executor is required")
	}
	if environments == nil || backings == nil {
		return nil, errs.New(errs.KindInternal, "Agent Environment provisioning executor is required")
	}
	if backups == nil {
		return nil, errs.New(errs.KindInternal, "Agent Backup executor is required")
	}
	if removalRecovery == nil || lifecycles == nil || components == nil || entries == nil || attachProvision == nil {
		return nil, errs.New(errs.KindInternal, "Agent Component executor is required")
	}
	if ctx == nil || limit < 1 || limit > 256 || identity.Validate() != nil || identity.Executor != workexecution.Agent || executor == nil || recovery == nil || scripts == nil || hierarchy == nil || attachRemoval == nil || volumes == nil || networks == nil || exchange == nil {
		return nil, errs.New(errs.KindInternal, "Agent execution workers require authenticated operation handlers and bounded capacity")
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	workerContext, cancel := context.WithCancel(ctx)
	return &executionWorkers{resolvers: resolvers, backings: backings, environments: environments, backups: backups, removalRecovery: removalRecovery, lifecycles: lifecycles, ctx: workerContext, cancel: cancel, identity: identity, executor: executor, recovery: recovery, scripts: scripts, hierarchy: hierarchy, attachRemoval: attachRemoval, volumes: volumes, networks: networks, exchange: exchange,
		components: components, entries: entries, attachProvision: attachProvision, limit: limit, ownership: ownership, active: make(map[string]activeExecution), outputs: make(chan executionOutput, limit)}, nil
}

func (workers *executionWorkers) submit(delivery *agentpb.ReleaseWorkAssignment, available int) error {
	attempt, _, err := workexecution.DecodeReleaseDelivery(delivery, workers.identity)
	if err != nil {
		return err
	}
	if attempt.Direction != workexecution.Forward {
		return errs.New(errs.KindValidationFailed, "forward Release delivery cannot authorize recovery")
	}
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "Release worker session is closed")
	}
	if active, exists := workers.active[attempt.WorkItemID]; exists {
		if active.forward == nil || !proto.Equal(active.forward, delivery) {
			return errs.New(errs.KindStateConflict, "Release work already has a different active delivery")
		}
		return nil
	}
	if available <= 0 || len(workers.active) >= workers.limit {
		return errs.New(errs.KindStateConflict, "Release execution capacity is exhausted")
	}
	owned := proto.CloneOf(delivery)
	execution, err := workers.ownership.Begin(attempt)
	if err != nil {
		return err
	}
	workers.active[attempt.WorkItemID] = activeExecution{attempt: attempt, forward: owned}
	workers.wait.Add(1)
	go workers.execute(attempt, owned, execution)
	return nil
}

func (workers *executionWorkers) execute(attempt workexecution.Attempt, delivery *agentpb.ReleaseWorkAssignment, execution *agentownership.Execution) {
	defer workers.wait.Done()
	result, err := workers.executor.Execute(workers.executionContext(attempt), delivery, workers.identity, workers.exchange)
	err = errors.Join(err, execution.Close())
	// Keep the local slot until the session consumes this result. Otherwise a
	// slow consumer can accumulate arbitrarily many undelivered completions.
	select {
	case workers.outputs <- executionOutput{attempt: attempt, result: result, err: err}:
	case <-workers.ctx.Done():
	}
}

func (workers *executionWorkers) consume(output executionOutput) error {
	workers.mu.Lock()
	defer workers.mu.Unlock()
	active, exists := workers.active[output.attempt.WorkItemID]
	if !exists {
		return errs.New(errs.KindStateConflict, "operation result has no local execution slot")
	}
	if !workexecution.SameAssignment(active.attempt, output.attempt) {
		return errs.New(errs.KindStateConflict, "operation result differs from its local execution slot")
	}
	if active.backup != nil || active.backupRecovery != nil {
		return workers.consumeBackup(output)
	}
	if output.backup != nil {
		return errs.New(errs.KindStateConflict, "Backup result belongs to another active operation")
	}
	if active.provisionRecovery != nil {
		if output.err == nil {
			if output.provisionRecovery == nil || output.provisionRecovery.Validate() != nil || output.provisionRecovery.FinishedAt == nil ||
				output.provisionRecovery.Result == nil || !workexecution.SameAssignment(*output.provisionRecovery, output.attempt) {
				return errs.New(errs.KindStateConflict, "Attach inspection result requires a saved terminal acknowledgement")
			}
			if err := workers.attachProvision.RetireRecovery(workers.ctx, *output.provisionRecovery); err != nil {
				return err
			}
		}
		workers.retireLocalExecution(output.attempt.WorkItemID)
		return nil
	}
	if output.provisionRecovery != nil {
		return errs.New(errs.KindStateConflict, "Attach inspection result belongs to another active operation")
	}
	if active.provision != nil {
		if err := workers.consumeAttachProvision(active, output); err != nil {
			return err
		}
		workers.retireLocalExecution(output.attempt.WorkItemID)
		return nil
	}
	if output.provision != nil {
		return errs.New(errs.KindStateConflict, "Attach provisioning result belongs to another active operation")
	}
	if active.entry != nil {
		if output.err == nil {
			if output.entry == nil || output.entry.Validate() != nil || output.entry.FinishedAt == nil ||
				output.entry.Result == nil || !workexecution.SameAssignment(*output.entry, output.attempt) {
				return errs.New(errs.KindStateConflict, "Entry result requires a saved terminal acknowledgement")
			}
			if err := workers.entries.Retire(workers.ctx, *output.entry); err != nil {
				return err
			}
		}
		workers.retireLocalExecution(output.attempt.WorkItemID)
		return nil
	}
	if output.entry != nil {
		return errs.New(errs.KindStateConflict, "Entry result belongs to another active operation")
	}

	if active.environmentProvision != nil {
		if output.err == nil {
			if output.environmentProvision == nil || output.environmentProvision.Validate() != nil || output.environmentProvision.FinishedAt == nil ||
				output.environmentProvision.Result == nil || !workexecution.SameAssignment(*output.environmentProvision, output.attempt) {
				return errs.New(errs.KindStateConflict, "Environment provisioning result requires its accepted terminal receipt")
			}
			if err := workers.environments.Retire(workers.ctx, *output.environmentProvision); err != nil {
				return err
			}
		}
		workers.retireLocalExecution(output.attempt.WorkItemID)
		return nil
	}
	if output.environmentProvision != nil {
		return errs.New(errs.KindStateConflict, "Environment provisioning result belongs to another operation")
	}
	if handled, err := workers.finishResolver(output, active); handled {
		return err
	}
	if handled, err := workers.finishBacking(output, active); handled {
		return err
	}
	if active.component != nil {
		if output.err == nil {
			if output.component == nil || output.component.Validate() != nil || output.component.FinishedAt == nil || output.component.Result == nil || !workexecution.SameAssignment(*output.component, output.attempt) {
				return errs.New(errs.KindStateConflict, "Component result requires a saved terminal acknowledgement")
			}
			if err := workers.components.Retire(workers.ctx, *output.component); err != nil {
				return err
			}
		}
		workers.retireLocalExecution(output.attempt.WorkItemID)
		return nil
	}
	if output.component != nil {
		return errs.New(errs.KindStateConflict, "Component result belongs to another active operation")
	}
	if active.network != nil {
		if output.err == nil {
			if output.network == nil || output.network.Validate() != nil || output.network.FinishedAt == nil || output.network.Result == nil ||
				!workexecution.SameAssignment(*output.network, output.attempt) {
				return errs.New(errs.KindStateConflict, "Network result requires a saved terminal acknowledgement")
			}
			if err := workers.networks.Retire(workers.ctx, *output.network); err != nil {
				return err
			}
		}
		workers.retireLocalExecution(output.attempt.WorkItemID)
		return nil
	}
	if output.network != nil {
		return errs.New(errs.KindStateConflict, "Network result belongs to another active operation")
	}
	if active.removalRecovery != nil {
		if output.err == nil {
			if output.removalRecovery == nil || output.removalRecovery.Validate() != nil || output.removalRecovery.FinishedAt == nil || output.removalRecovery.Result == nil || !workexecution.SameAssignment(*output.removalRecovery, output.attempt) {
				return errs.New(errs.KindStateConflict, "service removal recovery requires a saved terminal acknowledgement")
			}
			if err := workers.removalRecovery.Retire(workers.ctx, *output.removalRecovery); err != nil {
				return err
			}
		}
		workers.retireLocalExecution(output.attempt.WorkItemID)
		return nil
	}
	if output.removalRecovery != nil {
		return errs.New(errs.KindStateConflict, "service removal recovery result belongs to another active operation")
	}
	if active.lifecycle != nil {
		if output.err == nil {
			if output.lifecycle == nil || output.lifecycle.Validate() != nil || output.lifecycle.FinishedAt == nil || output.lifecycle.Result == nil ||
				!workexecution.SameAssignment(*output.lifecycle, output.attempt) {
				return errs.New(errs.KindStateConflict, "Service lifecycle result requires a saved terminal acknowledgement")
			}
			if err := workers.lifecycles.Retire(workers.ctx, *output.lifecycle); err != nil {
				return err
			}
		}
		workers.retireLocalExecution(output.attempt.WorkItemID)
		return nil
	}
	if output.lifecycle != nil {
		return errs.New(errs.KindStateConflict, "Service lifecycle result belongs to another active operation")
	}
	if active.volume != nil {
		if output.err == nil {
			if output.volume == nil || output.volume.Validate() != nil || output.volume.FinishedAt == nil || output.volume.Result == nil ||
				!workexecution.SameAssignment(*output.volume, output.attempt) {
				return errs.New(errs.KindStateConflict, "Volume result requires a saved terminal acknowledgement")
			}
			if err := workers.volumes.Retire(workers.ctx, *output.volume); err != nil {
				return err
			}
		}
		workers.retireLocalExecution(output.attempt.WorkItemID)
		return nil
	}
	if output.volume != nil {
		return errs.New(errs.KindStateConflict, "Volume result belongs to another active operation")
	}
	if active.attachRecovery == nil && output.attachRecovery != nil {
		return errs.New(errs.KindStateConflict, "Attach recovery result belongs to another operation")
	}
	if active.attachRecovery != nil {
		defer attachexecution.ClearRecoveryAssignment(active.attachRecovery)
	}
	if active.attachRecovery != nil && output.err == nil {
		if output.attachRecovery == nil || output.attachRecovery.FinishedAt == nil || output.attachRecovery.Result == nil ||
			!workexecution.SameAssignment(*output.attachRecovery, output.attempt) || output.attach != nil || output.attachFailure != nil ||
			output.environment != nil || output.environmentFailure != nil || output.script != nil || output.scriptFailure != nil ||
			output.scriptRecovery != nil || output.recovery != nil || output.result.Report != nil || output.result.Failure != nil {
			return errs.New(errs.KindStateConflict, "Attach recovery requires its acknowledged inspection result")
		}
	}
	if active.attach == nil && (output.attach != nil || output.attachFailure != nil) {
		return errs.New(errs.KindStateConflict, "Attach result belongs to a different active operation")
	}
	if active.attach != nil {
		defer attachexecution.ClearRemoval(active.attach.Removal)
		if output.err == nil {
			if (output.attach == nil) == (output.attachFailure == nil) || output.attach != nil && output.attach.InFlight ||
				output.environment != nil || output.environmentFailure != nil || output.script != nil || output.scriptFailure != nil ||
				output.scriptRecovery != nil || output.recovery != nil || output.result.Report != nil || output.result.Failure != nil {
				return errs.New(errs.KindStateConflict, "Attach removal result requires exactly one acknowledged completion or failure")
			}
			input, err := attachexecution.DecodeRemovalAssignment(active.attach, workers.identity)
			if err != nil {
				return err
			}
			defer attachexecution.ClearRemoval(input.Removal)
			if output.attach != nil && output.attach.Completed != input.CommandCount() {
				return errs.New(errs.KindStateConflict, "Attach removal result is missing captured commands")
			}
			if output.attachFailure != nil && (output.attachFailure.FinishedAt == nil || output.attachFailure.Outcome == workexecution.Succeeded || !workexecution.SameAssignment(*output.attachFailure, output.attempt)) {
				return errs.New(errs.KindStateConflict, "Attach removal failure lacks a saved terminal acknowledgement")
			}
			if err := workers.attachRemoval.Retire(workers.ctx, input); err != nil {
				return err
			}
		}
	}
	if output.err == nil && active.environment != nil {
		if (output.environment == nil) == (output.environmentFailure == nil) ||
			output.environment != nil && output.environment.Phase != hierarchyexecution.EnvironmentRemoved ||
			output.environmentFailure != nil && (output.environmentFailure.FinishedAt == nil || output.environmentFailure.Outcome == workexecution.Succeeded ||
				!workexecution.SameAssignment(*output.environmentFailure, output.attempt)) ||
			output.script != nil || output.scriptFailure != nil || output.scriptRecovery != nil || output.recovery != nil ||
			output.result.Report != nil || output.result.Failure != nil {
			return errs.New(errs.KindStateConflict, "Environment removal result requires its acknowledged absence proof")
		}
		input, err := hierarchyexecution.DecodeEnvironmentAssignment(active.environment, workers.identity)
		if err != nil {
			return err
		}
		if err := workers.hierarchy.RetireEnvironment(workers.ctx, input); err != nil {
			return err
		}
	}
	if output.err == nil && active.scriptRecovery != nil && ((output.scriptRecovery == nil) == (output.scriptFailure == nil) || output.script != nil || output.result.Report != nil || output.result.Failure != nil || output.recovery != nil) {
		return errs.New(errs.KindStateConflict, "Script recovery result requires its acknowledged cleanup proof")
	}
	if output.err == nil && active.script && ((output.script == nil) == (output.scriptFailure == nil) || output.scriptRecovery != nil || output.result.Report != nil || output.result.Failure != nil || output.recovery != nil) {
		return errs.New(errs.KindStateConflict, "Script result requires its acknowledged completion")
	}
	if output.err == nil && active.recovery != nil && ((output.recovery == nil) == (output.result.Failure == nil) || output.result.Report != nil) {
		return errs.New(errs.KindStateConflict, "recovery result requires acknowledged restoration")
	}
	if output.err == nil && active.forward != nil && ((output.result.Report == nil) == (output.result.Failure == nil) || output.recovery != nil) {
		return errs.New(errs.KindStateConflict, "Release result requires exactly one completion or recorded failure")
	}
	if output.err == nil && (active.script || active.scriptRecovery != nil) {
		if err := workers.scripts.Retire(workers.ctx, output.attempt); err != nil {
			return err
		}
	}
	if output.err == nil && active.forward != nil {
		if err := workers.executor.Retire(workers.ctx, output.attempt.ID); err != nil {
			return err
		}
	}
	if output.err == nil && active.recovery != nil {
		if err := workers.recovery.Retire(workers.ctx, output.attempt.ID); err != nil {
			return err
		}
	}
	workers.retireLocalExecution(output.attempt.WorkItemID)
	return nil
}

func (workers *executionWorkers) capacity() int {
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed {
		return 0
	}
	return workers.limit - len(workers.active)
}

func (workers *executionWorkers) close() {
	workers.mu.Lock()
	workers.closed = true
	workers.cancel()
	workers.mu.Unlock()
	workers.wait.Wait()
	workers.mu.Lock()
	defer workers.mu.Unlock()
	for _, active := range workers.active {
		attachexecution.ClearRemoval(active.attach.GetRemoval())
		attachexecution.ClearProvision(active.provision.GetProvision())
		attachexecution.ClearRecoveryAssignment(active.attachRecovery)
	}
}
