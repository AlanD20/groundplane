package agent

import (
	"context"
	"log/slog"
	"sync"

	"github.com/sample-tenant/groundplane/internal/adapters"
	"github.com/sample-tenant/groundplane/internal/common/runner"
	"github.com/sample-tenant/groundplane/pkg/errs"
)

// Assignment is a Controller-issued task the worker pool executes.
type Assignment struct {
	TaskID string
	Steps  []adapters.Step
}

// WorkerPool executes up to N tasks concurrently — Concurrency is
// Controller-managed via MaxConcurrentTasks (from AgentConfig);
// TaskAbort cancels the owning worker's context, and cancellation
// propagates to the underlying Docker operation (container stop). Every
// step runs through Runner (docs/standards.md, section 5) — the
// worker pool never shells out itself. See mvp.md, "Agent channel
// (locked)".
type WorkerPool struct {
	size    int
	runner  runner.Runner
	logger  *slog.Logger
	work    chan Assignment
	mu      sync.Mutex
	cancels map[string]context.CancelFunc // taskID -> cancel, for TaskAbort
}

func NewWorkerPool(size int, r runner.Runner, logger *slog.Logger) *WorkerPool {
	return &WorkerPool{
		size:    size,
		runner:  r,
		logger:  logger,
		work:    make(chan Assignment, size),
		cancels: map[string]context.CancelFunc{},
	}
}

// Capacity reports free slots — sent as Ready{capacity} on every pull tick.
func (p *WorkerPool) Capacity() int {
	return p.size - len(p.cancels)
}

// Run starts size worker goroutines, each pulling from work until ctx is
// cancelled.
func (p *WorkerPool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.size; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.runWorker(ctx)
		}()
	}
	wg.Wait()
}

func (p *WorkerPool) runWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case a := <-p.work:
			p.execute(ctx, a)
		}
	}
}

func (p *WorkerPool) execute(parent context.Context, a Assignment) {
	taskCtx, cancel := context.WithCancel(parent)
	p.mu.Lock()
	p.cancels[a.TaskID] = cancel
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.cancels, a.TaskID)
		p.mu.Unlock()
		cancel()
	}()

	for _, step := range a.Steps {
		select {
		case <-taskCtx.Done():
			p.logger.Info("agent: task aborted", "task_id", a.TaskID)
			return
		default:
		}
		if err := p.runStep(taskCtx, step); err != nil {
			p.logger.Error("agent: step failed", "task_id", a.TaskID, "op", step.Op, "error", err)
			// TODO: send TaskAck{success:false} over the channel.
			return
		}
	}
	// TODO: send TaskAck{success:true} over the channel.
}

// runStep executes exactly ONE member of the known step catalog through
// Runner — this is the enforced boundary: the renderer's ExecutionPlan
// and adapters compose steps, the pipeline executes them via the ONE
// subprocess abstraction, nothing else runs arbitrary commands. See
// architecture.md, "The guardrail", for the full catalog this switch
// mirrors.
func (p *WorkerPool) runStep(ctx context.Context, step adapters.Step) error {
	switch step.Op {
	case adapters.StepComposeUp, adapters.StepComposeDown, adapters.StepWaitHealthy,
		adapters.StepSwitchAlias, adapters.StepSwitchRoute, adapters.StepWriteFile,
		adapters.StepReload, adapters.StepJoinNetwork, adapters.StepProvisionNetwork:
		// TODO: dispatch to internal/infra/docker (Applier.Up/Down, alias/
		// route switching against the rendered Compose project) or
		// internal/agent's own materializers (StepWriteFile), per step.Op.
		return errs.Newf(errs.CodeNotImplemented, "agent: runStep %s not implemented", step.Op)

	case adapters.StepRunScript:
		// The explicit, operator-authored automation exception — its own
		// task boundary, never a generic exec escape hatch
		// (architecture.md, "The guardrail"). TODO: run the script body
		// through p.runner against the target service's container.
		return errs.New(errs.CodeNotImplemented, "agent: runStep run_script not implemented")

	case adapters.StepExec, adapters.StepSQL, adapters.StepDump, adapters.StepRestore,
		adapters.StepEncrypt, adapters.StepUpload, adapters.StepVerify, adapters.StepPrune, adapters.StepAck:
		// TODO: translate step.Params into a runner.RunCmdOpts (e.g. `docker
		// exec <container> psql -c "<stmt>"` for StepSQL against a postgres
		// adapter) and call p.runner.Run(ctx, opts); for encrypt/decrypt use
		// internal/infra/age instead, and for upload/verify the connector's
		// client. p.runner is already wired (see client.go's Run) so this is
		// purely the per-Op translation, not new plumbing.
		return errs.Newf(errs.CodeNotImplemented, "agent: runStep %s not implemented", step.Op)

	default:
		return errs.Newf(errs.CodeValidationFailed, "agent: unknown step op %q — not in the known step catalog", step.Op)
	}
}

// Abort cancels a running task's context — the TaskAbort handler.
func (p *WorkerPool) Abort(taskID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cancel, ok := p.cancels[taskID]; ok {
		cancel()
	}
}

// Submit enqueues a new assignment (called from the gRPC receive loop
// on TaskAssignment).
func (p *WorkerPool) Submit(a Assignment) {
	p.work <- a
}
