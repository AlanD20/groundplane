package controllerupgrade

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"log/slog"
	"math"
	"sync"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type JournalStore interface {
	Current(context.Context) (upgrade.Journal, bool, error)
	Inspect(context.Context, upgrade.Digest) (upgrade.Manifest, error)
	Installed(context.Context) (upgrade.Digest, error)
	Prepare(context.Context, upgrade.Journal) error
	Advance(context.Context, string, upgrade.Phase, upgrade.Phase) (upgrade.Journal, error)
	BeginRollback(context.Context, string, upgrade.Phase, *taskprogress.Failure) (upgrade.Journal, error)
}

type ActivationUnit interface {
	VerifyBootstrap(context.Context) error
	Launch(context.Context, string) error
}

type ManagedAgent interface {
	ListHealth(context.Context) ([]localagent.Health, error)
	Health(context.Context, string) (localagent.Health, error)
	RecoverUpdate(
		context.Context,
		localagent.UpdateRequest,
		localagent.UpdateGoal,
		*taskprogress.Failure,
	) (localagent.UpdateResult, error)
}

type ActiveAgentWork interface {
	RequireIdle(context.Context, string, uint64, int32) error
}

type Admission interface {
	PauseAssignments(context.Context, string, uint64) (func(), error)
}

type Dependencies struct {
	Journal       JournalStore
	Unit          ActivationUnit
	Agents        ManagedAgent
	Work          ActiveAgentWork
	Sessions      Admission
	Readiness     *Readiness
	ProcessDigest upgrade.Digest
	Logger        *slog.Logger
}

// Coordinator owns the native Task across process generations. The journal is
// the handoff/qualification authority; only the predecessor watchdog touches
// the installed executable or stops/starts the Controller service.
type Coordinator struct {
	journal   JournalStore
	unit      ActivationUnit
	agents    ManagedAgent
	work      ActiveAgentWork
	sessions  Admission
	readiness *Readiness
	process   upgrade.Digest
	logger    *slog.Logger
	now       func() time.Time
	mu        sync.Mutex
	active    *nativeOperation
}

type nativeOperation struct {
	expected upgrade.Journal
	resume   func()
	cancel   context.CancelFunc
	aborted  bool
	settled  workexecution.Outcome
	failure  *taskprogress.Failure
}

func NewCoordinator(dependencies Dependencies) (*Coordinator, error) {
	if dependencies.Journal == nil || dependencies.Unit == nil || dependencies.Agents == nil ||
		dependencies.Work == nil || dependencies.Sessions == nil || dependencies.Readiness == nil ||
		!dependencies.ProcessDigest.Valid() || dependencies.Logger == nil {
		return nil, errs.New(errs.KindInternal, "native controller update dependencies are invalid")
	}
	return &Coordinator{
		journal: dependencies.Journal, unit: dependencies.Unit, agents: dependencies.Agents,
		work: dependencies.Work, sessions: dependencies.Sessions, readiness: dependencies.Readiness,
		process: dependencies.ProcessDigest, logger: dependencies.Logger, now: time.Now,
	}, nil
}

// Restore runs before any Agent listener opens. It never waits for Ready or
// launches a process; storage and the immutable Task restore admission first.
func (coordinator *Coordinator) Restore(ctx context.Context, expected upgrade.Journal) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, upgrade.DrainTimeoutSeconds*time.Second)
	defer cancel()
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if previous := coordinator.active; previous != nil {
		if previous.expected.TaskID == expected.TaskID {
			if !previous.expected.SameOperation(expected) {
				return errs.New(errs.KindStateConflict, "native controller update Task changed")
			}
			return nil
		}
		if previous.settled == "" {
			return errs.New(errs.KindResourceInUse, "another native controller update still owns recovery")
		}
	}
	operation := &nativeOperation{expected: expected}
	if _, _, err := coordinator.current(ctx, operation); err != nil {
		return err
	}
	if expected.Agent != nil {
		resume, err := coordinator.sessions.PauseAssignments(ctx, expected.Agent.ID, math.MaxUint64)
		if err != nil {
			return err
		}
		operation.resume = resume
	}
	coordinator.active = operation
	return nil
}

func (coordinator *Coordinator) execute(
	ctx context.Context, expected upgrade.Journal, forward bool,
	progress Progress,
) (workexecution.Outcome, error) {
	coordinator.mu.Lock()
	operation := coordinator.active
	if operation == nil || !operation.expected.SameOperation(expected) {
		coordinator.mu.Unlock()
		return "", errs.New(errs.KindStateConflict, "native controller update admission is not restored")
	}
	settled := operation.settled
	coordinator.mu.Unlock()
	if settled != "" {
		return settled, nil
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	journal, found, err := coordinator.current(ctx, operation)
	if err != nil {
		return "", err
	}
	if !forward && !found || found && journal.Phase == upgrade.PhasePrepared {
		// A saved native start is not permission to prepare or activate again.
		// Prepared means only the inactive bundle/journal was staged; no cutover
		// was committed, so cancel that journal and preserve the predecessor.
		if found {
			if _, err := coordinator.journal.Advance(ctx, expected.TaskID, upgrade.PhasePrepared, upgrade.PhaseCancelled); err != nil {
				return "", err
			}
		}
		coordinator.mu.Lock()
		operation.failure = taskprogress.CaptureControllerFailure(
			errs.New(errs.KindRequestFailed, "Controller activation was interrupted before cutover"),
		)
		coordinator.mu.Unlock()
		return coordinator.settle(operation, workexecution.Failed), nil
	}
	if !found {
		return coordinator.prepare(ctx, operation, progress)
	}
	switch journal.Phase {
	case upgrade.PhaseActivating, upgrade.PhaseTrial, upgrade.PhaseRollingBack, upgrade.PhaseStopping:
		return coordinator.handoff(ctx, operation, progress)
	case upgrade.PhaseStarting:
		return coordinator.qualify(ctx, operation, progress)
	case upgrade.PhaseHealthy:
		return coordinator.confirmHealthy(ctx, operation, progress)
	case upgrade.PhaseRolledBack, upgrade.PhaseRecovered:
		return coordinator.recoverPredecessor(ctx, operation, journal.Phase, progress)
	case upgrade.PhaseCancelled:
		return coordinator.settle(operation, workexecution.Cancelled), nil
	default:
		return "", errs.New(errs.KindInternal, "native controller update phase is invalid")
	}
}

func (coordinator *Coordinator) current(
	ctx context.Context, operation *nativeOperation,
) (upgrade.Journal, bool, error) {
	journal, found, err := coordinator.journal.Current(ctx)
	if err != nil || !found {
		return upgrade.Journal{}, false, err
	}
	if err := journal.Validate(); err != nil {
		return upgrade.Journal{}, false, err
	}
	if journal.TaskID != operation.expected.TaskID {
		if journal.Phase.Settled() {
			return upgrade.Journal{}, false, nil
		}
		return upgrade.Journal{}, false, errs.New(errs.KindStateConflict, "another native journal owns recovery")
	}
	if !journal.SameOperation(operation.expected) {
		return upgrade.Journal{}, false, errs.New(errs.KindStateConflict, "native journal differs from its Task")
	}
	return journal, true, nil
}

func (coordinator *Coordinator) settle(
	operation *nativeOperation,
	status workexecution.Outcome,
) workexecution.Outcome {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if operation.aborted && status != workexecution.Succeeded {
		status = workexecution.Cancelled
	}
	operation.settled = status
	if operation.resume != nil {
		operation.resume()
	}
	return status
}

func (coordinator *Coordinator) handoff(
	ctx context.Context,
	operation *nativeOperation,
	progress Progress,
) (workexecution.Outcome, error) {
	// The transient predecessor service must stop this process. Never ACK from
	// the outgoing process, and never wait unboundedly if the manager is down.
	duration := max(operation.expected.Deadline.Sub(coordinator.now()), upgrade.RecoveryReserveSeconds*time.Second)
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	if err := progress.Run(ctx, "controller_handoff", func(ctx context.Context) error {
		return coordinator.unit.Launch(ctx, operation.expected.TaskID)
	}); err != nil {
		return "", err
	}
	if err := progress.Start("controller_wait_restart"); err != nil {
		return "", err
	}
	<-ctx.Done()
	return "", ctx.Err()
}

func waitPass(ctx context.Context) error {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
