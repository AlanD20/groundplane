package backupstage

import (
	"context"
	"golang.org/x/sys/unix"
	"math"
)

// Prepare leases one deterministic namespace under exactly one capacity mode.
// Bounded callers supply their source-specific conservative disk bound;
// ExclusiveUnknown serializes filesystem growth and allocates before writing.
func (stager *Stager) Prepare(ctx context.Context, ids IDs, capacity CapacityMode) (*Stage, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := validateIDs(ids); err != nil {
		return nil, err
	}
	if capacity.kind != capacityBounded && capacity.kind != capacityExclusiveUnknown {
		return nil, validationError("capacity mode must be Bounded or ExclusiveUnknown")
	}
	if capacity.kind == capacityBounded &&
		(capacity.requiredBytes == 0 || capacity.requiredBytes > math.MaxInt64) {
		return nil, validationError("bounded required bytes must be between 1 and %d", int64(math.MaxInt64))
	}
	if stager == nil {
		return nil, internalError("stager is required")
	}
	stager.mu.Lock()
	stagerLocked := true
	defer func() {
		if stagerLocked {
			stager.mu.Unlock()
		}
	}()
	if stager.rootFD < 0 {
		return nil, internalError("stager is closed")
	}
	if _, exists := stager.leases[ids]; exists {
		return nil, stateConflictError("stage already has a live lease")
	}
	stager.reservationMu.Lock()
	if err := lockWithContext(ctx, stager.rootFD, stager.ops); err != nil {
		stager.reservationMu.Unlock()
		return nil, err
	}
	rootLocked := true
	releaseRoot := func(primary error) error {
		if !rootLocked {
			return primary
		}
		rootLocked = false
		result := unlockWithPrimary(ctx, primary, stager.rootFD, stager.ops)
		stager.reservationMu.Unlock()
		return result
	}
	taskFD, err := openOrCreateDirectory(ctx, stager.rootFD, ids.Task, stager.ops)
	if err != nil {
		return nil, releaseRoot(err)
	}
	stepFD, err := openOrCreateDirectory(ctx, taskFD, ids.Step, stager.ops)
	if err != nil {
		return nil, releaseRoot(closeWithPrimary(ctx, err, taskFD))
	}
	pointFD, err := openOrCreateDirectory(ctx, stepFD, ids.Point, stager.ops)
	if err != nil {
		return nil, releaseRoot(closeWithPrimary(ctx, err, stepFD, taskFD))
	}
	if err := stager.ops.flock(pointFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		primary := systemError("acquire stage kernel lease", err)
		if flockWouldBlock(err) {
			primary = stateConflictError("stage already has a live kernel lease")
		}
		return nil, releaseRoot(closeWithPrimary(ctx, primary, pointFD, stepFD, taskFD))
	}
	if stager.ops.beforeReserve != nil {
		stager.ops.beforeReserve()
	}
	if err := validatePointContents(ctx, pointFD, stager.ops); err != nil {
		return nil, releaseRoot(closeWithPrimary(ctx, err, pointFD, stepFD, taskFD))
	}
	if err := removePointContents(ctx, pointFD, stager.ops); err != nil {
		return nil, releaseRoot(closeWithPrimary(ctx, err, pointFD, stepFD, taskFD))
	}
	rootFD, err := unix.Dup(stager.rootFD)
	if err != nil {
		return nil, releaseRoot(closeWithPrimary(ctx, systemError("duplicate stage root descriptor", err),
			pointFD, stepFD, taskFD))
	}
	unix.CloseOnExec(rootFD)
	stage := &Stage{
		owner: stager, rootFD: rootFD, taskFD: taskFD, stepFD: stepFD, pointFD: pointFD,
		reservationRootFD: -1, reservationTaskFD: -1, reservationStepFD: -1, reservationFD: -1,
		growthFD: -1, ids: ids, capacity: capacity, artifacts: make(map[*Artifact]struct{}),
	}
	cleanupFailedStage := func(primary error) error {
		stager.leases[ids] = stage
		stager.mu.Unlock()
		stagerLocked = false
		return joinPrivate(primary, stage.Cleanup(ctx))
	}
	if err := stage.syncParents(ctx); err != nil {
		return nil, cleanupFailedStage(releaseRoot(err))
	}
	if err := stage.reserveCapacityLocked(ctx); err != nil {
		return nil, cleanupFailedStage(releaseRoot(err))
	}
	if err := releaseRoot(nil); err != nil {
		return nil, cleanupFailedStage(err)
	}
	stager.leases[ids] = stage
	return stage, nil
}

// Close performs the same mandatory destructive teardown as Cleanup.
func (stage *Stage) Close(ctx context.Context) error {
	return stage.Cleanup(ctx)
}
