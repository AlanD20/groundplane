package workdispatch

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (runtime *Runtime) waitForStorageRecovery(
	ctx context.Context,
	ownership executionOwnership,
	identity workexecution.Identity,
) ([]Issue, error) {
	delay := 250 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := ownership.Check(); err != nil {
			return nil, err
		}
		probe, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := runtime.health.Health(probe)
		probeExpired := errors.Is(probe.Err(), context.DeadlineExceeded)
		cancel()
		if err == nil {
			issues, recoveryErr := runtime.reconcileRetained(ctx, ownership, identity)
			if recoveryErr == nil || !recoverableStorageFailure(recoveryErr) {
				return issues, recoveryErr
			}
		} else if !recoverableStorageFailure(err) && !probeExpired {
			return nil, err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
		delay = min(5*time.Second, delay*2)
	}
}

// reconcileRetained observes only attempts already granted to the retained
// Controller identity. Unstarted work remains assigned and cannot obtain a
// Start until forward admission reopens.
func (runtime *Runtime) reconcileRetained(
	ctx context.Context,
	ownership executionOwnership,
	identity workexecution.Identity,
) ([]Issue, error) {
	if ctx == nil || identity.Validate() != nil || identity.Executor != workexecution.Controller {
		return nil, errs.New(errs.KindInternal, "storage recovery requires a Controller identity")
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	retained, err := runtime.reconcileCurrent(ctx, identity)
	if err != nil {
		return retained, err
	}
	if err := ownership.Check(); err != nil {
		return retained, err
	}
	return retained, nil
}

func (runtime *Runtime) reconcileCurrent(
	ctx context.Context,
	identity workexecution.Identity,
) ([]Issue, error) {
	deliveries, err := runtime.queue.Deliveries(ctx, identity)
	if err != nil {
		return nil, err
	}
	started := deliveries[:0]
	for _, delivery := range deliveries {
		if delivery.Attempt.Attempt.StartedAt == nil {
			continue
		}
		if runtime.controller.owners[delivery.Item.Item.Operation] == nil {
			return nil, errs.New(errs.KindStateConflict, "retained work has no registered operation owner")
		}
		started = append(started, delivery)
	}
	issues := make([]Issue, len(started))
	jobs := make(chan int)
	var workers sync.WaitGroup
	for range min(4, len(started)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				delivery := started[index]
				if err := runtime.controller.execute(ctx, delivery); err != nil {
					issues[index] = Issue{
						TaskID: delivery.Item.Item.TaskID,
						WorkID: delivery.Item.Item.ID,
						Err:    err,
					}
				}
			}
		}()
	}
	for index := range started {
		select {
		case jobs <- index:
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return nil, ctx.Err()
		}
	}
	close(jobs)
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	retained := issues[:0]
	var unavailable error
	for _, issue := range issues {
		if issue.Err == nil {
			continue
		}
		retained = append(retained, issue)
		if unavailable == nil && storageUnavailable(issue.Err) {
			unavailable = issue.Err
		}
	}
	if unavailable != nil {
		return retained, unavailable
	}
	current, err := runtime.queue.ReadExecutor(
		ctx,
		workexecution.Placement{Executor: identity.Executor, HostID: identity.HostID},
	)
	if err != nil {
		return retained, err
	}
	if current.Revision == 0 || current.Record.Identity != identity || !current.Record.Ready {
		return retained, errs.New(errs.KindStateConflict, "Controller executor changed during storage recovery")
	}
	return retained, nil
}
