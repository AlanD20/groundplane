package workdispatch

import (
	"context"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type localAttempt struct {
	running bool
	retries uint8
}

type deliveryResult struct {
	delivery workqueue.Delivery
	err      error
}

// Run consumes durable Controller slots, not the scheduler's in-memory response.
// One invocation owns one process identity. Completed attempts leave the local
// map when their slots disappear. Unsettled effects retain their slot and claims;
// after three resume retries they need operator reconciliation. Every retry
// reads the durable start receipt: a started command is never replayed.
// report receives private diagnostics and must not expose raw errors publicly.
func (executor *ControllerExecutor) Run(ctx context.Context, identity workexecution.Identity,
	interval time.Duration, report func(Issue),
) error {
	if ctx == nil || identity.Validate() != nil || identity.Executor != workexecution.Controller || interval < 100*time.Millisecond || report == nil {
		return errs.New(errs.KindInternal, "controller execution loop configuration is invalid")
	}
	workerContext, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() {
		cancel()
		workers.Wait()
	}()
	// The queue allows at most 256 occupied slots. One result per worker fits
	// even during shutdown, when the receiving loop has stopped.
	results := make(chan deliveryResult, 256)
	active := make(map[string]localAttempt)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	sweep := func() error {
		deliveries, err := executor.queue.Deliveries(workerContext, identity)
		if err != nil {
			return err
		}
		present := make(map[string]struct{}, len(deliveries))
		for _, delivery := range deliveries {
			id := delivery.Attempt.Attempt.ID
			present[id] = struct{}{}
			local, seen := active[id]
			if local.running || seen && local.retries >= 3 {
				continue
			}
			local.running = true
			if seen {
				local.retries++
			}
			active[id] = local
			workers.Add(1)
			go func(delivery workqueue.Delivery) {
				defer workers.Done()
				err := executor.execute(workerContext, delivery)
				select {
				case results <- deliveryResult{delivery: delivery, err: err}:
				case <-workerContext.Done():
				}
			}(delivery)
		}
		for id, local := range active {
			if _, exists := present[id]; !exists && !local.running {
				delete(active, id)
			}
		}
		return nil
	}
	if err := sweep(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-results:
			id := result.delivery.Attempt.Attempt.ID
			local := active[id]
			local.running = false
			active[id] = local
			if result.err != nil {
				report(Issue{TaskID: result.delivery.Item.Item.TaskID, WorkID: result.delivery.Item.Item.ID, Err: result.err})
			}
		case <-ticker.C:
			if err := sweep(); err != nil {
				return err
			}
		}
	}
}
