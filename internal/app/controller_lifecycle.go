package app

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Run supervises HTTP, the local Agent channel, and the scheduler as one
// Controller lifetime. It joins every runtime before closing the etcd Store
// that NewController created.
func (c *Controller) Run(ctx context.Context) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "controller run context is required")
	}
	if c.workExecution == nil {
		return errs.New(errs.KindInternal, "controller execution runtime is not configured")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup
	for _, worker := range []controllerScheduler{c.scheduler, c.localAgent, c.runners, c.resolverConfiguration, c.taskAlerts, c.monitoring} {
		if worker == nil {
			continue
		}
		workers.Add(1)
		go func(worker controllerScheduler) { defer workers.Done(); worker.Run(runCtx) }(worker)
	}

	runtimes := []struct {
		operation, stopped string
		run                func(context.Context) error
	}{
		{"run execution runtime", "execution runtime", c.workExecution.Run},
		{"run Git delivery", "Git delivery runtime", c.gitDelivery},
		{"serve HTTP", "http server", func(ctx context.Context) error {
			return c.server.Serve(ctx, c.Config.HTTPAddresses())
		}},
		{"serve Agent channel", "agent channel", c.agent.Run},
		{"reconcile etcd container", "etcd container reconciliation", c.etcdContainer.Run},
	}
	done := make(chan struct{}, len(runtimes))
	failures := make([]error, len(runtimes))
	started := 0
	for index, runtime := range runtimes {
		if runtime.run == nil {
			continue
		}
		started++
		go func() {
			err := runtime.run(runCtx)
			if err == nil && runCtx.Err() == nil {
				err = fmt.Errorf("%s stopped unexpectedly", runtime.stopped)
			}
			failures[index] = wrapControllerRunError(runtime.operation, err)
			done <- struct{}{}
		}()
	}
	<-done
	cancel()
	for range started - 1 {
		<-done
	}
	workers.Wait()

	joined := errors.Join(append(failures,
		wrapControllerRunError("close platform runtime", c.container.Close()),
		wrapControllerRunError("close etcd", c.store.Close()),
		wrapControllerRunError("close etcd container lifecycle", c.etcdContainer.Close()),
	)...)
	if joined != nil {
		return errs.Wrap(errs.KindInternal, joined)
	}
	return nil
}

func wrapControllerRunError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("controller: %s: %w", operation, err)
}
