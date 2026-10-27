package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/AlanD20/groundplane/pkg/errs"
	"sync"
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
	for _, worker := range []controllerScheduler{c.scheduler, c.localAgent, c.runners, c.resolverConfiguration, c.taskAlerts} {
		if worker == nil {
			continue
		}
		workers.Add(1)
		go func(worker controllerScheduler) { defer workers.Done(); worker.Run(runCtx) }(worker)
	}

	type runtimeResult struct {
		name string
		err  error
	}
	runtimeDone := make(chan runtimeResult, 4)
	go func() {
		err := c.workExecution.Run(runCtx)
		if err == nil && runCtx.Err() == nil {
			err = errors.New("execution runtime stopped unexpectedly")
		}
		runtimeDone <- runtimeResult{name: "run execution runtime", err: err}
	}()
	go func() {
		err := c.server.Serve(runCtx, c.Config.Listen.HTTP)
		if err == nil && runCtx.Err() == nil {
			err = errors.New("http server stopped unexpectedly")
		}
		runtimeDone <- runtimeResult{name: "serve HTTP", err: err}
	}()
	go func() {
		err := c.agent.Run(runCtx)
		if err == nil && runCtx.Err() == nil {
			err = errors.New("agent channel stopped unexpectedly")
		}
		runtimeDone <- runtimeResult{name: "serve Agent channel", err: err}
	}()
	go func() {
		err := c.etcdContainer.Run(runCtx)
		if err == nil && runCtx.Err() == nil {
			err = errors.New("etcd container reconciliation stopped unexpectedly")
		}
		runtimeDone <- runtimeResult{name: "reconcile etcd container", err: err}
	}()

	runtimeErrors := make(map[string]error, 4)
	for index := 0; index < 4; index++ {
		result := <-runtimeDone
		runtimeErrors[result.name] = result.err
		if index == 0 {
			cancel()
		}
	}
	workers.Wait()

	containerCloseErr := c.container.Close()
	closeErr := c.store.Close()
	etcdContainerCloseErr := c.etcdContainer.Close()
	joined := errors.Join(
		wrapControllerRunError("run execution runtime", runtimeErrors["run execution runtime"]),
		wrapControllerRunError("serve HTTP", runtimeErrors["serve HTTP"]),
		wrapControllerRunError("serve Agent channel", runtimeErrors["serve Agent channel"]),
		wrapControllerRunError("reconcile etcd container", runtimeErrors["reconcile etcd container"]),
		wrapControllerRunError("close platform runtime", containerCloseErr),
		wrapControllerRunError("close etcd", closeErr),
		wrapControllerRunError("close etcd container lifecycle", etcdContainerCloseErr),
	)
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
