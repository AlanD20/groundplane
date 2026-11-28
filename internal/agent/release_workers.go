package agent

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (workers *executionWorkers) execute(
	attempt workexecution.Attempt,
	delivery *agentpb.ReleaseWorkAssignment,
	execution *agentownership.Execution,
) {
	defer workers.wait.Done()
	result, err := workers.executor.Execute(
		workers.executionContext(attempt),
		delivery,
		workers.identity,
		workers.exchange,
	)
	err = errors.Join(err, execution.Close())
	// Keep the local slot until the session consumes this result. Otherwise a
	// slow consumer can accumulate arbitrarily many undelivered completions.
	select {
	case workers.outputs <- executionOutput{attempt: attempt, result: result, err: err}:
	case <-workers.ctx.Done():
	}
}
