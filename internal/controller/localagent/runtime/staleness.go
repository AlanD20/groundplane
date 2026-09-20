package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type staleAgentReader interface {
	GetSingleton(context.Context) (etcd.Versioned[etcd.LocalAgentRecord], error)
}

type staleAgentTaskTimeout interface {
	TimeoutAgentAssignments(context.Context, string, uint64, int32, time.Time) (int, error)
}

// staleAgentTaskMaintenance combines durable Agent configuration, ephemeral
// authenticated Ready evidence, and the durable Task timeout transaction. A
// missing in-memory heartbeat after Controller restart is not evidence of
// staleness; assignment deadlines remain the recovery authority until Ready is
// observed again.
type staleAgentTaskMaintenance struct {
	agents   staleAgentReader
	sessions *agentchannel.Registry
	tasks    staleAgentTaskTimeout
}

func NewStaleTaskMaintenance(
	agents staleAgentReader,
	sessions *agentchannel.Registry,
	tasks staleAgentTaskTimeout,
) (*staleAgentTaskMaintenance, error) {
	if agents == nil || sessions == nil || tasks == nil {
		return nil, errs.New(errs.KindInternal, "stale Agent task maintenance is not configured")
	}
	return &staleAgentTaskMaintenance{agents: agents, sessions: sessions, tasks: tasks}, nil
}

func (maintenance *staleAgentTaskMaintenance) ExpireStaleAgentTasks(
	ctx context.Context,
	now time.Time,
) (int, error) {
	if ctx == nil {
		return 0, errs.New(errs.KindInternal, "stale Agent maintenance context is required")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	stored, err := maintenance.agents.GetSingleton(ctx)
	if errors.Is(err, errs.New(errs.KindAgentNotFound, "")) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	record := stored.Record
	if record.Phase != etcd.LocalAgentPhaseReady {
		return 0, nil
	}
	if record.Config.PullIntervalSeconds <= 0 || record.Config.MaxConcurrentTasks <= 0 {
		return 0, errs.New(errs.KindInternal, "local Agent has invalid stale-task configuration")
	}
	snapshot, ok := maintenance.sessions.Snapshot(record.ID)
	if !ok || snapshot.Generation != record.Generation || snapshot.LastReady.IsZero() {
		return 0, nil
	}
	staleAfter := snapshot.LastReady.Add(localagent.StaleWindow(record.Config.PullIntervalSeconds))
	if !now.UTC().After(staleAfter) {
		return 0, nil
	}
	return maintenance.tasks.TimeoutAgentAssignments(
		ctx,
		record.ID,
		record.Generation,
		record.Config.MaxConcurrentTasks,
		now.UTC(),
	)
}
