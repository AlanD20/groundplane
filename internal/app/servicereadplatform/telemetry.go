package servicereadplatform

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/monitoring"
	"github.com/AlanD20/groundplane/internal/controller/serviceobservation"
	"github.com/AlanD20/groundplane/internal/infra/hoststats"
	"github.com/AlanD20/groundplane/internal/infra/monitoringstore"
)

const DefaultHistoryPath = "/var/lib/groundplane/monitoring/metrics.sqlite"

type telemetryRuntime struct {
	host      *monitoring.HostSampler
	workloads *monitoring.WorkloadCollector
	health    *monitoring.HealthSampler
	history   *monitoringstore.Store
	logger    *slog.Logger
	closeOnce sync.Once
	closeErr  error
}

func newTelemetry(
	ctx context.Context,
	dependencies Dependencies,
	observations *serviceobservation.Reader,
) (*telemetryRuntime, *monitoring.Reader, error) {
	path := dependencies.HistoryPath
	if path == "" {
		path = DefaultHistoryPath
	}
	open, cancel := context.WithTimeout(ctx, 2*time.Second)
	history, err := monitoringstore.Open(open, monitoringstore.Options{Path: path})
	cancel()
	if err != nil {
		dependencies.Logger.WarnContext(
			ctx,
			"monitoring history unavailable; live monitoring remains enabled",
			slog.Any("error", err),
		)
	}
	runtime := &telemetryRuntime{history: history, logger: dependencies.Logger}
	var historyReads monitoring.HistoryReader
	var historyWrites monitoring.WorkloadHistory
	if history != nil {
		historyReads, historyWrites = history, history
	}
	host, err := monitoring.NewHostSampler(hoststats.New(), monitoring.LocalHost)
	if err != nil {
		_ = runtime.Close()
		return nil, nil, err
	}
	inventory, err := monitoring.NewInventory(dependencies.Hierarchy, dependencies.Services)
	if err != nil {
		_ = runtime.Close()
		return nil, nil, err
	}
	sources, err := monitoring.NewWorkloadSourceReader(dependencies.Services, dependencies.Releases)
	if err != nil {
		_ = runtime.Close()
		return nil, nil, err
	}
	health, err := monitoring.NewHealthSampler(inventory, observations, time.Now)
	if err != nil {
		_ = runtime.Close()
		return nil, nil, err
	}
	workloads, err := monitoring.NewWorkloadCollector(inventory, sources, dependencies.Agents,
		dependencies.Channel, historyWrites, monitoring.DefaultWorkloadConfig(), time.Now)
	if err != nil {
		_ = runtime.Close()
		return nil, nil, err
	}
	reader, err := monitoring.NewReader(dependencies.Hierarchy, dependencies.Services, dependencies.Backing,
		dependencies.Consumers, observations, health, host, workloads, historyReads, time.Now)
	if err != nil {
		_ = runtime.Close()
		return nil, nil, err
	}
	runtime.host, runtime.workloads, runtime.health = host, workloads, health
	return runtime, reader, nil
}

// Run is an optional Controller worker. Its reads/writes are bounded and do not
// join the critical HTTP/execution/etcd supervision group. Close follows worker
// cancellation and join, so no query or writer survives into Store teardown.
func (runtime *telemetryRuntime) Run(ctx context.Context) {
	var workers sync.WaitGroup
	var hostHistory monitoring.HostHistory
	if runtime.history != nil {
		hostHistory = runtime.history
	}
	workers.Add(3)
	go func() { defer workers.Done(); _ = runtime.host.Run(ctx, hostHistory, runtime.logger) }()
	go func() { defer workers.Done(); _ = runtime.workloads.Run(ctx, runtime.logger) }()
	go func() { defer workers.Done(); runtime.health.Run(ctx, runtime.logger) }()
	if runtime.history != nil {
		workers.Add(1)
		go func() { defer workers.Done(); runtime.maintain(ctx) }()
	}
	workers.Wait()
	if err := runtime.Close(); err != nil {
		runtime.logger.WarnContext(ctx, "monitoring history close failed", slog.Any("error", err))
	}
}

func (runtime *telemetryRuntime) maintain(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		read, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := runtime.history.Maintain(read, time.Now())
		cancel()
		if err != nil && ctx.Err() == nil {
			runtime.logger.DebugContext(ctx, "monitoring history maintenance unavailable", slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (runtime *telemetryRuntime) Close() error {
	runtime.closeOnce.Do(func() {
		if runtime.history != nil {
			runtime.closeErr = runtime.history.Close()
		}
	})
	return runtime.closeErr
}
