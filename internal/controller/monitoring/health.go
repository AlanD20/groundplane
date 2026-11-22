package monitoring

import (
	"context"
	"log/slog"
	"sync"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	observationwire "github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/internal/core"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const healthInterval = 10 * time.Second

type HealthScope struct{ TenantID, ProjectID, EnvironmentID string }

type HealthCache interface{ State(HealthScope) string }

type healthEntry struct {
	owner             metrics.Ownership
	intent            core.ServiceRuntimeIntent
	state             api.ServiceObservationState
	observed, expires time.Time
}

// HealthSampler keeps parent health reads independent of the number of viewers.
// It uses the existing exact-serving observation path, not utilization as a
// substitute for readiness or desired state as proof of a running container.
type HealthSampler struct {
	mu          sync.RWMutex
	inventory   WorkloadInventory
	observer    HealthReader
	now         func() time.Time
	cursor      string
	complete    bool
	collectedAt time.Time
	entries     []healthEntry
}

func NewHealthSampler(
	inventory WorkloadInventory,
	observer HealthReader,
	now func() time.Time,
) (*HealthSampler, error) {
	if inventory == nil || observer == nil || now == nil {
		return nil, errs.New(errs.KindInternal, "monitoring health sources are required")
	}
	return &HealthSampler{inventory: inventory, observer: observer, now: now}, nil
}

func (sampler *HealthSampler) Collect(ctx context.Context) error {
	if ctx == nil || sampler == nil {
		return errs.New(errs.KindInternal, "monitoring health context is required")
	}
	bounded, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	page, err := sampler.inventory.Page(bounded, sampler.cursor, MaximumInventoryPage)
	if err != nil {
		if kind, _ := errs.KindOf(err); kind == errs.KindCursorExpired {
			sampler.cursor = ""
		}
		return err
	}
	byEnvironment := make(map[string][]InventoryTarget)
	for _, target := range page.Items {
		if target.Ownership.Class == metrics.OwnerWorkload {
			byEnvironment[target.Ownership.EnvironmentID] = append(
				byEnvironment[target.Ownership.EnvironmentID],
				target,
			)
		}
	}
	entries := make([]healthEntry, 0, len(page.Items))
	for _, group := range byEnvironment {
		for start := 0; start < len(group); start += observationwire.MaximumTargets {
			chunk := group[start:min(len(group), start+observationwire.MaximumTargets)]
			services := make([]etcdstore.Versioned[servicerecord.ServiceRecord], len(chunk))
			for index, target := range chunk {
				services[index] = target.Service
			}
			observations := sampler.observer.ObserveServices(bounded, services)
			if len(observations) != len(chunk) {
				return errs.New(errs.KindInternal, "monitoring health result count is invalid")
			}
			for index, target := range chunk {
				observation := observations[index]
				entry := healthEntry{
					owner:  target.Ownership,
					intent: target.Service.Record.Runtime.RuntimeIntent,
					state:  observation.State,
				}
				if observation.ObservedAt != nil {
					entry.observed = *observation.ObservedAt
				}
				if observation.ExpiresAt != nil {
					entry.expires = *observation.ExpiresAt
				}
				entries = append(entries, entry)
			}
		}
	}
	sampler.mu.Lock()
	sampler.complete = sampler.cursor == "" && page.Next == ""
	sampler.collectedAt = sampler.now()
	sampler.entries = entries
	sampler.mu.Unlock()
	sampler.cursor = page.Next
	return nil
}

func (sampler *HealthSampler) State(scope HealthScope) string {
	if sampler == nil {
		return "unavailable"
	}
	sampler.mu.RLock()
	defer sampler.mu.RUnlock()
	now := sampler.now()
	if !sampler.complete || sampler.collectedAt.IsZero() || now.Before(sampler.collectedAt) ||
		now.Sub(sampler.collectedAt) >= observationwire.Freshness {
		return "unavailable"
	}
	var selected []healthEntry
	for _, entry := range sampler.entries {
		if entry.owner.TenantID != scope.TenantID ||
			scope.ProjectID != "" && entry.owner.ProjectID != scope.ProjectID ||
			scope.EnvironmentID != "" && entry.owner.EnvironmentID != scope.EnvironmentID {
			continue
		}
		if entry.observed.IsZero() || entry.expires.IsZero() || now.Before(entry.observed) ||
			!now.Before(entry.expires) {
			entry.state = api.ServiceObservationUnavailable
		}
		selected = append(selected, entry)
	}
	return summarizeHealth(selected)
}

func summarizeHealth(entries []healthEntry) string {
	if len(entries) == 0 {
		return "absent"
	}
	failed, degraded, unknown, starting, running, healthy, stopped := false, false, false, false, false, false, false
	for _, entry := range entries {
		switch entry.state {
		case api.ServiceObservationFailed:
			failed = true
		case api.ServiceObservationDegraded:
			degraded = true
		case api.ServiceObservationStarting:
			starting = true
		case api.ServiceObservationHealthy:
			healthy = true
		case api.ServiceObservationRunning:
			running = true
		case api.ServiceObservationStopped:
			if entry.intent == core.ServiceRuntimeIntentRunning {
				degraded = true
			} else {
				stopped = true
			}
		case api.ServiceObservationAbsent:
			if entry.intent == core.ServiceRuntimeIntentRunning {
				degraded = true
			}
		default:
			unknown = true
		}
	}
	switch {
	case failed:
		return "failed"
	case degraded:
		return "degraded"
	case unknown:
		return "unavailable"
	case starting:
		return "starting"
	case running:
		return "running"
	case healthy:
		return "healthy"
	case stopped:
		return "stopped"
	default:
		return "absent"
	}
}

func (sampler *HealthSampler) Run(ctx context.Context, logger *slog.Logger) {
	ticker := time.NewTicker(healthInterval)
	defer ticker.Stop()
	for {
		if err := sampler.Collect(ctx); err != nil && ctx.Err() == nil {
			logger.DebugContext(ctx, "monitoring health unavailable", slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
