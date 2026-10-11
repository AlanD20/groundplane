package composeruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// One reporter belongs to one sealed step. It never publishes raw Docker health
// output, command text, image references or configuration. State changes are
// coalesced into at most one observation every five seconds, plus final readiness
// or an execution diagnostic; a stable wait emits a heartbeat every 30 seconds.
type healthWaitReporter struct {
	report   func(*agentpb.HealthWaitProgress)
	start    time.Time
	deadline time.Time
	lastAt   time.Time
	last     *agentpb.HealthWaitProgress
}

func newHealthWaitReporter(ctx context.Context, report func(*agentpb.HealthWaitProgress)) *healthWaitReporter {
	deadline, _ := ctx.Deadline()
	return &healthWaitReporter{report: report, start: time.Now(), deadline: deadline}
}

func (r *healthWaitReporter) observe(artifact *agentpb.ComposeArtifact, project *agentpb.ObservedProject,
	names []string, convergence composeConvergence,
) {
	if r.report == nil {
		return
	}
	services, err := convergenceServices(artifact, names)
	if err != nil {
		return // Selection failure is reported by the executor's terminal result.
	}
	p := &agentpb.HealthWaitProgress{Diagnostic: convergence.Diagnostic}
	for _, service := range services {
		p.Expected += service.GetExpectedReplicas()
		for _, container := range containersForService(project.GetContainers(), service) {
			p.Observed++
			if container.GetState() == agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING {
				p.Running++
				if !service.GetHasHealthcheck() ||
					container.GetHealth() == agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY {
					p.Ready++
				}
			}
			switch container.GetHealth() {
			case agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_STARTING:
				p.Starting++
			case agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_UNHEALTHY:
				p.Unhealthy++
			}
			if container.GetState() == agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_EXITED ||
				container.GetState() == agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_DEAD {
				p.Exited++
			}
		}
	}
	now := time.Now()
	changed := r.last == nil || r.last.Expected != p.Expected || r.last.Observed != p.Observed ||
		r.last.Running != p.Running || r.last.Ready != p.Ready || r.last.Starting != p.Starting ||
		r.last.Unhealthy != p.Unhealthy || r.last.Exited != p.Exited || r.last.Diagnostic != p.Diagnostic
	final := convergence.Ready || p.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE &&
		p.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_UNSPECIFIED
	if r.last != nil && !final &&
		(now.Sub(r.lastAt) < 5*time.Second || !changed && now.Sub(r.lastAt) < 30*time.Second) {
		return
	}
	p.ElapsedMilliseconds = uint64(max(0, now.Sub(r.start).Milliseconds()))
	if !r.deadline.IsZero() {
		p.TimeoutMilliseconds = uint64(max(1, (r.deadline.Sub(r.start)+time.Millisecond-1)/time.Millisecond))
	}
	r.report(p)
	r.last, r.lastAt = p, now
}
