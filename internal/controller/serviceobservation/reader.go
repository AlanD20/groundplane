package serviceobservation

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"time"

	wire "github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Agents supplies the one enrolled host identity, never an arbitrary online
// connection. The channel separately fences its authenticated session.
type Agents interface {
	GetSingleton(context.Context) (etcdstore.Versioned[etcd.LocalAgentRecord], error)
}

type Reader struct {
	agents   Agents
	observer *Observer
}

func NewReader(agents Agents, observer *Observer) (*Reader, error) {
	if agents == nil || observer == nil {
		return nil, errs.New(errs.KindInternal, "Service observation reader dependencies are required")
	}
	return &Reader{agents: agents, observer: observer}, nil
}

// ObserveServices serves the existing human read surfaces without creating an
// operator action. Unavailable evidence never prevents the desired read.
func (reader *Reader) ObserveServices(
	ctx context.Context, services []etcdstore.Versioned[servicerecord.ServiceRecord],
) []api.ServiceObservation {
	result := unavailablePublic(len(services))
	if ctx == nil || reader == nil || !validBatch(services) {
		return result
	}
	ctx, cancel := context.WithTimeout(ctx, wire.Timeout)
	defer cancel()
	agent, err := reader.agents.GetSingleton(ctx)
	if err != nil || agent.Record.Phase != etcd.LocalAgentPhaseReady || agent.Record.Generation == 0 {
		return result
	}
	observations := reader.observer.Observe(ctx, agent.Record.ID, services)
	current, err := reader.agents.GetSingleton(ctx)
	if err != nil || ctx.Err() != nil || current.Revision != agent.Revision ||
		current.Record.ID != agent.Record.ID || current.Record.Generation != agent.Record.Generation ||
		current.Record.Phase != etcd.LocalAgentPhaseReady {
		return result
	}
	now := reader.observer.now()
	for index, observation := range observations {
		result[index] = publicObservation(observation, now)
	}
	return result
}

func publicObservation(observation Observation, now time.Time) api.ServiceObservation {
	snapshot := observation.Snapshot
	if observation.State == wire.Unavailable || snapshot == nil ||
		now.Before(snapshot.ObservedAt) || !now.Before(snapshot.ExpiresAt) {
		return api.ServiceObservation{State: api.ServiceObservationUnavailable}
	}
	counts := snapshot.Replicas
	return api.ServiceObservation{
		State:      api.ServiceObservationState(observation.State),
		ObservedAt: &snapshot.ObservedAt, ExpiresAt: &snapshot.ExpiresAt,
		ServingReleaseID: &snapshot.ServingReleaseID, ExpectedReplicas: &snapshot.ExpectedReplicas,
		Replicas: &api.ServiceReplicaCounts{
			Running: counts.Running, Healthy: counts.Healthy, Starting: counts.Starting,
			Unhealthy: counts.Unhealthy, Transitional: counts.Transitional, Stopped: counts.Stopped, Failed: counts.Failed,
		},
	}
}

func unavailablePublic(count int) []api.ServiceObservation {
	result := make([]api.ServiceObservation, count)
	for index := range result {
		result[index].State = api.ServiceObservationUnavailable
	}
	return result
}
