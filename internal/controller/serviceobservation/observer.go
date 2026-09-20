package serviceobservation

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	wire "github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Channel owns request correlation and the authenticated connection fence.
type Channel interface {
	ObserveServices(
		context.Context,
		string,
		[]*agentpb.ServiceObservationTarget,
	) (*agentpb.ServiceObservationResult, error)
}

// Observation is unavailable unless it carries a complete, fresh Snapshot.
// The transport maps this value; mutation responses never request a snapshot.
type Observation struct {
	State    wire.State
	Snapshot *Snapshot
}

type Snapshot struct {
	ObservedAt, ExpiresAt time.Time
	ServingReleaseID      string
	ExpectedReplicas      uint32
	Replicas              *agentpb.ServiceReplicaCounts
}

type Observer struct {
	services Services
	releases Releases
	channel  Channel
	now      func() time.Time
}

func New(services Services, releases Releases, channel Channel, now func() time.Time) (*Observer, error) {
	if services == nil || releases == nil || channel == nil || now == nil {
		return nil, errs.New(errs.KindInternal, "Service observation dependencies are required")
	}
	return &Observer{services: services, releases: releases, channel: channel, now: now}, nil
}

// Observe preserves input order and always returns one outcome per Service.
// Missing authority or live evidence never turns a successful desired read into
// an error. All storage work and the single exchange share one five-second bound.
func (observer *Observer) Observe(
	ctx context.Context, agentID string, services []etcdstore.Versioned[etcd.ServiceRecord],
) []Observation {
	observations := unavailable(len(services))
	if ctx == nil || observer == nil || ids.Validate(ids.KindAgent, agentID) != nil || !validBatch(services) {
		return observations
	}
	ctx, cancel := context.WithTimeout(ctx, wire.Timeout)
	defer cancel()
	started := observer.now()
	sources := make([]source, len(services))
	targets := make([]*agentpb.ServiceObservationTarget, 0, len(services))
	ordinals := make([]int, 0, len(services))
	for index, service := range services {
		if ctx.Err() != nil {
			return observations
		}
		captured, ok := capture(ctx, observer.releases, service)
		if !ok {
			continue
		}
		sources[index] = captured
		targets, ordinals = append(targets, captured.target), append(ordinals, index)
	}
	if len(targets) == 0 || ctx.Err() != nil {
		return observations
	}
	result, err := observer.channel.ObserveServices(ctx, agentID, targets)
	if err != nil || result == nil || wire.ValidateResult(
		&agentpb.ObserveServices{RequestId: result.GetRequestId(), Targets: targets}, result,
	) != nil {
		return observations
	}
	// Recheck all usable rows at one new storage revision. An intent change and
	// change-back is still rejected by its MVCC revision, not just its value.
	currentServices, ok := observer.currentServices(ctx, services)
	if !ok {
		return observations
	}
	for ordinal, index := range ordinals {
		if result.Observations[ordinal].GetReplicas() == nil {
			continue
		}
		if ctx.Err() != nil {
			return unavailable(len(services))
		}
		original := services[index]
		current, exists := currentServices[original.Record.Desired.ID]
		if !exists || current.Record.EnvironmentID != original.Record.EnvironmentID ||
			current.Record.Desired.ID != original.Record.Desired.ID || current.Record.Runtime != original.Record.Runtime {
			continue
		}
		checked, ok := capture(ctx, observer.releases, current)
		if !ok || !sources[index].unchanged(checked) {
			continue
		}
		counts := result.Observations[ordinal].GetReplicas()
		state := wire.Summarize(counts, sources[index].expected)
		if sources[index].target.ProxyComposeName != "" {
			state = wire.SummarizeProxy(counts, sources[index].expected, result.Observations[ordinal].ProxyState)
		}
		observations[index] = Observation{
			State: state,
			Snapshot: &Snapshot{
				ObservedAt: started.UTC(), ExpiresAt: started.Add(wire.Freshness).UTC(),
				ServingReleaseID: sources[index].target.ReleaseId, ExpectedReplicas: sources[index].expected, Replicas: counts,
			},
		}
	}
	finished := observer.now()
	if ctx.Err() != nil || finished.Before(started) || !finished.Before(started.Add(wire.Freshness)) {
		return unavailable(len(services))
	}
	return observations
}

// currentServices reuses the ordinary Service read, so Component-generated
// Services remain excluded. It stops once all requested ids are found; cursor
// pages share one MVCC view and the enclosing observation deadline.
func (observer *Observer) currentServices(
	ctx context.Context, requested []etcdstore.Versioned[etcd.ServiceRecord],
) (map[string]etcdstore.Versioned[etcd.ServiceRecord], bool) {
	wanted := make(map[string]bool, len(requested))
	for _, service := range requested {
		wanted[service.Record.Desired.ID] = true
	}
	current := make(map[string]etcdstore.Versioned[etcd.ServiceRecord], len(requested))
	request := etcdstore.PageRequest{Limit: etcdstore.MaximumPageLimit}
	seenCursors := make(map[string]bool)
	for ctx.Err() == nil {
		page, err := observer.services.ListServices(ctx, requested[0].Record.EnvironmentID, request)
		if err != nil || page.Revision < requested[0].ReadRevision ||
			request.Revision != 0 && page.Revision != request.Revision || len(page.Items) > request.Limit {
			return nil, false
		}
		request.Revision = page.Revision
		for _, service := range page.Items {
			if service.ReadRevision != page.Revision ||
				service.Record.EnvironmentID != requested[0].Record.EnvironmentID {
				return nil, false
			}
			if wanted[service.Record.Desired.ID] {
				current[service.Record.Desired.ID] = service
			}
		}
		if len(current) == len(wanted) || page.NextCursor == "" {
			return current, true
		}
		if seenCursors[page.NextCursor] {
			return nil, false
		}
		request.Cursor = page.NextCursor
		seenCursors[page.NextCursor] = true
	}
	return nil, false
}

func validBatch(services []etcdstore.Versioned[etcd.ServiceRecord]) bool {
	if len(services) == 0 || len(services) > wire.MaximumTargets || services[0].ReadRevision <= 0 {
		return false
	}
	seen := make(map[string]bool, len(services))
	for _, service := range services {
		if service.ReadRevision != services[0].ReadRevision ||
			service.Record.EnvironmentID != services[0].Record.EnvironmentID ||
			ids.Validate(ids.KindEnvironment, service.Record.EnvironmentID) != nil ||
			ids.Validate(ids.KindService, service.Record.Desired.ID) != nil || seen[service.Record.Desired.ID] {
			return false
		}
		seen[service.Record.Desired.ID] = true
	}
	return true
}

func unavailable(count int) []Observation {
	result := make([]Observation, count)
	for index := range result {
		result[index].State = wire.Unavailable
	}
	return result
}
