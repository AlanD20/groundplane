package serviceobservation

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	wire "github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type serviceRead struct {
	ctx           context.Context
	environmentID string
	request       etcd.PageRequest
}

type fakeServices struct {
	pages  []etcd.Page[etcd.ServiceRecord]
	calls  []serviceRead
	failed bool
}

func (services *fakeServices) ListServices(
	ctx context.Context, environmentID string, request etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	services.calls = append(services.calls, serviceRead{
		ctx: ctx, environmentID: environmentID, request: request,
	})
	if services.failed {
		return etcd.Page[etcd.ServiceRecord]{}, errs.New(errs.KindRequestFailed, "service read failed")
	}
	index := len(services.calls) - 1
	if index >= len(services.pages) {
		return etcd.Page[etcd.ServiceRecord]{}, errs.New(errs.KindRequestFailed, "unexpected service page")
	}
	return services.pages[index], nil
}

type fakeChannel struct {
	calls    int
	contexts []context.Context
	agentIDs []string
	targets  [][]*agentpb.ServiceObservationTarget
	result   func([]*agentpb.ServiceObservationTarget) (*agentpb.ServiceObservationResult, error)
}

func (channel *fakeChannel) ObserveServices(
	ctx context.Context, agentID string, targets []*agentpb.ServiceObservationTarget,
) (*agentpb.ServiceObservationResult, error) {
	channel.calls++
	channel.contexts = append(channel.contexts, ctx)
	channel.agentIDs = append(channel.agentIDs, agentID)
	channel.targets = append(channel.targets, targets)
	return channel.result(targets)
}

func availableResult(
	targets []*agentpb.ServiceObservationTarget,
	counts map[string]*agentpb.ServiceReplicaCounts,
) *agentpb.ServiceObservationResult {
	rows := make([]*agentpb.ServiceObservationRow, len(targets))
	for index, target := range targets {
		if counts[target.ServiceId] == nil {
			rows[index] = &agentpb.ServiceObservationRow{
				ServiceId: target.ServiceId, ReleaseId: target.ReleaseId,
				Outcome: &agentpb.ServiceObservationRow_Unavailable{Unavailable: true},
			}
			continue
		}
		rows[index] = &agentpb.ServiceObservationRow{
			ServiceId: target.ServiceId, ReleaseId: target.ReleaseId,
			Outcome: &agentpb.ServiceObservationRow_Replicas{Replicas: counts[target.ServiceId]},
		}
		if target.ProxyComposeName != "" {
			rows[index].ProxyState = agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_MATCHING
		}
	}
	return &agentpb.ServiceObservationResult{RequestId: "01ARZ3NDEKTSV4RRFFQ69G5FAV", Observations: rows}
}

func fixtureMap(fixtures ...sourceFixture) map[string]sourceFixture {
	result := make(map[string]sourceFixture, len(fixtures))
	for _, fixture := range fixtures {
		result[fixture.service.Record.Desired.ID] = fixture
	}
	return result
}

func rebindEnvironment(t *testing.T, fixture sourceFixture, environmentID string) sourceFixture {
	t.Helper()
	fixture.service.Record.EnvironmentID = environmentID
	fixture.serving.Projection.EnvironmentID = environmentID
	fixture.serving.Intent.EnvironmentID = environmentID
	fixture.render.Record.EnvironmentID = environmentID
	fixture.render.Record.Projection.EnvironmentID = environmentID
	rebindSourceRuntimeEnvironment(t, &fixture, environmentID)
	digest, err := domain.Digest(fixture.render.Record)
	if err != nil {
		t.Fatal(err)
	}
	fixture.serving.Intent.RenderInputDigest = digest
	return fixture
}

func servicePage(revision int64, fixtures ...sourceFixture) etcd.Page[etcd.ServiceRecord] {
	result := etcd.Page[etcd.ServiceRecord]{Revision: revision}
	for _, fixture := range fixtures {
		service := fixture.service
		service.ReadRevision = revision
		result.Items = append(result.Items, service)
	}
	return result
}

func sequenceClock(values ...time.Time) func() time.Time {
	index := 0
	return func() time.Time {
		if index >= len(values) {
			return values[len(values)-1]
		}
		value := values[index]
		index++
		return value
	}
}

// Rationale: one desired read batch must produce at most one Agent exchange,
// keep unavailable members in place, and share one five-second context across
// capture, exchange, and a same-revision recheck without mutating inputs.
func TestObserveUsesOneBoundedBatchAndPreservesUnavailableDesiredReads(t *testing.T) {
	first := newSourceFixture(t, 100, domain.StrategyRecreate, "", nil, 2)
	missing := rebindEnvironment(
		t,
		newSourceFixture(t, 110, domain.StrategyRecreate, "", nil, 1),
		first.service.Record.EnvironmentID,
	)
	last := rebindEnvironment(
		t,
		newSourceFixture(t, 120, domain.StrategyBlueGreen, domain.SlotGreen, []uint16{8080}, 1),
		first.service.Record.EnvironmentID,
	)
	input := []etcd.Versioned[etcd.ServiceRecord]{first.service, missing.service, last.service}
	before, err := domain.Digest(input)
	if err != nil {
		t.Fatal(err)
	}
	releases := &fakeReleases{fixtures: fixtureMap(first, last), followRevision: true}
	services := &fakeServices{pages: []etcd.Page[etcd.ServiceRecord]{servicePage(60, first, last)}}
	channel := &fakeChannel{result: func(targets []*agentpb.ServiceObservationTarget) (
		*agentpb.ServiceObservationResult, error,
	) {
		return availableResult(targets, map[string]*agentpb.ServiceReplicaCounts{
			first.service.Record.Desired.ID: {Healthy: 2},
			last.service.Record.Desired.ID:  {Healthy: 1},
		}), nil
	}}
	started := time.Date(2026, 9, 12, 14, 0, 0, 0, time.FixedZone("source", 2*60*60))
	observer, err := New(services, releases, channel, sequenceClock(started, started.Add(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	observations := observer.Observe(t.Context(), ids.NewAt(ids.KindAgent, started, 1), input)
	if len(observations) != len(input) || observations[0].State != wire.Healthy ||
		observations[1].State != wire.Unavailable || observations[1].Snapshot != nil ||
		observations[2].State != wire.Healthy {
		t.Fatalf("observations = %#v", observations)
	}
	if channel.calls != 1 || len(channel.targets[0]) != 2 ||
		channel.targets[0][0].ServiceId != first.service.Record.Desired.ID ||
		channel.targets[0][1].ServiceId != last.service.Record.Desired.ID {
		t.Fatalf("exchange calls=%d targets=%v", channel.calls, channel.targets)
	}
	after, err := domain.Digest(input)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatal("desired input records were mutated")
	}
	if observations[0].Snapshot.ObservedAt != started.UTC() ||
		observations[0].Snapshot.ExpiresAt != started.Add(wire.Freshness).UTC() ||
		observations[0].Snapshot.ExpectedReplicas != 2 {
		t.Fatalf("snapshot = %#v", observations[0].Snapshot)
	}
	shared := channel.contexts[0]
	deadline, ok := shared.Deadline()
	if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > wire.Timeout {
		t.Fatalf("exchange deadline = %v, %v", deadline, ok)
	}
	for _, call := range append(append([]sourceRead(nil), releases.resolveCalls...), releases.renderCalls...) {
		if call.ctx != shared {
			t.Fatal("Release read did not share bounded context")
		}
	}
	for _, call := range services.calls {
		if call.ctx != shared {
			t.Fatal("Service recheck did not share bounded context")
		}
	}
	if len(services.calls) != 1 || services.calls[0].request != (etcd.PageRequest{Limit: etcd.MaximumPageLimit}) {
		t.Fatalf("recheck page requests = %v", services.calls)
	}
	for _, call := range releases.resolveCalls[:3] {
		if call.revision != 40 {
			t.Fatalf("initial source read revision = %d", call.revision)
		}
	}
	for _, call := range releases.resolveCalls[3:] {
		if call.revision != 60 {
			t.Fatalf("recheck source read revision = %d", call.revision)
		}
	}
}

// Rationale: OBS-05; exact healthy workload counts must be degraded when the
// Agent confirms the acknowledged stable proxy serves different live config.
func TestObserveDegradesHealthyWorkloadForProxyConfigMismatch(t *testing.T) {
	fixture := newSourceFixture(t, 125, domain.StrategyBlueGreen, domain.SlotBlue, []uint16{8080}, 1)
	releases := &fakeReleases{fixtures: fixtureMap(fixture), followRevision: true}
	services := &fakeServices{pages: []etcd.Page[etcd.ServiceRecord]{servicePage(60, fixture)}}
	channel := &fakeChannel{result: func(targets []*agentpb.ServiceObservationTarget) (
		*agentpb.ServiceObservationResult, error,
	) {
		result := availableResult(targets, map[string]*agentpb.ServiceReplicaCounts{
			fixture.service.Record.Desired.ID: {Healthy: 1},
		})
		result.Observations[0].ProxyState =
			agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_CONFIG_MISMATCH
		return result, nil
	}}
	now := time.Date(2026, 9, 12, 14, 30, 0, 0, time.UTC)
	observer, err := New(services, releases, channel, sequenceClock(now, now.Add(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	observations := observer.Observe(
		t.Context(), ids.NewAt(ids.KindAgent, now, 1), []etcd.Versioned[etcd.ServiceRecord]{fixture.service},
	)
	if len(observations) != 1 || observations[0].State != wire.Degraded ||
		observations[0].Snapshot == nil || observations[0].Snapshot.Replicas.GetHealthy() != 1 {
		t.Fatalf("proxy mismatch observation = %#v", observations)
	}
}

// Rationale: OBS-05; byte-identical proxy authority rewritten after the Agent
// read is stale evidence and cannot be degraded or healthy current state.
func TestObserveRejectsAcknowledgedProxyRuntimeRevisionABA(t *testing.T) {
	fixture := newSourceFixture(t, 126, domain.StrategyRecreate, "", []uint16{8080}, 1)
	releases := &fakeReleases{fixtures: fixtureMap(fixture), followRevision: true}
	releases.mutateRuntime = func(_ string, call int, runtime *etcd.Versioned[serviceruntimerecord.Record]) {
		if call == 2 {
			runtime.Revision++
		}
	}
	services := &fakeServices{pages: []etcd.Page[etcd.ServiceRecord]{servicePage(60, fixture)}}
	channel := &fakeChannel{result: func(targets []*agentpb.ServiceObservationTarget) (
		*agentpb.ServiceObservationResult, error,
	) {
		return availableResult(targets, map[string]*agentpb.ServiceReplicaCounts{
			fixture.service.Record.Desired.ID: {Healthy: 1},
		}), nil
	}}
	now := time.Date(2026, 9, 12, 14, 31, 0, 0, time.UTC)
	observer, err := New(services, releases, channel, sequenceClock(now, now.Add(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	observations := observer.Observe(
		t.Context(), ids.NewAt(ids.KindAgent, now, 2), []etcd.Versioned[etcd.ServiceRecord]{fixture.service},
	)
	if len(observations) != 1 || observations[0].State != wire.Unavailable || observations[0].Snapshot != nil {
		t.Fatalf("stale acknowledged proxy authority escaped: %#v", observations)
	}
}

// Rationale: input Services from different MVCC snapshots, duplicate Services,
// or an oversized request cannot be combined into live evidence or side effects.
func TestObserveRejectsInvalidBatchesBeforeReads(t *testing.T) {
	fixture := newSourceFixture(t, 130, domain.StrategyRecreate, "", nil, 1)
	stamp := time.Date(2026, 9, 12, 15, 0, 0, 0, time.UTC)
	mixed := []etcd.Versioned[etcd.ServiceRecord]{fixture.service, fixture.service}
	mixed[1].ReadRevision++
	duplicate := []etcd.Versioned[etcd.ServiceRecord]{fixture.service, fixture.service}
	oversized := make([]etcd.Versioned[etcd.ServiceRecord], wire.MaximumTargets+1)
	for index := range oversized {
		oversized[index] = fixture.service
	}
	for name, input := range map[string][]etcd.Versioned[etcd.ServiceRecord]{
		"mixed revisions": mixed,
		"duplicate":       duplicate,
		"oversized":       oversized,
	} {
		t.Run(name, func(t *testing.T) {
			releases := &fakeReleases{fixtures: fixtureMap(fixture), followRevision: true}
			services := &fakeServices{}
			channel := &fakeChannel{result: func([]*agentpb.ServiceObservationTarget) (
				*agentpb.ServiceObservationResult, error,
			) {
				t.Fatal("invalid batch reached channel")
				return nil, nil
			}}
			observer, err := New(services, releases, channel, func() time.Time { return stamp })
			if err != nil {
				t.Fatal(err)
			}
			observations := observer.Observe(t.Context(), ids.NewAt(ids.KindAgent, stamp, 2), input)
			if len(observations) != len(input) {
				t.Fatalf("result length = %d, want %d", len(observations), len(input))
			}
			for _, observation := range observations {
				if observation.State != wire.Unavailable || observation.Snapshot != nil {
					t.Fatalf("invalid batch observation = %#v", observation)
				}
			}
			if len(releases.resolveCalls) != 0 || len(services.calls) != 0 || channel.calls != 0 {
				t.Fatal("invalid batch performed reads or exchange")
			}
		})
	}
}

// Rationale: malformed evidence, source revision ABA, read failure, or expiry
// makes live data unavailable while retaining one outcome for the desired read.
func TestObserveReturnsUnavailableForUnusableOrExpiredEvidence(t *testing.T) {
	started := time.Date(2026, 9, 12, 16, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		configure  func(*fakeServices, *fakeReleases, *fakeChannel)
		finishedAt time.Time
	}{
		{
			name: "malformed result",
			configure: func(_ *fakeServices, _ *fakeReleases, channel *fakeChannel) {
				channel.result = func(targets []*agentpb.ServiceObservationTarget) (
					*agentpb.ServiceObservationResult, error,
				) {
					result := availableResult(targets, map[string]*agentpb.ServiceReplicaCounts{
						targets[0].ServiceId: {Healthy: 1},
					})
					result.Observations[0].ReleaseId = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
					return result, nil
				}
			},
			finishedAt: started.Add(time.Second),
		},
		{
			name: "runtime read failure",
			configure: func(services *fakeServices, _ *fakeReleases, _ *fakeChannel) {
				services.failed = true
			},
			finishedAt: started.Add(time.Second),
		},
		{
			name: "service missing from recheck",
			configure: func(services *fakeServices, _ *fakeReleases, _ *fakeChannel) {
				services.pages[0].Items = nil
			},
			finishedAt: started.Add(time.Second),
		},
		{
			name: "runtime intent changed",
			configure: func(services *fakeServices, _ *fakeReleases, _ *fakeChannel) {
				services.pages[0].Items[0].Record.Runtime.RuntimeIntent = core.ServiceRuntimeIntentStopped
			},
			finishedAt: started.Add(time.Second),
		},
		{
			name: "serving revision ABA",
			configure: func(_ *fakeServices, releases *fakeReleases, _ *fakeChannel) {
				releases.mutateServing = func(_ string, call int, serving *etcd.ServingRelease) {
					if call == 2 {
						serving.ProjectionRevision++
					}
				}
			},
			finishedAt: started.Add(time.Second),
		},
		{
			name: "intent revision ABA",
			configure: func(_ *fakeServices, releases *fakeReleases, _ *fakeChannel) {
				releases.mutateServing = func(_ string, call int, serving *etcd.ServingRelease) {
					if call == 2 {
						serving.IntentRevision++
					}
				}
			},
			finishedAt: started.Add(time.Second),
		},
		{
			name: "render revision ABA",
			configure: func(_ *fakeServices, releases *fakeReleases, _ *fakeChannel) {
				releases.mutateRender = func(_ string, call int, render *etcd.Versioned[etcd.ReleaseRenderInput]) {
					if call == 2 {
						render.Revision++
					}
				}
			},
			finishedAt: started.Add(time.Second),
		},
		{
			name:       "expired",
			configure:  func(*fakeServices, *fakeReleases, *fakeChannel) {},
			finishedAt: started.Add(wire.Freshness),
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSourceFixture(t, int64(150+index*10), domain.StrategyRecreate, "", nil, 1)
			releases := &fakeReleases{fixtures: fixtureMap(fixture), followRevision: true}
			services := &fakeServices{pages: []etcd.Page[etcd.ServiceRecord]{servicePage(60, fixture)}}
			channel := &fakeChannel{result: func(targets []*agentpb.ServiceObservationTarget) (
				*agentpb.ServiceObservationResult, error,
			) {
				return availableResult(targets, map[string]*agentpb.ServiceReplicaCounts{
					fixture.service.Record.Desired.ID: {Healthy: 1},
				}), nil
			}}
			test.configure(services, releases, channel)
			observer, err := New(services, releases, channel, sequenceClock(started, test.finishedAt))
			if err != nil {
				t.Fatal(err)
			}
			observations := observer.Observe(
				t.Context(), ids.NewAt(ids.KindAgent, started, int64(index+10)),
				[]etcd.Versioned[etcd.ServiceRecord]{fixture.service},
			)
			if len(observations) != 1 || observations[0].State != wire.Unavailable ||
				observations[0].Snapshot != nil {
				t.Fatalf("unusable evidence escaped: %#v", observations)
			}
		})
	}
}

// Rationale: an unavailable Agent row is a complete desired-read outcome, not
// partial counts that can be mistaken for current workload evidence.
func TestObservePreservesAgentUnavailable(t *testing.T) {
	fixture := newSourceFixture(t, 210, domain.StrategyRecreate, "", nil, 1)
	releases := &fakeReleases{fixtures: fixtureMap(fixture), followRevision: true}
	services := &fakeServices{pages: []etcd.Page[etcd.ServiceRecord]{servicePage(60, fixture)}}
	channel := &fakeChannel{result: func(targets []*agentpb.ServiceObservationTarget) (
		*agentpb.ServiceObservationResult, error,
	) {
		return availableResult(targets, nil), nil
	}}
	started := time.Date(2026, 9, 12, 17, 0, 0, 0, time.UTC)
	observer, err := New(services, releases, channel, sequenceClock(started, started.Add(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	observations := observer.Observe(
		t.Context(), ids.NewAt(ids.KindAgent, started, 30),
		[]etcd.Versioned[etcd.ServiceRecord]{fixture.service},
	)
	if len(observations) != 1 || observations[0].State != wire.Unavailable || observations[0].Snapshot != nil {
		t.Fatalf("unavailable row = %#v", observations)
	}
}

// Rationale: the observer has no usable partial construction; every dependency
// is required before a desired-read path can request live evidence.
func TestNewRequiresEveryDependency(t *testing.T) {
	services := &fakeServices{}
	releases := &fakeReleases{}
	channel := &fakeChannel{result: func([]*agentpb.ServiceObservationTarget) (
		*agentpb.ServiceObservationResult, error,
	) {
		return nil, errs.New(errs.KindRequestFailed, "unavailable")
	}}
	for name, construct := range map[string]func() (*Observer, error){
		"services": func() (*Observer, error) { return New(nil, releases, channel, time.Now) },
		"releases": func() (*Observer, error) { return New(services, nil, channel, time.Now) },
		"channel":  func() (*Observer, error) { return New(services, releases, nil, time.Now) },
		"clock":    func() (*Observer, error) { return New(services, releases, channel, nil) },
	} {
		t.Run(name, func(t *testing.T) {
			if observer, err := construct(); err == nil || observer != nil {
				t.Fatalf("New() = %#v, %v", observer, err)
			}
		})
	}
}

// Rationale: a requested Service can be beyond the first current page. All
// pages must use one revision, and a corrupt/repeated cursor must not loop.
func TestCurrentServicesKeepsPaginationOnOneRevision(t *testing.T) {
	first := newSourceFixture(t, 300, domain.StrategyRecreate, "", nil, 1)
	last := rebindEnvironment(
		t,
		newSourceFixture(t, 310, domain.StrategyRecreate, "", nil, 1),
		first.service.Record.EnvironmentID,
	)
	for _, test := range []struct {
		name   string
		change func(*etcd.Page[etcd.ServiceRecord])
		valid  bool
	}{
		{"same view", func(*etcd.Page[etcd.ServiceRecord]) {}, true},
		{"changed view", func(page *etcd.Page[etcd.ServiceRecord]) { page.Revision++ }, false},
		{"mixed row", func(page *etcd.Page[etcd.ServiceRecord]) { page.Items[0].ReadRevision++ }, false},
		{"foreign row", func(page *etcd.Page[etcd.ServiceRecord]) { page.Items[0].Record.EnvironmentID = "foreign" }, false},
		{"repeated cursor", func(page *etcd.Page[etcd.ServiceRecord]) {
			page.Items, page.NextCursor = nil, "second"
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			page := servicePage(60, first)
			page.NextCursor = "second"
			next := servicePage(60, last)
			test.change(&next)
			reader := &fakeServices{pages: []etcd.Page[etcd.ServiceRecord]{page, next}}
			observer := &Observer{services: reader}
			got, ok := observer.currentServices(t.Context(), []etcd.Versioned[etcd.ServiceRecord]{last.service})
			if ok != test.valid || test.valid && got[last.service.Record.Desired.ID].ReadRevision != 60 {
				t.Fatalf("current Services = %+v, %v", got, ok)
			}
			if len(reader.calls) != 2 || reader.calls[1].request != (etcd.PageRequest{
				Limit: etcd.MaximumPageLimit, Cursor: "second", Revision: 60,
			}) {
				t.Fatalf("pagination calls = %+v", reader.calls)
			}
		})
	}
}

// Rationale: cancellation must discard even a successful Agent response, and a
// cancelled request must not start fresh storage work or claim live evidence.
func TestObserveDiscardsEvidenceAfterCancellation(t *testing.T) {
	for _, before := range []bool{true, false} {
		fixture := newSourceFixture(t, 320, domain.StrategyRecreate, "", nil, 1)
		releases := &fakeReleases{fixtures: fixtureMap(fixture), followRevision: true}
		services := &fakeServices{pages: []etcd.Page[etcd.ServiceRecord]{servicePage(60, fixture)}}
		ctx, cancel := context.WithCancel(t.Context())
		channel := &fakeChannel{
			result: func(targets []*agentpb.ServiceObservationTarget) (*agentpb.ServiceObservationResult, error) {
				cancel()
				return availableResult(targets, map[string]*agentpb.ServiceReplicaCounts{
					fixture.service.Record.Desired.ID: {Healthy: 1},
				}), nil
			},
		}
		observer, err := New(services, releases, channel, time.Now)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if before {
			cancel()
		}
		got := observer.Observe(
			ctx,
			ids.NewAt(ids.KindAgent, time.Now(), 40),
			[]etcd.Versioned[etcd.ServiceRecord]{fixture.service},
		)
		cancel()
		if len(got) != 1 || got[0].State != wire.Unavailable || got[0].Snapshot != nil || len(services.calls) != 0 {
			t.Fatalf("cancelled evidence escaped or triggered a recheck: %+v, %+v", got, services.calls)
		}
		if before && (len(releases.resolveCalls) != 0 || channel.calls != 0) {
			t.Fatal("cancelled request started work")
		}
	}
}
