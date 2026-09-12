package serviceobservation

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type sourceFixture struct {
	service etcd.Versioned[etcd.ServiceRecord]
	serving etcd.ServingRelease
	render  etcd.Versioned[etcd.ReleaseRenderInput]
}

type sourceRead struct {
	ctx      context.Context
	identity string
	revision int64
}

type fakeReleases struct {
	fixtures       map[string]sourceFixture
	resolveCalls   []sourceRead
	renderCalls    []sourceRead
	resolveCounts  map[string]int
	renderCounts   map[string]int
	followRevision bool
	mutateServing  func(string, int, *etcd.ServingRelease)
	mutateRender   func(string, int, *etcd.Versioned[etcd.ReleaseRenderInput])
}

func (releases *fakeReleases) ResolveServing(
	ctx context.Context, environmentID, serviceID string, revision int64,
) (etcd.ServingRelease, error) {
	releases.resolveCalls = append(releases.resolveCalls, sourceRead{ctx: ctx, identity: serviceID, revision: revision})
	fixture, ok := releases.fixtures[serviceID]
	if !ok || fixture.service.Record.EnvironmentID != environmentID {
		return etcd.ServingRelease{}, errs.New(errs.KindReleaseNotFound, "Service has no serving Release")
	}
	if releases.resolveCounts == nil {
		releases.resolveCounts = make(map[string]int)
	}
	releases.resolveCounts[serviceID]++
	result := fixture.serving
	if releases.followRevision {
		result.Revision = revision
	}
	if releases.mutateServing != nil {
		releases.mutateServing(serviceID, releases.resolveCounts[serviceID], &result)
	}
	return result, nil
}

func (releases *fakeReleases) GetReleaseRenderInputAt(
	ctx context.Context, releaseID string, revision int64,
) (etcd.Versioned[etcd.ReleaseRenderInput], error) {
	releases.renderCalls = append(releases.renderCalls, sourceRead{ctx: ctx, identity: releaseID, revision: revision})
	for serviceID, fixture := range releases.fixtures {
		if fixture.serving.Intent.ID != releaseID {
			continue
		}
		if releases.renderCounts == nil {
			releases.renderCounts = make(map[string]int)
		}
		releases.renderCounts[serviceID]++
		result := fixture.render
		if releases.followRevision {
			result.ReadRevision = revision
		}
		if releases.mutateRender != nil {
			releases.mutateRender(serviceID, releases.renderCounts[serviceID], &result)
		}
		return result, nil
	}
	return etcd.Versioned[etcd.ReleaseRenderInput]{}, errs.New(
		errs.KindReleaseNotFound,
		"Release render input not found",
	)
}

func newSourceFixture(
	t *testing.T,
	seed int64,
	strategy domain.Strategy,
	slot domain.Slot,
	proxyPorts []uint16,
	expected uint32,
) sourceFixture {
	t.Helper()
	stamp := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, stamp, seed)
	serviceID := ids.NewAt(ids.KindService, stamp, seed+1)
	releaseID := ids.NewAt(ids.KindDeployment, stamp, seed+2)
	planID := ids.NewAt(ids.KindPlan, stamp, seed+3)
	artifactID := ids.NewAt(ids.KindConfig, stamp, seed+4)
	target, err := domain.TargetFor(strategy, slot)
	if err != nil {
		t.Fatal(err)
	}
	seal := domain.WorkloadSeal{
		RequestedReference: "registry.example/app:sealed",
		LocalImageID:       "sha256:" + strings.Repeat("a", 64),
		ReplicaCount:       expected,
	}
	if err := domain.ValidateWorkloadSeal(seal); err != nil {
		t.Fatal(err)
	}
	desired := core.Service{
		ID: serviceID, Name: "api", Image: "registry.example/app:desired", Replicas: int(expected) + 8,
	}
	record, err := etcd.NewServiceRecord(environmentID, desired, "")
	if err != nil {
		t.Fatal(err)
	}
	input := etcd.ReleaseRenderInput{
		ReleaseID: releaseID, PlanID: planID, ArtifactID: artifactID,
		ServiceID: serviceID, ServiceName: desired.Name, CandidateWorkload: seal,
		Strategy: strategy, Slot: slot, CandidateTarget: target, ProxyPorts: proxyPorts,
		EnvironmentID: environmentID,
		Projection:    etcd.EnvironmentComposeProjection{EnvironmentID: environmentID, RenderGeneration: 7},
	}
	digest, err := domain.Digest(input)
	if err != nil {
		t.Fatal(err)
	}
	return sourceFixture{
		service: etcd.Versioned[etcd.ServiceRecord]{Record: record, Revision: 29, ReadRevision: 40},
		serving: etcd.ServingRelease{
			Projection: domain.ServiceProjection{
				EnvironmentID: environmentID, ServiceID: serviceID, ServingReleaseID: releaseID,
				CurrentSuccessfulReleaseID: ids.NewAt(ids.KindDeployment, stamp, seed+5), ServingSlot: slot,
			},
			ProjectionRevision: 31,
			Intent: domain.Intent{
				ID: releaseID, EnvironmentID: environmentID, ServiceID: serviceID,
				CandidateWorkload: seal, Strategy: strategy, Slot: slot,
				RenderInputID: artifactID, RenderInputDigest: digest,
			},
			IntentRevision: 32, Revision: 40,
		},
		render: etcd.Versioned[etcd.ReleaseRenderInput]{Record: input, Revision: 33, ReadRevision: 40},
	}
}

// Rationale: observation follows the serving Release's sealed workload rather
// than current successful or mutable desired state, and each topology has an
// unambiguous Compose identity.
func TestCaptureSelectsServingSealedWorkloadAndComposeIdentity(t *testing.T) {
	tests := []struct {
		name       string
		strategy   domain.Strategy
		slot       domain.Slot
		proxyPorts []uint16
		expected   uint32
		compose    string
		role       string
	}{
		{name: "portless recreate", strategy: domain.StrategyRecreate, expected: 3, compose: "api", role: "singleton"},
		{
			name: "proxied recreate", strategy: domain.StrategyRecreate, proxyPorts: []uint16{8080},
			expected: 3, compose: "api--singleton", role: "singleton",
		},
		{
			name: "blue slot", strategy: domain.StrategyBlueGreen, slot: domain.SlotBlue,
			proxyPorts: []uint16{8080}, expected: 1, compose: "api--blue", role: "slot",
		},
		{
			name: "green slot", strategy: domain.StrategyBlueGreen, slot: domain.SlotGreen,
			proxyPorts: []uint16{8080}, expected: 1, compose: "api--green", role: "slot",
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSourceFixture(t, int64(index*10+1), test.strategy, test.slot, test.proxyPorts, test.expected)
			releases := &fakeReleases{fixtures: map[string]sourceFixture{
				fixture.service.Record.Desired.ID: fixture,
			}}
			captured, ok := capture(t.Context(), releases, fixture.service)
			if !ok {
				t.Fatal("valid serving source rejected")
			}
			if captured.target.ReleaseId != fixture.serving.Intent.ID ||
				captured.target.ReleaseId == fixture.serving.Projection.CurrentSuccessfulReleaseID ||
				captured.expected != test.expected || captured.expected == uint32(fixture.service.Record.Desired.Replicas) {
				t.Fatalf("captured mutable or successful source: %#v", captured)
			}
			if captured.target.ComposeName != test.compose || captured.target.RuntimeRole != test.role ||
				captured.target.Slot != string(test.slot) {
				t.Fatalf("target = %#v", captured.target)
			}
			if len(releases.resolveCalls) != 1 || len(releases.renderCalls) != 1 ||
				releases.resolveCalls[0].revision != fixture.service.ReadRevision ||
				releases.renderCalls[0].revision != fixture.service.ReadRevision {
				t.Fatalf("source reads escaped input revision: resolve=%v render=%v",
					releases.resolveCalls, releases.renderCalls)
			}
		})
	}
}

// Rationale: an otherwise cross-bound render record cannot replace immutable
// publication authority without also matching the intent's canonical digest.
func TestCaptureRejectsRenderInputDigestMismatch(t *testing.T) {
	fixture := newSourceFixture(t, 50, domain.StrategyRecreate, "", nil, 2)
	fixture.render.Record.TenantSlug = "tampered-after-publication"
	releases := &fakeReleases{fixtures: map[string]sourceFixture{
		fixture.service.Record.Desired.ID: fixture,
	}}
	if _, ok := capture(t.Context(), releases, fixture.service); ok {
		t.Fatal("render input with mismatched immutable digest accepted")
	}
}

// Rationale: individually well-shaped records are not authority unless their
// snapshot revision, identities, target, and workload seal bind across records.
func TestCaptureRejectsMalformedOrCrossBoundSources(t *testing.T) {
	tests := map[string]func(*sourceFixture){
		"serving read revision":      func(fixture *sourceFixture) { fixture.serving.Revision++ },
		"projection record revision": func(fixture *sourceFixture) { fixture.serving.ProjectionRevision = 0 },
		"intent record revision":     func(fixture *sourceFixture) { fixture.serving.IntentRevision = 0 },
		"render read revision":       func(fixture *sourceFixture) { fixture.render.ReadRevision++ },
		"render record revision":     func(fixture *sourceFixture) { fixture.render.Revision = 0 },
		"projection release": func(fixture *sourceFixture) {
			fixture.serving.Projection.ServingReleaseID = fixture.serving.Projection.CurrentSuccessfulReleaseID
		},
		"render service": func(fixture *sourceFixture) { fixture.render.Record.ServiceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV" },
		"workload target": func(fixture *sourceFixture) {
			fixture.render.Record.CandidateTarget = domain.WorkloadBlue
		},
		"workload seal": func(fixture *sourceFixture) { fixture.render.Record.CandidateWorkload.ReplicaCount = 0 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newSourceFixture(t, 60, domain.StrategyRecreate, "", nil, 2)
			mutate(&fixture)
			releases := &fakeReleases{fixtures: map[string]sourceFixture{
				fixture.service.Record.Desired.ID: fixture,
			}}
			if _, ok := capture(t.Context(), releases, fixture.service); ok {
				t.Fatal("malformed or unrelated serving source accepted")
			}
		})
	}
}

// Rationale: equal source values after an intervening write are ABA, so every
// private MVCC fence independently invalidates previously captured evidence.
func TestSourceUnchangedRejectsEveryRevisionFenceAfterABA(t *testing.T) {
	fixture := newSourceFixture(t, 70, domain.StrategyRecreate, "", nil, 2)
	releases := &fakeReleases{fixtures: map[string]sourceFixture{
		fixture.service.Record.Desired.ID: fixture,
	}}
	original, ok := capture(t.Context(), releases, fixture.service)
	if !ok || !original.unchanged(original) {
		t.Fatal("valid source was not stable")
	}
	for name, mutate := range map[string]func(*source){
		"runtime":    func(value *source) { value.runtimeRevision++ },
		"projection": func(value *source) { value.projectionRevision++ },
		"intent":     func(value *source) { value.intentRevision++ },
		"render":     func(value *source) { value.renderRevision++ },
	} {
		t.Run(name, func(t *testing.T) {
			current := original
			mutate(&current)
			if original.unchanged(current) {
				t.Fatal("equal value with changed MVCC fence accepted")
			}
		})
	}
}
