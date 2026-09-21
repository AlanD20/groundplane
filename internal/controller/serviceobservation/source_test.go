package serviceobservation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type sourceFixture struct {
	service testkeyvalue.Versioned[testservices.ServiceRecord]
	serving testreleasequeries.ServingRelease
	render  testkeyvalue.Versioned[testreleaserender.ReleaseRenderInput]
	runtime *testkeyvalue.Versioned[serviceruntimerecord.Record]
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
	runtimeCalls   []sourceRead
	resolveCounts  map[string]int
	renderCounts   map[string]int
	followRevision bool
	mutateServing  func(string, int, *testreleasequeries.ServingRelease)
	mutateRender   func(string, int, *testkeyvalue.Versioned[testreleaserender.ReleaseRenderInput])
	mutateRuntime  func(string, int, *testkeyvalue.Versioned[serviceruntimerecord.Record])
	runtimeCounts  map[string]int
}

func (releases *fakeReleases) LoadAcknowledgedServiceRuntimesAtRevision(
	ctx context.Context,
	environmentID string,
	serviceIDs []string,
	revision int64,
) ([]testkeyvalue.Versioned[serviceruntimerecord.Record], error) {
	if len(serviceIDs) != 1 {
		return nil, errs.New(errs.KindValidationFailed, "unexpected runtime selection")
	}
	serviceID := serviceIDs[0]
	releases.runtimeCalls = append(releases.runtimeCalls, sourceRead{ctx: ctx, identity: serviceID, revision: revision})
	fixture, ok := releases.fixtures[serviceID]
	if !ok || fixture.runtime == nil || fixture.runtime.Record.EnvironmentID != environmentID {
		return nil, errs.New(errs.KindStateConflict, "acknowledged runtime is unavailable")
	}
	if releases.runtimeCounts == nil {
		releases.runtimeCounts = make(map[string]int)
	}
	releases.runtimeCounts[serviceID]++
	result := *fixture.runtime
	if releases.followRevision {
		result.ReadRevision = revision
	}
	if releases.mutateRuntime != nil {
		releases.mutateRuntime(serviceID, releases.runtimeCounts[serviceID], &result)
	}
	return []testkeyvalue.Versioned[serviceruntimerecord.Record]{result}, nil
}

func (releases *fakeReleases) ResolveServing(
	ctx context.Context, environmentID, serviceID string, revision int64,
) (testreleasequeries.ServingRelease, error) {
	releases.resolveCalls = append(releases.resolveCalls, sourceRead{ctx: ctx, identity: serviceID, revision: revision})
	fixture, ok := releases.fixtures[serviceID]
	if !ok || fixture.service.Record.EnvironmentID != environmentID {
		return testreleasequeries.ServingRelease{}, errs.New(errs.KindReleaseNotFound, "Service has no serving Release")
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
) (testkeyvalue.Versioned[testreleaserender.ReleaseRenderInput], error) {
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
	return testkeyvalue.Versioned[testreleaserender.ReleaseRenderInput]{}, errs.New(
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
	record, err := testservices.NewServiceRecord(environmentID, desired, "")
	if err != nil {
		t.Fatal(err)
	}
	input := testreleaserender.ReleaseRenderInput{
		ReleaseID: releaseID, PlanID: planID, ArtifactID: artifactID,
		ServiceID: serviceID, ServiceName: desired.Name, CandidateWorkload: seal,
		Strategy: strategy, Slot: slot, CandidateTarget: target, ProxyPorts: proxyPorts,
		EnvironmentID: environmentID,
		Projection: testenvironmentprojection.EnvironmentComposeProjection{
			EnvironmentID:    environmentID,
			RenderGeneration: 7,
		},
	}
	var runtime *testkeyvalue.Versioned[serviceruntimerecord.Record]
	if len(proxyPorts) != 0 {
		proxyConfig, proxyErr := domain.RenderProxyConfig(desired.Name, releaseID, target, 5, proxyPorts)
		if proxyErr != nil {
			t.Fatal(proxyErr)
		}
		input.ProxyGeneration, input.ProxyConfigDigest = 5, hex.EncodeToString(proxyConfig.SHA256[:])
		runtime = sourceRuntimeFixture(t, stamp, seed, input, proxyConfig)
	}
	digest, err := domain.Digest(input)
	if err != nil {
		t.Fatal(err)
	}
	return sourceFixture{
		service: testkeyvalue.Versioned[testservices.ServiceRecord]{Record: record, Revision: 29, ReadRevision: 40},
		serving: testreleasequeries.ServingRelease{
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
		render: testkeyvalue.Versioned[testreleaserender.ReleaseRenderInput]{
			Record:       input,
			Revision:     33,
			ReadRevision: 40,
		}, runtime: runtime,
	}
}

func sourceRuntimeFixture(
	t *testing.T,
	stamp time.Time,
	seed int64,
	input testreleaserender.ReleaseRenderInput,
	config domain.ProxyConfig,
) *testkeyvalue.Versioned[serviceruntimerecord.Record] {
	t.Helper()
	proxyPlan := ids.NewAt(ids.KindPlan, stamp, seed+6)
	proxyGeneration := uint64(17)
	workloadName := input.ServiceName
	workloadRole := agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON
	runtimeRole, slot := "singleton", ""
	if input.CandidateTarget != domain.WorkloadSingleton {
		workloadName = input.ServiceName + "--" + string(input.CandidateTarget)
		workloadRole = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT
		runtimeRole, slot = "slot", string(input.CandidateTarget)
	}
	proxy := &agentpb.ComposeService{
		ServiceId: input.ServiceID, ComposeName: input.ServiceName, ExpectedReplicas: 1,
		Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
		ExpectedLabels: sourceRuntimeLabels(
			input.EnvironmentID,
			input.ServiceID,
			proxyPlan,
			proxyGeneration,
			"proxy",
			"",
			"",
		),
		ProxyConfigJson: append(
			[]byte(nil),
			config.JSON...), ProxyConfigSha256: append([]byte(nil), config.SHA256[:]...),
	}
	workload := &agentpb.ComposeService{
		ServiceId: input.ServiceID, ComposeName: workloadName, ExpectedReplicas: input.CandidateWorkload.ReplicaCount,
		HasHealthcheck: true, Role: workloadRole, Slot: slot, ImageReference: input.CandidateWorkload.LocalImageID,
		ExpectedLabels: sourceRuntimeLabels(
			input.EnvironmentID,
			input.ServiceID,
			input.PlanID,
			input.Projection.RenderGeneration,
			runtimeRole,
			slot,
			input.ReleaseID,
		),
	}
	yaml := []byte("services: {}\n")
	yamlDigest := sha256.Sum256(yaml)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: ids.NewAt(ids.KindConfig, stamp, seed+7),
		OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:    input.EnvironmentID, ProjectName: "gp-" + strings.ToLower(input.EnvironmentID),
		CanonicalYaml: yaml, YamlSha256: yamlDigest[:], Services: []*agentpb.ComposeService{proxy, workload},
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	record := serviceruntimerecord.Record{
		EnvironmentID: input.EnvironmentID,
		Runtime:       executionplan.CandidateRuntime{},
	}
	record.Runtime.ServiceID, record.Runtime.ReleaseID = input.ServiceID, input.ReleaseID
	record.Runtime.Target, record.Runtime.ProxyGeneration = string(input.CandidateTarget), input.ProxyGeneration
	record.Runtime.ProxyConfigSHA256, record.Runtime.CurrentArtifact = append([]byte(nil), config.SHA256[:]...), encoded
	record.Source = serviceruntimerecord.Acknowledgement{
		TaskID: ids.NewAt(ids.KindTask, stamp, seed+8), PlanID: ids.NewAt(ids.KindPlan, stamp, seed+9),
		PlanHash: strings.Repeat("a", 64), StepID: ids.NewAt(ids.KindStep, stamp, seed+10),
		AgentID: ids.NewAt(ids.KindAgent, stamp, seed+11), AssignmentID: ids.NewAt(ids.KindAssignment, stamp, seed+12),
		ExecutionEpoch: 1, RenderGeneration: input.Projection.RenderGeneration,
		EffectDigest: strings.Repeat("b", 64), AcknowledgedAt: stamp,
	}
	if err := serviceruntimerecord.Validate(record); err != nil {
		t.Fatal(err)
	}
	return &testkeyvalue.Versioned[serviceruntimerecord.Record]{Record: record, Revision: 34, ReadRevision: 40}
}

func sourceRuntimeLabels(
	environmentID, serviceID, planID string,
	generation uint64,
	role, slot, releaseID string,
) []*agentpb.LabelPair {
	values := []agentpb.LabelPair{
		{Key: "com.groundplane.environment-id", Value: environmentID},
		{Key: "com.groundplane.kind", Value: "service"},
		{Key: "com.groundplane.managed", Value: "true"},
		{Key: "com.groundplane.plan-id", Value: planID},
	}
	if releaseID != "" {
		values = append(values, agentpb.LabelPair{Key: "com.groundplane.release-id", Value: releaseID})
	}
	values = append(values,
		agentpb.LabelPair{Key: "com.groundplane.render-generation", Value: strconv.FormatUint(generation, 10)},
		agentpb.LabelPair{Key: "com.groundplane.runtime-role", Value: role},
		agentpb.LabelPair{Key: "com.groundplane.service-id", Value: serviceID},
	)
	if slot != "" {
		values = append(values, agentpb.LabelPair{Key: "com.groundplane.slot", Value: slot})
	}
	result := make([]*agentpb.LabelPair, len(values))
	for index := range values {
		result[index] = &values[index]
	}
	return result
}

func rebindSourceRuntimeEnvironment(t *testing.T, fixture *sourceFixture, environmentID string) {
	t.Helper()
	if fixture.runtime == nil {
		return
	}
	artifact := new(agentpb.ComposeArtifact)
	if err := proto.Unmarshal(fixture.runtime.Record.Runtime.CurrentArtifact, artifact); err != nil {
		t.Fatal(err)
	}
	artifact.OwnerId, artifact.ProjectName = environmentID, "gp-"+strings.ToLower(environmentID)
	for _, service := range artifact.GetServices() {
		for _, pair := range service.GetExpectedLabels() {
			if pair.GetKey() == "com.groundplane.environment-id" {
				pair.Value = environmentID
			}
		}
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	fixture.runtime.Record.EnvironmentID = environmentID
	fixture.runtime.Record.Runtime.CurrentArtifact = encoded
	if err := serviceruntimerecord.Validate(fixture.runtime.Record); err != nil {
		t.Fatal(err)
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
			if len(test.proxyPorts) == 0 {
				if captured.target.ProxyComposeName != "" || len(releases.runtimeCalls) != 0 {
					t.Fatalf("portless Service gained proxy authority: %#v", captured.target)
				}
			} else if captured.target.ProxyComposeName != "api" || captured.target.ProxyPlanId == "" ||
				captured.target.ProxyPlanId == fixture.render.Record.PlanID ||
				captured.target.ProxyRenderGeneration == 0 || len(captured.target.ProxyConfigSha256) != sha256.Size ||
				captured.acknowledgedRuntimeRevision != fixture.runtime.Revision || len(releases.runtimeCalls) != 1 ||
				releases.runtimeCalls[0].revision != fixture.service.ReadRevision {
				t.Fatalf("proxy did not use exact acknowledged runtime: target=%#v calls=%v", captured.target, releases.runtimeCalls)
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

// Rationale: OBS-05; a proxied serving Release without its self-contained
// acknowledged runtime cannot authorize a live proxy comparison.
func TestCaptureRejectsMissingAcknowledgedProxyRuntime(t *testing.T) {
	fixture := newSourceFixture(t, 55, domain.StrategyRecreate, "", []uint16{8080}, 1)
	fixture.runtime = nil
	releases := &fakeReleases{fixtures: map[string]sourceFixture{
		fixture.service.Record.Desired.ID: fixture,
	}}
	if _, ok := capture(t.Context(), releases, fixture.service); ok {
		t.Fatal("missing acknowledged proxy runtime became observation authority")
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
		"runtime":              func(value *source) { value.runtimeRevision++ },
		"acknowledged runtime": func(value *source) { value.acknowledgedRuntimeRevision++ },
		"projection":           func(value *source) { value.projectionRevision++ },
		"intent":               func(value *source) { value.intentRevision++ },
		"render":               func(value *source) { value.renderRevision++ },
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
