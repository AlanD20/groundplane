package composehelper

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: an observation error is not permission to replace a container
// whose labels do not match the captured predecessor authority.
func TestServingCompensationRejectsForeignRuntimeBeforeMutation(t *testing.T) {
	t.Run("workload", func(t *testing.T) { testServingRejectsForeignRuntime(t, false) })
	t.Run("proxy with missing workload", func(t *testing.T) { testServingRejectsForeignRuntime(t, true) })
}

func testServingRejectsForeignRuntime(t *testing.T, proxyOnly bool) {
	t.Helper()
	predecessor := &agentpb.ComposeArtifact{ArtifactId: helperArtifactID, ProjectName: "gp-serving",
		Services: []*agentpb.ComposeService{{
			ServiceId: helperServiceID, ComposeName: "api", ExpectedReplicas: 1,
			Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			ImageReference: servingPredecessorImageReference,
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.runtime-role", Value: "singleton"},
				{Key: "com.groundplane.release-id", Value: candidateAbsenceReleaseID},
			},
		}},
	}
	observedLabels := servingPredecessorLabels("singleton", "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW")
	observedLabels["com.docker.compose.service"] = "api"
	if proxyOnly {
		predecessor.Services = append(predecessor.Services, &agentpb.ComposeService{
			ServiceId: helperServiceID, ComposeName: "api-proxy", ExpectedReplicas: 1,
			Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.runtime-role", Value: "proxy"},
				{Key: "com.groundplane.plan-id", Value: helperPlanID},
			},
		})
		observedLabels = servingPredecessorLabels("proxy", "")
		observedLabels["com.docker.compose.service"] = "api-proxy"
		observedLabels["com.groundplane.plan-id"] = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	}
	request, _ := sealedServingPredecessorRequest(t, predecessor)
	step := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
		CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
			ServiceId:          helperServiceID,
			CandidateReleaseId: servingRecoveryCandidateID,
		},
	}}
	labels, err := json.Marshal(observedLabels)
	if err != nil {
		t.Fatal(err)
	}
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, options runner.RunCmdOpts) (runner.Result, error) {
		if options.Args[0] != "container" {
			t.Fatalf("foreign runtime allowed mutation: %v", options.Args)
		}
		switch options.Args[1] {
		case "ls":
			return runner.Result{Stdout: []byte("0123456789abcdef\n")}, nil
		case "inspect":
			return runner.Result{Stdout: labels}, nil
		default:
			t.Fatalf("unexpected mutation: %v", options.Args)
			return runner.Result{}, nil
		}
	}
	response, err := executeServingPredecessor(context.Background(), fake, 30, request, predecessor, step)
	if err != nil || response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED {
		t.Fatalf("foreign predecessor = %v, %v", response, err)
	}
}

// Rationale: restoring one predecessor must not start configured-only members,
// expand dependencies, or delete unrelated containers through orphan removal.
func TestServingCompensationStartsOnlySelectedMember(t *testing.T) {
	for _, withProxy := range []bool{false, true} {
		name := "portless"
		if withProxy {
			name = "addressable"
		}
		t.Run(name, func(t *testing.T) { testServingCompensationScope(t, withProxy) })
	}
}

func testServingCompensationScope(t *testing.T, withProxy bool) {
	t.Helper()
	predecessor := &agentpb.ComposeArtifact{
		ArtifactId: helperArtifactID, ProjectName: "gp-serving",
		CanonicalYaml: []byte(
			"services:\n  api:\n    depends_on:\n      configured:\n        condition: service_started\n  configured: {}\n",
		),
		Services: []*agentpb.ComposeService{{
			ServiceId: helperServiceID, ComposeName: "api", ExpectedReplicas: 1,
			Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			ImageReference: servingPredecessorImageReference,
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.release-id", Value: candidateAbsenceReleaseID},
				{Key: "com.groundplane.runtime-role", Value: "singleton"},
			},
		}, {
			ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW", ComposeName: "configured", ExpectedReplicas: 1,
		}},
	}
	want := []string{"api"}
	if withProxy {
		predecessor.Services = append(predecessor.Services, &agentpb.ComposeService{
			ServiceId: helperServiceID, ComposeName: "api-proxy", ExpectedReplicas: 1,
			Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			ExpectedLabels: []*agentpb.LabelPair{{Key: "com.groundplane.runtime-role", Value: "proxy"}},
		})
		predecessor.CanonicalYaml = append(predecessor.CanonicalYaml, []byte("  api-proxy: {}\n")...)
		want = append(want, "api-proxy")
	}
	request, _ := sealedServingPredecessorRequest(t, predecessor)
	step := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
		CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
			ServiceId:          helperServiceID,
			CandidateReleaseId: servingRecoveryCandidateID,
		},
	}}
	started := false
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, options runner.RunCmdOpts) (runner.Result, error) {
		if len(options.Args) > 1 && options.Args[0] == "container" {
			return runner.Result{}, nil // No predecessor yet; force compensation.
		}
		if slices.Contains(options.Args, "config") {
			return runner.Result{}, nil
		}
		index := slices.Index(options.Args, "up")
		if index < 0 ||
			!slices.Equal(options.Args[index:], append([]string{"up", "--detach", "--no-deps", "--"}, want...)) {
			t.Fatalf("restoration exceeds member authority: %v", options.Args)
		}
		started = true
		return runner.Result{}, nil
	}
	response, err := executeServingPredecessor(context.Background(), fake, 30, request, predecessor, step)
	if err != nil || !started || response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED {
		t.Fatalf("unproven scoped restoration = %v, %v, started=%t", response, err, started)
	}
	proxy, err := servingPredecessorProxy(predecessor, helperServiceID)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := startupClosure(predecessor, servingPredecessorNames(predecessor.Services[0], proxy), false)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, service := range selected {
		names = append(names, service.ComposeName)
	}
	if !slices.Equal(names, want) {
		t.Fatalf("startup selection = %v, want %v", names, want)
	}
}
