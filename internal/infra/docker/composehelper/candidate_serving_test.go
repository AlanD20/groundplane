package composehelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const servingPredecessorImageReference = "registry.example/api@sha256:" +
	"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// Rationale: serving restoration consumes the exact deterministic artifact
// sealed at claim; a digest-valid identity without those bytes is insufficient.
func TestOpenServingPredecessorRequiresExactClaimSealedArtifact(t *testing.T) {
	releaseID := "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	predecessor := &agentpb.ComposeArtifact{
		ArtifactId: helperArtifactID, ProjectName: "gp-serving", CanonicalYaml: []byte("services:\n  api: {}\n"),
		Services: []*agentpb.ComposeService{{
			ServiceId: helperServiceID, ComposeName: "api", ExpectedReplicas: 1,
			Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.release-id", Value: releaseID},
				{Key: "com.groundplane.runtime-role", Value: "singleton"},
			},
			ImageReference: servingPredecessorImageReference,
		}, {
			ServiceId: helperServiceID, ComposeName: "api-green", ExpectedReplicas: 1, Slot: "green",
			Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
			ExpectedLabels: []*agentpb.LabelPair{{Key: "com.groundplane.runtime-role", Value: "slot"}},
		}},
	}
	request, _ := sealedServingPredecessorRequest(t, predecessor)
	step := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
		CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
			ServiceId:          helperServiceID,
			CandidateReleaseId: servingRecoveryCandidateID,
		},
	}}
	opened, service, openedRelease, target, err := openServingPredecessor(
		request, &agentpb.ComposeArtifact{ProjectName: predecessor.ProjectName}, step,
	)
	if err != nil || !proto.Equal(opened, predecessor) || service.GetServiceId() != helperServiceID ||
		openedRelease != releaseID || target != "singleton" {
		t.Fatalf("openServingPredecessor() = %#v, %#v, %q, %q, %v", opened, service, openedRelease, target, err)
	}
	slotPredecessor := proto.Clone(predecessor).(*agentpb.ComposeArtifact)
	slotPredecessor.Services[0].Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT
	slotPredecessor.Services[0].Slot = "green"
	slotPredecessor.Services[0].ExpectedLabels[1].Value = "slot"
	slotRequest, _ := sealedServingPredecessorRequest(t, slotPredecessor)
	_, _, _, slotTarget, slotErr := openServingPredecessor(
		slotRequest, &agentpb.ComposeArtifact{ProjectName: predecessor.ProjectName}, step,
	)
	if slotErr != nil || slotTarget != "green" {
		t.Fatalf("openServingPredecessor(slot) target = %q, error = %v", slotTarget, slotErr)
	}
	for _, test := range []struct {
		name   string
		labels []*agentpb.LabelPair
		mutate func(*agentpb.ComposeService)
	}{
		{name: "absent runtime role", labels: []*agentpb.LabelPair{{Key: "com.groundplane.release-id", Value: releaseID}}},
		{name: "conflicting runtime role", labels: []*agentpb.LabelPair{
			{Key: "com.groundplane.release-id", Value: releaseID},
			{Key: "com.groundplane.runtime-role", Value: "slot"},
		}},
		{name: "conflicting slot runtime role", labels: []*agentpb.LabelPair{
			{Key: "com.groundplane.release-id", Value: releaseID},
			{Key: "com.groundplane.runtime-role", Value: "singleton"},
		}, mutate: func(service *agentpb.ComposeService) {
			service.Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT
			service.Slot = "green"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := proto.Clone(predecessor).(*agentpb.ComposeArtifact)
			invalid.Services[0].ExpectedLabels = test.labels
			if test.mutate != nil {
				test.mutate(invalid.Services[0])
			}
			invalidRequest, _ := sealedServingPredecessorRequest(t, invalid)
			if _, _, _, _, err := openServingPredecessor(
				invalidRequest, &agentpb.ComposeArtifact{ProjectName: predecessor.ProjectName}, step,
			); err == nil {
				t.Fatal("openServingPredecessor() accepted invalid sealed runtime role")
			}
		})
	}
	request.RestorationAuthority.AppliedPredecessor.ComposeArtifact[0] ^= 0xff
	if _, _, _, _, err := openServingPredecessor(
		request, &agentpb.ComposeArtifact{ProjectName: predecessor.ProjectName}, step,
	); err == nil {
		t.Fatal("openServingPredecessor() accepted changed artifact bytes")
	}
}

// Rationale: the helper must open the immutable native C snapshot selected by
// the claim, even when the independent applied A witness contains a different
// historical service. The current blue/green snapshot includes one proxy;
// the retained inactive snapshot is workload-only observation data.
func TestOpenServingPredecessorUsesNativeWitnessOverAppliedArtifact(t *testing.T) {
	const (
		nativeCurrentArtifact  = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		nativeRetainedArtifact = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAX"
		nativeCurrentRelease   = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		nativeRetainedRelease  = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	)
	current := nativeHelperArtifact(t, nativeCurrentArtifact, nativeCurrentRelease, "blue", true)
	retained := nativeHelperArtifact(t, nativeRetainedArtifact, nativeRetainedRelease, "green", false)
	applied := &agentpb.ComposeArtifact{
		ArtifactId: helperArtifactID, ProjectName: current.ProjectName,
		Services: []*agentpb.ComposeService{{
			ServiceId: helperServiceID, ComposeName: "api-applied", ExpectedReplicas: 1,
			Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			ImageReference: servingPredecessorImageReference,
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.release-id", Value: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAY"},
				{Key: "com.groundplane.runtime-role", Value: "singleton"},
			},
		}},
	}
	appliedBytes := marshalNativeHelperArtifact(t, applied)
	appliedDigest := sha256.Sum256(appliedBytes)
	currentBytes := marshalNativeHelperArtifact(t, current)
	retainedBytes := marshalNativeHelperArtifact(t, retained)
	request := &agentpb.ComposeHelperRequest{RestorationAuthority: &agentpb.ReleaseRestorationAuthority{
		EnvironmentId: helperEnvironmentID,
		Candidates: []*agentpb.ReleaseRestorationCandidate{{
			ServiceId: helperServiceID, ReleaseId: servingRecoveryCandidateID,
			Target: agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR,
		}},
		AppliedPredecessor: &agentpb.ReleaseAppliedPredecessorAuthority{
			ComposeArtifact: appliedBytes, ComposeArtifactSha256: appliedDigest[:],
		},
		NativePredecessors: []*agentpb.ReleaseNativePredecessorAuthority{{
			ServiceId: helperServiceID, CurrentArtifact: currentBytes, RetainedPriorArtifact: retainedBytes,
		}},
	}}
	step := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
		CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
			ServiceId: helperServiceID, CandidateReleaseId: servingRecoveryCandidateID,
		},
	}}
	opened, service, releaseID, target, err := openServingPredecessor(
		request, &agentpb.ComposeArtifact{ProjectName: current.ProjectName}, step,
	)
	if err != nil || !proto.Equal(opened, current) || service.GetServiceId() != helperServiceID ||
		releaseID != nativeCurrentRelease || target != "blue" {
		t.Fatalf("open native predecessor = %#v, %#v, %q, %q, %v", opened, service, releaseID, target, err)
	}
	if !bytes.Equal(request.RestorationAuthority.AppliedPredecessor.ComposeArtifact, appliedBytes) ||
		!bytes.Equal(request.RestorationAuthority.AppliedPredecessor.ComposeArtifactSha256, appliedDigest[:]) {
		t.Fatal("native helper selection changed applied A witness")
	}

	for name, mutate := range map[string]func(*agentpb.ComposeHelperRequest){
		"duplicate native witness": func(value *agentpb.ComposeHelperRequest) {
			value.RestorationAuthority.NativePredecessors = append(value.RestorationAuthority.NativePredecessors,
				proto.Clone(value.RestorationAuthority.NativePredecessors[0]).(*agentpb.ReleaseNativePredecessorAuthority))
		},
		"altered native bytes": func(value *agentpb.ComposeHelperRequest) {
			value.RestorationAuthority.NativePredecessors[0].CurrentArtifact[0] ^= 0xff
		},
		"native absence": func(value *agentpb.ComposeHelperRequest) {
			value.RestorationAuthority.NativePredecessors[0].CurrentArtifact = nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(request).(*agentpb.ComposeHelperRequest)
			mutate(changed)
			if _, _, _, _, err := openServingPredecessor(
				changed, &agentpb.ComposeArtifact{ProjectName: current.ProjectName}, step,
			); err == nil {
				t.Fatal("accepted incomplete or altered native witness")
			}
		})
	}
}

func nativeHelperArtifact(t *testing.T, artifactID, releaseID, slot string, proxy bool) *agentpb.ComposeArtifact {
	t.Helper()
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: helperEnvironmentID, ProjectName: "gp-" + strings.ToLower(helperEnvironmentID),
		Services: []*agentpb.ComposeService{{
			ServiceId: helperServiceID, ComposeName: "api-" + slot, ExpectedReplicas: 1,
			HasHealthcheck: true, Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
			Slot: slot, ImageReference: "sha256:" + strings.Repeat("a", 64),
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.release-id", Value: releaseID},
				{Key: "com.groundplane.runtime-role", Value: "slot"},
				{Key: "com.groundplane.slot", Value: slot},
			},
		}},
	}
	if proxy {
		artifact.Services = append(artifact.Services, &agentpb.ComposeService{
			ServiceId: helperServiceID, ComposeName: "api-proxy",
			Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			ExpectedLabels: []*agentpb.LabelPair{{Key: "com.groundplane.runtime-role", Value: "proxy"}},
		})
	}
	return artifact
}

func marshalNativeHelperArtifact(t *testing.T, artifact *agentpb.ComposeArtifact) []byte {
	t.Helper()
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// Rationale: the stable proxy shares the Service id but is not a workload
// replica; restoration succeeds only for the exact healthy predecessor count.
func TestServingPredecessorCountsOnlyExactHealthyWorkloadLineage(t *testing.T) {
	const releaseID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	workloadLabels := servingPredecessorLabels("singleton", releaseID)
	proxyLabels := servingPredecessorLabels("proxy", "")
	workload := func(id string) servingPredecessorObservation {
		return servingPredecessorObservation{
			id: id, labels: workloadLabels, image: servingPredecessorImageReference,
			state: `{"Running":true,"Health":{"Status":"healthy"}}`,
		}
	}
	proxy := servingPredecessorObservation{id: "aaaaaaaaaaaaaaaa", labels: proxyLabels}
	one := workload("1111111111111111")
	two := workload("2222222222222222")
	three := workload("3333333333333333")
	missingImage := workload("1111111111111111")
	missingImage.image = ""
	wrongImage := workload("1111111111111111")
	wrongImage.image = "registry.example/api@sha256:" + strings.Repeat("b", 64)
	for _, test := range []struct {
		name         string
		replicas     uint32
		observations []servingPredecessorObservation
		mutate       func(*agentpb.ComposeService)
		wantProven   bool
		wantUnproven bool
	}{
		{
			name: "one workload excludes stable proxy", replicas: 1,
			observations: []servingPredecessorObservation{proxy, one}, wantProven: true,
		},
		{
			name: "three workloads exclude stable proxy", replicas: 3,
			observations: []servingPredecessorObservation{proxy, one, two, three}, wantProven: true,
		},
		{name: "missing workload", replicas: 3, observations: []servingPredecessorObservation{proxy, one, two}, wantUnproven: true},
		{name: "duplicate workload identity", replicas: 2, observations: []servingPredecessorObservation{proxy, one, one}},
		{name: "extra workload", replicas: 1, observations: []servingPredecessorObservation{proxy, one, two}},
		{name: "unhealthy workload", replicas: 1, wantUnproven: true, observations: []servingPredecessorObservation{proxy, {
			id: "1111111111111111", labels: workloadLabels, state: `{"Running":true,"Health":{"Status":"unhealthy"}}`,
			image: servingPredecessorImageReference,
		}}},
		{name: "mixed workload lineage", replicas: 2, observations: []servingPredecessorObservation{proxy, one, {
			id: "2222222222222222", labels: servingPredecessorLabels("singleton", "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"),
			image: servingPredecessorImageReference,
			state: `{"Running":true,"Health":{"Status":"healthy"}}`,
		}}},
		{name: "missing sealed image", replicas: 1, observations: []servingPredecessorObservation{proxy, one},
			mutate: func(service *agentpb.ComposeService) { service.ImageReference = "" }},
		{name: "missing observed image", replicas: 1, observations: []servingPredecessorObservation{proxy, missingImage}},
		{name: "wrong observed image", replicas: 1, observations: []servingPredecessorObservation{proxy, wrongImage}},
	} {
		t.Run(test.name, func(t *testing.T) {
			artifact := &agentpb.ComposeArtifact{ArtifactId: helperArtifactID, ProjectName: "gp-serving"}
			service := &agentpb.ComposeService{
				ServiceId: helperServiceID, ExpectedReplicas: test.replicas, HasHealthcheck: true,
				Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
				ImageReference: servingPredecessorImageReference,
				ExpectedLabels: []*agentpb.LabelPair{
					{Key: "com.groundplane.environment-id", Value: helperEnvironmentID},
					{Key: "com.groundplane.kind", Value: "service"},
					{Key: "com.groundplane.managed", Value: "true"},
					{Key: "com.groundplane.plan-id", Value: helperPlanID},
					{Key: "com.groundplane.project-id", Value: helperProjectID},
					{Key: "com.groundplane.release-id", Value: releaseID},
					{Key: "com.groundplane.render-generation", Value: "7"},
					{Key: "com.groundplane.runtime-role", Value: "singleton"},
					{Key: "com.groundplane.service-id", Value: helperServiceID},
					{Key: "com.groundplane.tenant-id", Value: helperTenantID},
				},
			}
			if test.mutate != nil {
				test.mutate(service)
			}
			fake := servingPredecessorRunner(t, artifact.GetProjectName(), test.observations)
			proven, err := servingPredecessorProven(context.Background(), fake, artifact, service)
			if test.wantProven && (err != nil || !proven) {
				t.Fatalf("servingPredecessorProven() = %t, %v, want proven", proven, err)
			}
			if test.wantUnproven && (err != nil || proven) {
				t.Fatalf("servingPredecessorProven() = %t, %v, want owned but unproven", proven, err)
			}
			if !test.wantProven && !test.wantUnproven && (err == nil || proven) {
				t.Fatalf("servingPredecessorProven() = %t, %v, want rejection", proven, err)
			}
		})
	}
}

type servingPredecessorObservation struct {
	id     string
	labels map[string]string
	image  string
	state  string
}

func servingPredecessorLabels(role, releaseID string) map[string]string {
	labels := map[string]string{
		"com.docker.compose.project":        "gp-serving",
		"com.groundplane.environment-id":    helperEnvironmentID,
		"com.groundplane.kind":              "service",
		"com.groundplane.managed":           "true",
		"com.groundplane.plan-id":           helperPlanID,
		"com.groundplane.project-id":        helperProjectID,
		"com.groundplane.render-generation": "7",
		"com.groundplane.runtime-role":      role,
		"com.groundplane.service-id":        helperServiceID,
		"com.groundplane.tenant-id":         helperTenantID,
	}
	if releaseID != "" {
		labels["com.groundplane.release-id"] = releaseID
	}
	return labels
}

func servingPredecessorRunner(
	t *testing.T,
	projectName string,
	observations []servingPredecessorObservation,
) *runner.FakeRunner {
	t.Helper()
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, options runner.RunCmdOpts) (runner.Result, error) {
		listArgs := []string{
			"container", "ls", "--all", "--filter", "label=com.docker.compose.project=" + projectName, "--format", "{{.ID}}",
		}
		if slices.Equal(options.Args, listArgs) {
			ids := make([]string, len(observations))
			for index, observation := range observations {
				ids[index] = observation.id
			}
			return runner.Result{Stdout: []byte(strings.Join(ids, "\n") + "\n")}, nil
		}
		for _, observation := range observations {
			labelsArgs := []string{"container", "inspect", "--format", "{{json .Config.Labels}}", observation.id}
			if slices.Equal(options.Args, labelsArgs) {
				encoded, err := json.Marshal(observation.labels)
				if err != nil {
					t.Fatal(err)
				}
				return runner.Result{Stdout: encoded}, nil
			}
			imageArgs := []string{"container", "inspect", "--format", "{{.Config.Image}}", observation.id}
			if slices.Equal(options.Args, imageArgs) {
				return runner.Result{Stdout: []byte(observation.image + "\n")}, nil
			}
			stateArgs := []string{"container", "inspect", "--format", "{{json .State}}", observation.id}
			if slices.Equal(options.Args, stateArgs) {
				return runner.Result{Stdout: []byte(observation.state)}, nil
			}
		}
		t.Fatalf("unexpected Docker argv: %#v", options.Args)
		return runner.Result{}, nil
	}
	return fake
}

func sealedServingPredecessorRequest(
	t *testing.T,
	predecessor *agentpb.ComposeArtifact,
) (*agentpb.ComposeHelperRequest, []byte) {
	t.Helper()
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(predecessor)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	return &agentpb.ComposeHelperRequest{RestorationAuthority: &agentpb.ReleaseRestorationAuthority{
		Candidates: []*agentpb.ReleaseRestorationCandidate{
			{
				ServiceId: helperServiceID,
				ReleaseId: servingRecoveryCandidateID,
				Target:    agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR,
			},
		},
		AppliedPredecessor: &agentpb.ReleaseAppliedPredecessorAuthority{
			ComposeArtifact: encoded, ComposeArtifactSha256: digest[:],
		},
	}}, encoded
}
