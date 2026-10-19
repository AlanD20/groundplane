package executionplan

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

const (
	candidateRuntimeEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	candidateRuntimeServiceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	candidateRuntimeOtherService  = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	candidateRuntimeReleaseID     = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	candidateRuntimePriorRelease  = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	candidateRuntimeArtifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	candidateRuntimePriorArtifact = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	candidateRuntimePlanID        = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	candidateRuntimePriorPlanID   = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW"
)

// SVC-15/TASK-70: preserve actual proxy ownership and exact final traffic; filter
// unrelated workloads/resources without changing the captured historical input.
func TestPrepareReleaseWorkRuntimeBlueGreenPostActivation(t *testing.T) {
	attempt, input := nativeTopologyRelease(t)
	input.Candidate = candidateRuntimeFixtureArtifact(
		t,
		candidateRuntimeArtifactID,
		candidateRuntimePlanID,
		7,
		candidateRuntimeReleaseID,
		"blue",
		input.Candidate.Services[0].ProxyConfigJson,
		true,
	)
	prior := candidateRuntimeOpenArtifact(t, input.Predecessor.CurrentArtifact)
	prior.CanonicalYaml = bytes.ReplaceAll(
		prior.GetCanonicalYaml(), []byte("gp-proxy-runtime"), []byte("gp-proxy-prior"),
	)
	prior.CanonicalYaml = bytes.Replace(
		prior.GetCanonicalYaml(), []byte("    image: proxy:sealed\n"),
		[]byte("    image: proxy:sealed\n    x-runtime-source: acknowledged\n"), 1,
	)
	priorDigest := sha256.Sum256(prior.GetCanonicalYaml())
	prior.YamlSha256 = priorDigest[:]
	input.Predecessor.CurrentArtifact = nativeWitnessBytes(t, prior)
	original := proto.CloneOf(input)
	finalConfig := input.TrafficConfigurations[0]
	finalDigest := sha256.Sum256(finalConfig)
	preSwitch := slices.Clone(input.Candidate.GetServices()[0].GetProxyConfigJson())

	got, err := PrepareReleaseWorkRuntime(attempt, input)
	if err != nil {
		t.Fatalf("PrepareReleaseWorkRuntime() error = %v", err)
	}
	if !proto.Equal(input, original) {
		t.Fatal("PrepareReleaseWorkRuntime() mutated its sealed input")
	}
	if got.ServiceID != candidateRuntimeServiceID ||
		got.ReleaseID != candidateRuntimeReleaseID || got.Target != "blue" ||
		got.ProxyGeneration != uint64(7) ||
		!bytes.Equal(got.ProxyConfigSHA256, finalDigest[:]) {
		t.Fatalf("prepared runtime = %#v", got)
	}
	current := candidateRuntimeOpenArtifact(t, got.CurrentArtifact)
	if current.GetArtifactId() != candidateRuntimeArtifactID || len(current.GetServices()) != 2 {
		t.Fatalf("current artifact selection = %#v", current.GetServices())
	}
	wantWorkload := candidateRuntimeService(
		input.Candidate,
		candidateRuntimeServiceID,
		"blue",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
	)
	gotWorkload := candidateRuntimeService(
		current,
		candidateRuntimeServiceID,
		"blue",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
	)
	gotProxy := candidateRuntimeService(
		current,
		candidateRuntimeServiceID,
		"",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
	)
	wantProxy := proto.CloneOf(candidateRuntimeService(
		prior,
		candidateRuntimeServiceID,
		"",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
	))
	wantProxy.ProxyConfigJson = slices.Clone(finalConfig)
	wantProxy.ProxyConfigSha256 = slices.Clone(finalDigest[:])
	if !proto.Equal(gotWorkload, wantWorkload) || !proto.Equal(gotProxy, wantProxy) {
		t.Fatal("current artifact changed candidate workload or existing proxy ownership")
	}
	if bytes.Contains(current.GetCanonicalYaml(), preSwitch) ||
		!candidateRuntimeYAMLConfigEquals(
			t,
			current.GetCanonicalYaml(),
			"gp-proxy-prior",
			finalConfig,
		) {
		t.Fatalf("current YAML did not replace pre-switch proxy config:\n%s", current.GetCanonicalYaml())
	}
	if !bytes.Contains(current.GetCanonicalYaml(), []byte("x-runtime-source: acknowledged")) ||
		bytes.Contains(current.GetCanonicalYaml(), []byte("gp-proxy-runtime")) {
		t.Fatalf("current YAML did not retain acknowledged proxy resources:\n%s", current.GetCanonicalYaml())
	}
	for _, unwanted := range []string{"api--green:", "worker:", "unrelated-data:", "unrelated-config:"} {
		if bytes.Contains(current.GetCanonicalYaml(), []byte(unwanted)) {
			t.Fatalf("current runtime retained unrelated %q:\n%s", unwanted, current.GetCanonicalYaml())
		}
	}
	for _, wanted := range []string{"shared:", "api-data:", "app-config:", "api-secret:"} {
		if !bytes.Contains(current.GetCanonicalYaml(), []byte(wanted)) {
			t.Fatalf("current runtime lost referenced %q:\n%s", wanted, current.GetCanonicalYaml())
		}
	}

	retained := candidateRuntimeOpenArtifact(t, got.RetainedPriorArtifact)
	if retained.GetArtifactId() != candidateRuntimePriorArtifact || len(retained.GetServices()) != 1 ||
		candidateRuntimeService(
			retained,
			candidateRuntimeServiceID,
			"green",
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
		) == nil || candidateRuntimeService(
		retained,
		candidateRuntimeServiceID,
		"",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
	) != nil {
		t.Fatalf("retained artifact = %#v", retained)
	}
	if err := ValidateNativePredecessorWitness(
		candidateRuntimeEnvironmentID,
		candidateRuntimeServiceID,
		got.CurrentArtifact,
		got.RetainedPriorArtifact,
	); err != nil {
		t.Fatalf("prepared runtime is not a native witness: %v", err)
	}
	repeated, err := PrepareReleaseWorkRuntime(attempt, input)
	if err != nil || !candidateRuntimeEqual(got, repeated) {
		t.Fatalf("repeated derivation diverged: %v", err)
	}
}

// SVC-15/BP-04: first deployment records its candidate proxy, not an invented predecessor.
func TestPrepareReleaseWorkRuntimeFirstBlueGreenAppliesCandidateProxy(t *testing.T) {
	attempt, input := nativeTopologyRelease(t)
	input.Predecessor = &agentpb.ReleaseWorkPredecessor{Absent: true}
	original := proto.CloneOf(input)
	finalConfig := input.TrafficConfigurations[0]
	finalDigest := sha256.Sum256(finalConfig)
	got, err := PrepareReleaseWorkRuntime(attempt, input)
	if err != nil {
		t.Fatalf("PrepareReleaseWorkRuntime() error = %v", err)
	}
	if !proto.Equal(input, original) {
		t.Fatal("PrepareReleaseWorkRuntime() mutated its sealed first-Deploy input")
	}
	current := candidateRuntimeOpenArtifact(t, got.CurrentArtifact)
	gotProxy := candidateRuntimeService(
		current,
		candidateRuntimeServiceID,
		"",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
	)
	wantProxy := proto.CloneOf(candidateRuntimeService(
		input.Candidate,
		candidateRuntimeServiceID,
		"",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
	))
	wantProxy.ProxyConfigJson = slices.Clone(finalConfig)
	wantProxy.ProxyConfigSha256 = slices.Clone(finalDigest[:])
	if !proto.Equal(gotProxy, wantProxy) || !candidateRuntimeYAMLConfigEquals(
		t,
		current.GetCanonicalYaml(),
		"gp-proxy-runtime",
		finalConfig,
	) {
		t.Fatal("first Deploy receipt did not retain the applied candidate proxy and switched config")
	}
}

// SVC-15: conflicting shared resources cannot authorize a fabricated runtime receipt.
func TestPrepareReleaseWorkRuntimeRejectsRetainedProxyResourceConflict(t *testing.T) {
	attempt, input := nativeTopologyRelease(t)
	prior := candidateRuntimeOpenArtifact(t, input.Predecessor.CurrentArtifact)
	prior.CanonicalYaml = bytes.Replace(
		prior.GetCanonicalYaml(),
		[]byte("  shared:\n    external: true\n"),
		[]byte("  shared:\n    driver: bridge\n"),
		1,
	)
	digest := sha256.Sum256(prior.GetCanonicalYaml())
	prior.YamlSha256 = digest[:]
	input.Predecessor.CurrentArtifact = nativeWitnessBytes(t, prior)
	if _, err := PrepareReleaseWorkRuntime(attempt, input); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("PrepareReleaseWorkRuntime() error = %v, want validation.failed", err)
	}
}

// SVC-15: portless recreate preserves health policy without proxy-switch authority.
func TestPrepareReleaseWorkRuntimePortlessRecreate(t *testing.T) {
	for _, hasHealthcheck := range []bool{true, false} {
		t.Run(fmt.Sprintf("healthcheck=%t", hasHealthcheck), func(t *testing.T) {
			attempt, input := nativeTopologyRelease(t)
			input.Strategy = agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE
			input.Predecessor = &agentpb.ReleaseWorkPredecessor{Absent: true}
			input.TrafficConfigurations = nil
			input.Candidate = candidateRuntimeFixtureArtifact(
				t,
				candidateRuntimeArtifactID,
				candidateRuntimePlanID,
				7,
				candidateRuntimeReleaseID,
				"singleton",
				nil,
				true,
			)
			input.Candidate.Services[0].HasHealthcheck = hasHealthcheck
			got, err := PrepareReleaseWorkRuntime(attempt, input)
			if err != nil {
				t.Fatalf("PrepareReleaseWorkRuntime() error = %v", err)
			}
			if got.Target != "singleton" || got.ProxyGeneration != 0 ||
				len(got.ProxyConfigSHA256) != 0 || len(got.RetainedPriorArtifact) != 0 {
				t.Fatalf("portless recreate runtime = %#v", got)
			}
			current := candidateRuntimeOpenArtifact(t, got.CurrentArtifact)
			if len(current.GetServices()) != 1 || current.GetServices()[0].GetRole() !=
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON ||
				current.GetServices()[0].GetComposeName() != "api" ||
				current.GetServices()[0].GetHasHealthcheck() != hasHealthcheck ||
				bytes.Contains(current.GetCanonicalYaml(), []byte("worker:")) {
				t.Fatalf("portless recreate selection = %#v\n%s", current.GetServices(), current.GetCanonicalYaml())
			}
			if err := ValidateNativePredecessorWitness(
				candidateRuntimeEnvironmentID,
				candidateRuntimeServiceID,
				got.CurrentArtifact,
				nil,
			); err != nil {
				t.Fatalf("portless recreate runtime is not a native witness: %v", err)
			}
		})
	}
}

// TASK-70: a receipt must reject changed capture identity, bytes or traffic;
// it cannot invent a serving runtime from unrelated or contradictory inputs.
func TestPrepareReleaseWorkRuntimeRejectsInvalidAuthority(t *testing.T) {
	attempt, valid := nativeTopologyRelease(t)
	valid.Predecessor = &agentpb.ReleaseWorkPredecessor{Absent: true}
	for _, test := range []struct {
		name   string
		mutate func(*agentpb.ReleaseWorkInput)
	}{
		{"missing predecessor", func(v *agentpb.ReleaseWorkInput) { v.Predecessor = nil }},
		{"missing candidate", func(v *agentpb.ReleaseWorkInput) { v.Candidate = nil }},
		{"wrong environment", func(v *agentpb.ReleaseWorkInput) { v.EnvironmentId = "foreign" }},
		{"wrong service", func(v *agentpb.ReleaseWorkInput) { v.ServiceId = candidateRuntimeOtherService }},
		{"wrong generation", func(v *agentpb.ReleaseWorkInput) { v.RenderGeneration++ }},
		{"changed candidate bytes", func(v *agentpb.ReleaseWorkInput) { v.Candidate.CanonicalYaml[0] ^= 1 }},
		{"wrong traffic target", func(v *agentpb.ReleaseWorkInput) {
			v.TrafficConfigurations[0] = candidateRuntimeProxyConfig(v.ReleaseId, "green", 7)
		}},
		{"missing traffic", func(v *agentpb.ReleaseWorkInput) { v.TrafficConfigurations = nil }},
		{"proxy YAML differs from metadata", func(v *agentpb.ReleaseWorkInput) {
			artifact := v.Candidate
			content := []byte("content: " + strconv.Quote(string(artifact.Services[0].ProxyConfigJson)))
			artifact.CanonicalYaml = bytes.Replace(artifact.CanonicalYaml, content, []byte("content: '{}'"), 1)
			digest := sha256.Sum256(artifact.CanonicalYaml)
			artifact.YamlSha256 = digest[:]
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := proto.CloneOf(valid)
			test.mutate(input)
			if _, err := PrepareReleaseWorkRuntime(attempt, input); !errors.Is(
				err,
				errs.New(errs.KindValidationFailed, ""),
			) {
				t.Fatalf("invalid authority accepted: %v", err)
			}
		})
	}
	if _, err := PrepareReleaseWorkRuntime(attempt, nil); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("nil input accepted: %v", err)
	}
}

func candidateRuntimeFixtureArtifact(
	t *testing.T,
	artifactID, ownerPlanID string,
	generation uint64,
	releaseID, target string,
	proxyConfig []byte,
	includeUnrelated bool,
) *agentpb.ComposeArtifact {
	t.Helper()
	image := "sha256:" + strings.Repeat("a", 64)
	services := []*agentpb.ComposeService{}
	var yamlServices string
	if len(proxyConfig) != 0 {
		proxyDigest := sha256.Sum256(proxyConfig)
		services = append(services, &agentpb.ComposeService{
			ServiceId: candidateRuntimeServiceID, ComposeName: "api", ExpectedReplicas: 1,
			Role:                agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			ProxyImplementation: agentpb.ServiceProxyImplementation_SERVICE_PROXY_IMPLEMENTATION_CADDY,
			ExpectedLabels: candidateRuntimeLabels(
				ownerPlanID,
				generation,
				candidateRuntimeServiceID,
				"proxy",
				"",
				"",
			),
			ProxyConfigJson: proxyConfig, ProxyConfigSha256: proxyDigest[:],
		})
		yamlServices += "  api:\n    image: proxy:sealed\n    configs:\n      - source: gp-proxy-runtime\n" +
			"        target: /etc/caddy/groundplane-proxy.json\n    networks:\n      shared: {}\n"
	}
	if target == "singleton" {
		services = append(services, candidateRuntimeWorkload(ownerPlanID, generation, releaseID, "", "api", image,
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON))
		yamlServices += "  api:\n    image: " + image + "\n    networks:\n      shared: {}\n"
	} else {
		services = append(services, candidateRuntimeWorkload(ownerPlanID, generation, releaseID, target, "api--"+target, image,
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT))
		yamlServices += "  api--" + target + ":\n    image: " + image + "\n" + candidateRuntimeResourceBindingsYAML()
		otherTarget := "green"
		if target == "green" {
			otherTarget = "blue"
		}
		if includeUnrelated {
			services = append(services, candidateRuntimeWorkload(ownerPlanID, generation, "", otherTarget,
				"api--"+otherTarget, image, agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT))
			yamlServices += "  api--" + otherTarget + ":\n    image: " + image + "\n"
		}
	}
	if includeUnrelated {
		services = append(services, &agentpb.ComposeService{
			ServiceId: candidateRuntimeOtherService, ComposeName: "worker", ExpectedReplicas: 1,
			ExpectedLabels: candidateRuntimeLabels(
				ownerPlanID,
				generation,
				candidateRuntimeOtherService,
				"",
				"",
				"",
			),
		})
		yamlServices += "  worker:\n    image: worker:unrelated\n    volumes:\n      - type: volume\n        source: unrelated-data\n        target: /data\n"
	}
	sort.Slice(services, func(left, right int) bool {
		if services[left].GetServiceId() == services[right].GetServiceId() {
			return services[left].GetComposeName() < services[right].GetComposeName()
		}
		return services[left].GetServiceId() < services[right].GetServiceId()
	})
	configs := ""
	if len(proxyConfig) != 0 {
		configs += "  gp-proxy-runtime:\n    content: " + strconv.Quote(string(proxyConfig)) + "\n"
	}
	configs += "  app-config:\n    content: selected\n  unrelated-config:\n    content: unrelated\n"
	canonical := []byte("services:\n" + yamlServices +
		"networks:\n  shared:\n    external: true\n  unrelated-network:\n    external: true\n" +
		"volumes:\n  api-data: {}\n  unrelated-data: {}\nconfigs:\n" + configs +
		"secrets:\n  api-secret:\n    file: /sealed/api-secret\n  unrelated-secret:\n    file: /sealed/unrelated\n")
	digest := sha256.Sum256(canonical)
	return &agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: candidateRuntimeEnvironmentID, ProjectName: "gp-" + strings.ToLower(candidateRuntimeEnvironmentID),
		AuthorizedVolumeDir: "/var/lib/groundplane/volumes/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
			"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + candidateRuntimeEnvironmentID,
		CanonicalYaml: canonical, YamlSha256: digest[:], Services: services,
	}
}

func candidateRuntimeResourceBindingsYAML() string {
	return "    networks:\n      shared: {}\n" +
		"    volumes:\n      - type: volume\n        source: api-data\n        target: /data\n" +
		"    configs:\n      - source: app-config\n        target: /etc/app/config\n" +
		"    secrets:\n      - source: api-secret\n        target: api-secret\n"
}

func candidateRuntimeWorkload(
	planID string,
	generation uint64,
	releaseID, slot, composeName, image string,
	role agentpb.ComposeServiceRole,
) *agentpb.ComposeService {
	runtimeRole := "singleton"
	if role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
		runtimeRole = "slot"
	}
	return &agentpb.ComposeService{
		ServiceId: candidateRuntimeServiceID, ComposeName: composeName, ExpectedReplicas: 1,
		HasHealthcheck: true, Role: role, Slot: slot, ImageReference: image,
		ExpectedLabels: candidateRuntimeLabels(
			planID,
			generation,
			candidateRuntimeServiceID,
			runtimeRole,
			releaseID,
			slot,
		),
	}
}

func candidateRuntimeLabels(
	planID string,
	generation uint64,
	serviceID, runtimeRole, releaseID, slot string,
) []*agentpb.LabelPair {
	values := map[string]string{
		labelEnvironmentID: candidateRuntimeEnvironmentID,
		labelKind:          "service",
		labelManaged:       "true",
		labelPlanID:        planID,
		labelRenderGen:     strconv.FormatUint(generation, 10),
		labelServiceID:     serviceID,
	}
	if runtimeRole != "" {
		values[labelRuntimeRole] = runtimeRole
	}
	if releaseID != "" {
		values[labelReleaseID] = releaseID
	}
	if slot != "" {
		values[labelSlot] = slot
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	labels := make([]*agentpb.LabelPair, len(keys))
	for index, key := range keys {
		labels[index] = &agentpb.LabelPair{Key: key, Value: values[key]}
	}
	return labels
}

func candidateRuntimeProxyConfig(releaseID, target string, generation uint64) []byte {
	return []byte(fmt.Sprintf(
		`{"admin":{"listen":"127.0.0.1:2019"},"apps":{"http":{"servers":{"gp_g%d_%s_p8080":{"listen":[":8080"],"routes":[{"handle":[{"handler":"reverse_proxy","upstreams":[{"dial":"api--%s:8080"}]}]}]}}}}}`,
		generation,
		strings.ToLower(releaseID),
		target,
	))
}

func candidateRuntimeService(
	artifact *agentpb.ComposeArtifact,
	serviceID, slot string,
	role agentpb.ComposeServiceRole,
) *agentpb.ComposeService {
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() == serviceID && service.GetSlot() == slot && service.GetRole() == role {
			return service
		}
	}
	return nil
}

func candidateRuntimeOpenArtifact(t *testing.T, encoded []byte) *agentpb.ComposeArtifact {
	t.Helper()
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(encoded, artifact); err != nil {
		t.Fatalf("unmarshal prepared runtime artifact: %v", err)
	}
	return artifact
}

func candidateRuntimeYAMLConfigEquals(t *testing.T, encoded []byte, name string, want []byte) bool {
	t.Helper()
	var document struct {
		Configs map[string]struct {
			Content string `yaml:"content"`
		} `yaml:"configs"`
	}
	if err := yaml.Unmarshal(encoded, &document); err != nil {
		t.Fatalf("unmarshal prepared runtime YAML: %v", err)
	}
	return document.Configs[name].Content == string(want)
}

func candidateRuntimeEqual(left, right CandidateRuntime) bool {
	return left.ServiceID == right.ServiceID && left.ReleaseID == right.ReleaseID && left.Target == right.Target &&
		left.ProxyGeneration == right.ProxyGeneration && bytes.Equal(left.ProxyConfigSHA256, right.ProxyConfigSHA256) &&
		bytes.Equal(left.CurrentArtifact, right.CurrentArtifact) &&
		bytes.Equal(left.RetainedPriorArtifact, right.RetainedPriorArtifact)
}
