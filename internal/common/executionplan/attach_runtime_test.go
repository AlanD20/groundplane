package executionplan

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

const (
	attachRuntimeArtifactID = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	attachRuntimePlanID     = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	attachRuntimeTargetID   = "att_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	attachRuntimeStepID     = "step_01ARZ3NDEKTSV4RRFFQ69G5FBA"
)

// QA: ATT-08, ATT-10; local plan projection, not live qualification.
// Rationale: successful Attach and Detach mutate only the selected running
// workload; release authority, the stable proxy, and the retained inactive
// slot must remain the last acknowledged bytes rather than the render source.
func TestPrepareAttachRuntimesPreservesUnselectedAuthority(t *testing.T) {
	for _, operation := range []agentpb.PlanOperation{
		agentpb.PlanOperation_PLAN_OPERATION_ATTACH,
		agentpb.PlanOperation_PLAN_OPERATION_DETACH,
	} {
		t.Run(operation.String(), func(t *testing.T) {
			plan, previous := attachRuntimePlan(t, operation, true)
			originalPlan := proto.CloneOf(plan)
			originalPrevious := cloneAttachCandidateRuntime(previous)

			got, err := PrepareAttachRuntimes(plan, []CandidateRuntime{previous})
			if err != nil {
				t.Fatalf("PrepareAttachRuntimes() error = %v", err)
			}
			if !proto.Equal(plan, originalPlan) || !candidateRuntimeEqual(previous, originalPrevious) {
				t.Fatal("PrepareAttachRuntimes() mutated an input")
			}
			if len(got) != 1 || got[0].ServiceID != previous.ServiceID ||
				got[0].ReleaseID != previous.ReleaseID || got[0].Target != previous.Target ||
				got[0].ProxyGeneration != previous.ProxyGeneration ||
				!bytes.Equal(got[0].ProxyConfigSHA256, previous.ProxyConfigSHA256) ||
				!bytes.Equal(got[0].RetainedPriorArtifact, previous.RetainedPriorArtifact) {
				t.Fatalf("prepared runtime = %#v", got)
			}

			current := candidateRuntimeOpenArtifact(t, got[0].CurrentArtifact)
			priorCurrent := candidateRuntimeOpenArtifact(t, previous.CurrentArtifact)
			source := plan.GetArtifacts()[0]
			selected := candidateRuntimeService(
				source,
				previous.ServiceID,
				previous.Target,
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
			)
			workload := candidateRuntimeService(
				current,
				previous.ServiceID,
				previous.Target,
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
			)
			proxy := candidateRuntimeService(
				current,
				previous.ServiceID,
				"",
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			)
			priorProxy := candidateRuntimeService(
				priorCurrent,
				previous.ServiceID,
				"",
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			)
			if current.GetArtifactId() != attachRuntimeArtifactID || len(current.GetServices()) != 2 ||
				!proto.Equal(workload, selected) || !proto.Equal(proxy, priorProxy) {
				t.Fatalf("current runtime services = %#v", current.GetServices())
			}
			if !bytes.Contains(current.GetCanonicalYaml(), []byte("attached-network")) ||
				bytes.Contains(current.GetCanonicalYaml(), []byte("SOURCE_PROXY_SENTINEL")) ||
				bytes.Contains(current.GetCanonicalYaml(), []byte("INACTIVE_SENTINEL")) ||
				bytes.Contains(current.GetCanonicalYaml(), []byte("api--green:")) {
				t.Fatalf("current runtime YAML widened the selected mutation:\n%s", current.GetCanonicalYaml())
			}
			if !bytes.Equal(
				attachRuntimeYAMLServiceBytes(t, current.GetCanonicalYaml(), "api"),
				attachRuntimeYAMLServiceBytes(t, priorCurrent.GetCanonicalYaml(), "api"),
			) {
				t.Fatal("stable proxy YAML changed")
			}
			assertAttachRuntimeCanonical(t, got[0])
			if err := ValidateNativePredecessorWitness(
				candidateRuntimeEnvironmentID,
				previous.ServiceID,
				got[0].CurrentArtifact,
				got[0].RetainedPriorArtifact,
			); err != nil {
				t.Fatalf("prepared runtime is not a native witness: %v", err)
			}
			repeated, err := PrepareAttachRuntimes(plan, []CandidateRuntime{previous})
			if err != nil || len(repeated) != 1 || !candidateRuntimeEqual(got[0], repeated[0]) {
				t.Fatalf("repeated projection diverged: %#v, %v", repeated, err)
			}
		})
	}
}

// QA: ATT-10, SVC-13; local authority rejection.
// Rationale: an Attach compiler cannot fabricate acknowledged runtime from a
// missing Release input, accept a different release/slot, or choose between
// duplicate records for the same selected Service.
func TestPrepareAttachRuntimesRejectsMissingWrongOrAmbiguousPredecessor(t *testing.T) {
	plan, previous := attachRuntimePlan(t, agentpb.PlanOperation_PLAN_OPERATION_ATTACH, true)
	tests := []struct {
		name     string
		previous []CandidateRuntime
	}{
		{name: "missing"},
		{name: "wrong release", previous: []CandidateRuntime{func() CandidateRuntime {
			value := cloneAttachCandidateRuntime(previous)
			value.ReleaseID = candidateRuntimePriorRelease
			return value
		}()}},
		{name: "wrong target", previous: []CandidateRuntime{func() CandidateRuntime {
			value := cloneAttachCandidateRuntime(previous)
			value.Target = "green"
			return value
		}()}},
		{name: "wrong proxy hash", previous: []CandidateRuntime{func() CandidateRuntime {
			value := cloneAttachCandidateRuntime(previous)
			value.ProxyConfigSHA256[0] ^= 0xff
			return value
		}()}},
		{name: "ambiguous", previous: []CandidateRuntime{
			cloneAttachCandidateRuntime(previous),
			cloneAttachCandidateRuntime(previous),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := PrepareAttachRuntimes(plan, test.previous); !errors.Is(
				err,
				errs.New(errs.KindValidationFailed, ""),
			) {
				t.Fatalf("PrepareAttachRuntimes() error = %v, want validation.failed", err)
			}
		})
	}
}

// QA: ATT-09; local stopped-selection proof.
// Rationale: a stopped Attach or Detach is validation-only and must not
// backfill a missing runtime record or acknowledge any captured inactive bytes.
func TestPrepareAttachRuntimesStoppedSelectionReturnsNoUpdate(t *testing.T) {
	for _, operation := range []agentpb.PlanOperation{
		agentpb.PlanOperation_PLAN_OPERATION_ATTACH,
		agentpb.PlanOperation_PLAN_OPERATION_DETACH,
	} {
		plan, _ := attachRuntimePlan(t, operation, false)
		original := proto.CloneOf(plan)
		got, err := PrepareAttachRuntimes(plan, nil)
		if err != nil || len(got) != 0 {
			t.Fatalf("PrepareAttachRuntimes(%s) = %#v, %v", operation, got, err)
		}
		if !proto.Equal(plan, original) {
			t.Fatal("PrepareAttachRuntimes() mutated a stopped plan")
		}
	}
}

// QA: ATT-10; local operation-scope proof.
// Rationale: only the closed Attach/Detach plan contract may advance runtime;
// an otherwise valid ordinary Release plan is not interchangeable authority.
func TestPrepareAttachRuntimesRejectsOtherOperations(t *testing.T) {
	plan := candidateRuntimeBlueGreenPlan(t, true)
	if _, err := PrepareAttachRuntimes(plan, nil); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("PrepareAttachRuntimes() error = %v, want validation.failed", err)
	}
}

// QA: ATT-08.
// Rationale: preserving the proxy service node is insufficient if a replaced
// top-level network definition changes the network that proxy would restore.
func TestPrepareAttachRuntimesRejectsChangedSharedProxyResource(t *testing.T) {
	plan, previous := attachRuntimePlan(t, agentpb.PlanOperation_PLAN_OPERATION_ATTACH, true)
	artifact := plan.GetArtifacts()[0]
	var document yaml.Node
	if err := yaml.Unmarshal(artifact.GetCanonicalYaml(), &document); err != nil {
		t.Fatal(err)
	}
	shared := candidateRuntimeMappingValue(candidateRuntimeMappingValue(document.Content[0], "networks"), "shared")
	shared.Content = append(shared.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "name"},
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "different-shared-network"})
	encoded, err := yaml.Marshal(&document)
	if err != nil {
		t.Fatal(err)
	}
	artifact.CanonicalYaml = encoded
	digest := sha256.Sum256(encoded)
	artifact.YamlSha256 = slices.Clone(digest[:])
	plan.PlanHash = nil
	plan, err = Seal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareAttachRuntimes(plan, []CandidateRuntime{previous}); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("changed shared proxy network accepted: %v", err)
	}
}

// QA: ATT-08.
// Rationale: equivalent mapping order must not become a false rejection,
// while changed scalar types, values and ordered sequences remain significant.
func TestAttachRuntimeSharedResourceEquality(t *testing.T) {
	for _, test := range []struct {
		name, left, right string
		equal             bool
	}{
		{"mapping order", "{external: true, name: shared}", "{name: shared, external: true}", true},
		{"changed name", "{external: true, name: shared}", "{external: true, name: other}", false},
		{"scalar type", "{external: true}", "{external: 'true'}", false},
		{"sequence order", "{names: [a, b]}", "{names: [b, a]}", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var left, right yaml.Node
			if yaml.Unmarshal([]byte(test.left), &left) != nil || yaml.Unmarshal([]byte(test.right), &right) != nil {
				t.Fatal("invalid fixture")
			}
			if attachRuntimeYAMLEqual(&left, &right) != test.equal {
				t.Fatal("resource comparison differs")
			}
		})
	}
}

func attachRuntimePlan(
	t *testing.T,
	operation agentpb.PlanOperation,
	running bool,
) (*agentpb.ExecutionPlan, CandidateRuntime) {
	t.Helper()
	prepared, err := PrepareCandidateRuntimes(candidateRuntimeBlueGreenPlan(t, true))
	if err != nil || len(prepared) != 1 {
		t.Fatalf("prepare predecessor = %#v, %v", prepared, err)
	}
	previous := prepared[0]
	artifact := proto.CloneOf(candidateRuntimeOpenArtifact(t, previous.CurrentArtifact))
	artifact.ArtifactId = attachRuntimeArtifactID
	retained := candidateRuntimeOpenArtifact(t, previous.RetainedPriorArtifact)
	inactive := proto.CloneOf(retained.GetServices()[0])
	inactive.ExpectedReplicas = 0
	inactive.ImageReference = "sha256:" + strings.Repeat("b", 64)
	artifact.Services = append(artifact.Services, inactive)
	sort.Slice(artifact.Services, func(left, right int) bool {
		leftKey := artifact.Services[left].GetServiceId() + "\x00" + artifact.Services[left].GetComposeName()
		rightKey := artifact.Services[right].GetServiceId() + "\x00" + artifact.Services[right].GetComposeName()
		return leftKey < rightKey
	})
	if !running {
		candidateRuntimeService(
			artifact,
			previous.ServiceID,
			previous.Target,
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
		).ExpectedReplicas = 0
	}
	mutateAttachRuntimeYAML(t, artifact, retained, previous, running)
	selected := []string(nil)
	if running {
		selected = []string{previous.ServiceID}
	}
	plan, err := Seal(&agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: attachRuntimePlanID, RenderGeneration: 8,
		Operation: operation, TargetId: attachRuntimeTargetID,
		Artifacts: []*agentpb.ComposeArtifact{artifact},
		Steps: []*agentpb.ExecutionStep{{
			StepId: attachRuntimeStepID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: artifact.GetArtifactId(), ServiceIds: selected, NoDependencies: true,
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Seal(%s Attach runtime fixture) error = %v", operation, err)
	}
	return plan, previous
}

func mutateAttachRuntimeYAML(
	t *testing.T,
	artifact, retained *agentpb.ComposeArtifact,
	previous CandidateRuntime,
	running bool,
) {
	t.Helper()
	var document, retainedDocument yaml.Node
	if yaml.Unmarshal(artifact.GetCanonicalYaml(), &document) != nil ||
		yaml.Unmarshal(retained.GetCanonicalYaml(), &retainedDocument) != nil {
		t.Fatal("unmarshal Attach runtime fixture YAML")
	}
	root, retainedRoot := document.Content[0], retainedDocument.Content[0]
	workload := attachRuntimeTestServiceNode(t, root, "api--blue")
	if running {
		networks := candidateRuntimeMappingValue(workload, "networks")
		networks.Content = append(networks.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "attached-network"},
			&yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"},
		)
		definitions := candidateRuntimeMappingValue(root, "networks")
		definitions.Content = append(definitions.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "attached-network"},
			&yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: []*yaml.Node{
				{Kind: yaml.ScalarNode, Tag: "!!str", Value: "external"},
				{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "true"},
			}},
		)
	}
	proxy := attachRuntimeTestServiceNode(t, root, "api")
	proxy.Content = append(proxy.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "environment"},
		&yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: "SOURCE_PROXY_SENTINEL"},
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: "present"},
		}},
	)
	sourceProxyConfig := candidateRuntimeProxyConfig(previous.ReleaseID, "green", previous.ProxyGeneration+1)
	sourceProxy := candidateRuntimeService(
		artifact,
		previous.ServiceID,
		"",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
	)
	sourceProxy.ProxyConfigJson = sourceProxyConfig
	sourceProxyDigest := sha256.Sum256(sourceProxyConfig)
	sourceProxy.ProxyConfigSha256 = slices.Clone(sourceProxyDigest[:])
	config := candidateRuntimeMappingValue(candidateRuntimeMappingValue(root, "configs"), "gp-proxy-runtime")
	candidateRuntimeMappingValue(config, "content").Value = string(sourceProxyConfig)

	retainedWorkload := attachRuntimeTestServiceNode(t, retainedRoot, "api--green")
	retainedWorkload.Content = append(retainedWorkload.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "environment"},
		&yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: "INACTIVE_SENTINEL"},
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: "present"},
		}},
	)
	services := candidateRuntimeMappingValue(root, "services")
	services.Content = append(services.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "api--green"},
		retainedWorkload,
	)
	encoded, err := yaml.Marshal(&document)
	if err != nil {
		t.Fatalf("marshal Attach runtime fixture YAML: %v", err)
	}
	artifact.CanonicalYaml = encoded
	digest := sha256.Sum256(encoded)
	artifact.YamlSha256 = slices.Clone(digest[:])
}

func attachRuntimeTestServiceNode(t *testing.T, root *yaml.Node, name string) *yaml.Node {
	t.Helper()
	services := candidateRuntimeMappingValue(root, "services")
	index, err := attachRuntimeYAMLMappingIndex(services, name)
	if err != nil || index < 0 {
		t.Fatalf("find fixture Service %q: %v", name, err)
	}
	return services.Content[index+1]
}

func attachRuntimeYAMLServiceBytes(t *testing.T, encoded []byte, name string) []byte {
	t.Helper()
	var document yaml.Node
	if err := yaml.Unmarshal(encoded, &document); err != nil {
		t.Fatalf("unmarshal runtime YAML: %v", err)
	}
	service := attachRuntimeTestServiceNode(t, document.Content[0], name)
	result, err := yaml.Marshal(service)
	if err != nil {
		t.Fatalf("marshal runtime Service YAML: %v", err)
	}
	return result
}

func assertAttachRuntimeCanonical(t *testing.T, runtime CandidateRuntime) {
	t.Helper()
	artifact := candidateRuntimeOpenArtifact(t, runtime.CurrentArtifact)
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil || !bytes.Equal(encoded, runtime.CurrentArtifact) {
		t.Fatalf("runtime protobuf is not canonical: %v", err)
	}
	var document yaml.Node
	if yaml.Unmarshal(artifact.GetCanonicalYaml(), &document) != nil {
		t.Fatal("runtime YAML is invalid")
	}
	canonical, err := yaml.Marshal(&document)
	if err != nil || !bytes.Equal(canonical, artifact.GetCanonicalYaml()) {
		t.Fatalf("runtime YAML is not canonical: %v", err)
	}
	digest := sha256.Sum256(artifact.GetCanonicalYaml())
	if !bytes.Equal(digest[:], artifact.GetYamlSha256()) {
		t.Fatal("runtime YAML hash does not match")
	}
}

func cloneAttachCandidateRuntime(value CandidateRuntime) CandidateRuntime {
	return CandidateRuntime{
		ServiceID: value.ServiceID, ReleaseID: value.ReleaseID, Target: value.Target,
		ProxyGeneration:       value.ProxyGeneration,
		ProxyConfigSHA256:     slices.Clone(value.ProxyConfigSHA256),
		CurrentArtifact:       slices.Clone(value.CurrentArtifact),
		RetainedPriorArtifact: slices.Clone(value.RetainedPriorArtifact),
	}
}
