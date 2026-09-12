package controller

import (
	"bytes"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: compensation restores historical configuration as well as the
// image. Re-rendering the predecessor from new desired state changes that state.
func TestOrdinaryReleasePreservesPriorConfiguration(t *testing.T) {
	resolver, task, input := redeployRestorationInput(t, domain.StrategyRecreate)
	member := &input.Members[0]
	before := bytes.Clone(member.Render.PriorRuntime.CurrentArtifact)
	project, err := LoadNormalizedEnvironmentProject(t.Context(), member.Render.Projection)
	if err != nil {
		t.Fatal(err)
	}
	workload := project.Services[member.Render.ServiceName]
	value := "new-candidate-only"
	workload.Environment = map[string]*string{"RECOVERY_CONFIG_PROBE": &value}
	project.Services[member.Render.ServiceName] = workload
	member.Render.Projection.NormalizedCompose, err = MarshalNormalizedEnvironmentProject(project)
	if err != nil {
		t.Fatal(err)
	}
	_, plan, err := resolver.PrepareReleaseTask(t.Context(), task, input)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(plan.Artifacts[0].CanonicalYaml, []byte(value)) {
		t.Fatal("candidate did not use changed desired configuration")
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(plan.Artifacts[1])
	if err != nil || !bytes.Equal(encoded, before) || bytes.Contains(plan.Artifacts[1].CanonicalYaml, []byte(value)) {
		t.Fatalf("predecessor configuration changed with candidate: %v", err)
	}
	// Historical labels do not authorize an ordinary forward ComposeApply.
	corrupt := proto.CloneOf(plan)
	for _, step := range corrupt.Steps {
		if apply := step.GetComposeApply(); apply != nil {
			apply.ArtifactId = member.Render.PriorArtifactID
			break
		}
	}
	if _, err := executionplan.Seal(corrupt); err == nil {
		t.Fatal("historical runtime was accepted as a forward candidate")
	}
}

// Rationale: native history may include both blue/green slots; ordinary
// compensation must retain and execute the complete sealed restoration pair.
func TestOrdinaryReleaseRetainsInactivePredecessor(t *testing.T) {
	resolver, task, input := redeployRestorationInput(t, domain.StrategyBlueGreen)
	member := &input.Members[0]
	source := member.Render
	source.ReleaseID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FC2"
	source.ArtifactID = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FC2"
	source.CandidateWorkload = *member.Render.PriorWorkload
	source.Slot, source.CandidateTarget = domain.SlotGreen, domain.WorkloadGreen
	retained, err := resolver.renderServiceLifecycleArtifact(t.Context(), source, "", false)
	if err != nil {
		t.Fatal(err)
	}
	member.Render.PriorRuntime.RetainedPriorArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(
		retained,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, plan, err := resolver.PrepareReleaseTask(t.Context(), task, input)
	if err != nil {
		t.Fatal(err)
	}
	prior := plan.CandidateReleaseProcedure.Members[0].ServingPredecessor
	if len(plan.Artifacts) != 3 || prior.RetainedPriorArtifactId != retained.ArtifactId ||
		plan.Steps[3].GetCandidateRestorationProbe() == nil ||
		plan.Steps[4].GetCandidateRestorationCompensate() == nil {
		t.Fatal("inactive historical slot was not bound to complete restoration")
	}
	var found *agentpb.ComposeArtifact
	for _, artifact := range plan.Artifacts {
		if artifact.ArtifactId == retained.ArtifactId {
			found = artifact
		}
	}
	if !proto.Equal(found, retained) {
		t.Fatal("retained inactive runtime bytes changed")
	}
}
