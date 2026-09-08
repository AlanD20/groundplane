package executionplan

import (
	"crypto/sha256"
	"errors"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: the immutable plan owns one candidate Release procedure even
// when several Services are selected, and a first application must authorize
// exact candidate absence without fabricating a predecessor.
func TestBuildCandidateReleaseProcedureBlueprintFirstApplication(t *testing.T) {
	t.Parallel()

	procedure, err := BuildCandidateReleaseProcedure(CandidateReleaseProcedureInput{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
		Members: []CandidateReleaseMemberInput{{
			ServiceID:           "svc_01K4A1B2C3D4E5F6G7H8J9K0MN",
			CandidateReleaseID:  "dep_01K4A1B2C3D4E5F6G7H8J9K0MN",
			CandidateArtifactID: "cfg_01K4A1B2C3D4E5F6G7H8J9K0MN",
			ForwardStepIDs: []string{
				"step_01K4A1B2C3D4E5F6G7H8J9K0MN",
				"step_01K4A1B2C3D4E5F6G7H8J9K0MP",
			},
			ServingPredecessor: &ServingPredecessorInput{
				ProbeStepID: "step_01K4A1B2C3D4E5F6G7H8J9K0MQ", CompensateStepID: "step_01K4A1B2C3D4E5F6G7H8J9K0MR",
			},
			CandidateAbsence: &CandidateAbsenceInput{
				ComposeProjectName: "gp-env_01K4A1B2C3D4E5F6G7H8J9K0MN",
				ProbeStepID:        "step_01K4A1B2C3D4E5F6G7H8J9K0MQ", CompensateStepID: "step_01K4A1B2C3D4E5F6G7H8J9K0MR",
				Services: []CandidateServiceIdentity{{
					ServiceID: "svc_01K4A1B2C3D4E5F6G7H8J9K0MN",
					ReleaseID: "dep_01K4A1B2C3D4E5F6G7H8J9K0MN",
				}},
			},
		}},
	})
	if err != nil {
		t.Fatalf("build first-application candidate procedure: %v", err)
	}
	if len(procedure.GetMembers()) != 1 {
		t.Fatalf("candidate procedure members = %d, want one", len(procedure.GetMembers()))
	}
	member := procedure.GetMembers()[0]
	if member.GetCandidateAbsence() == nil || member.GetServingPredecessor() == nil {
		t.Fatal("Blueprint first application omitted a lawful restoration alternative")
	}
	if member.GetCandidateArtifactId() == "baseline" || member.GetCandidateReleaseId() == "baseline" {
		t.Fatal("first application fabricated baseline authority")
	}
}

// Rationale: a native predecessor is an explicit plan artifact, not the
// original source render. Its complete identity closure must survive the
// input-to-protobuf boundary without silently dropping retained history.
func TestBuildCandidateReleaseProcedureCarriesServingPredecessorReferences(t *testing.T) {
	t.Parallel()

	const (
		priorArtifactID    = "cfg_01K4A1B2C3D4E5F6G7H8J9K0MT"
		priorReleaseID     = "dep_01K4A1B2C3D4E5F6G7H8J9K0MS"
		retainedArtifactID = "cfg_01K4A1B2C3D4E5F6G7H8J9K0MV"
	)
	procedure, err := BuildCandidateReleaseProcedure(CandidateReleaseProcedureInput{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
		Members: []CandidateReleaseMemberInput{{
			ServiceID: nativeWitnessService, CandidateReleaseID: nativeWitnessCandidate,
			CandidateArtifactID: "cfg_01K4A1B2C3D4E5F6G7H8J9K0MN",
			ForwardStepIDs:      []string{"step_01K4A1B2C3D4E5F6G7H8J9K0MN"},
			ServingPredecessor: &ServingPredecessorInput{
				ProbeStepID: "step_01K4A1B2C3D4E5F6G7H8J9K0MP", CompensateStepID: "step_01K4A1B2C3D4E5F6G7H8J9K0MQ",
				PriorArtifactID: priorArtifactID, PriorReleaseID: priorReleaseID,
				PriorTarget: "blue", RetainedPriorArtifactID: retainedArtifactID,
			},
			CandidateAbsence: &CandidateAbsenceInput{
				ComposeProjectName: "gp-env_01K4A1B2C3D4E5F6G7H8J9K0MN",
				Services: []CandidateServiceIdentity{
					{ServiceID: nativeWitnessService, ReleaseID: nativeWitnessCandidate},
				},
				ProbeStepID: "step_01K4A1B2C3D4E5F6G7H8J9K0MP", CompensateStepID: "step_01K4A1B2C3D4E5F6G7H8J9K0MQ",
			},
		}},
	})
	if err != nil {
		t.Fatalf("build candidate procedure: %v", err)
	}
	serving := procedure.GetMembers()[0].GetServingPredecessor()
	if serving.GetPriorArtifactId() != priorArtifactID || serving.GetPriorReleaseId() != priorReleaseID ||
		serving.GetPriorTarget() != "blue" || serving.GetRetainedPriorArtifactId() != retainedArtifactID {
		t.Fatalf("serving predecessor references = %#v", serving)
	}
}

func TestValidateCandidateServingPredecessorReferencesRequiresExplicitPlanArtifacts(t *testing.T) {
	t.Parallel()

	const (
		candidateArtifactID = "cfg_01K4A1B2C3D4E5F6G7H8J9K0MN"
		priorArtifactID     = "cfg_01K4A1B2C3D4E5F6G7H8J9K0MP"
		retainedArtifactID  = "cfg_01K4A1B2C3D4E5F6G7H8J9K0MQ"
		priorReleaseID      = "dep_01K4A1B2C3D4E5F6G7H8J9K0MR"
	)
	artifacts := map[string]*agentpb.ComposeArtifact{
		candidateArtifactID: &agentpb.ComposeArtifact{}, priorArtifactID: &agentpb.ComposeArtifact{}, retainedArtifactID: &agentpb.ComposeArtifact{},
	}
	valid := &agentpb.ServingPredecessorRestoration{
		PriorArtifactId: priorArtifactID, PriorReleaseId: priorReleaseID,
		PriorTarget: "blue", RetainedPriorArtifactId: retainedArtifactID,
	}
	if err := validateCandidateServingPredecessorReferences(valid, nativeWitnessService, candidateArtifactID, artifacts); err != nil {
		t.Fatalf("valid fresh prior topology rejected: %v", err)
	}
	for name, mutate := range map[string]func(*agentpb.ServingPredecessorRestoration){
		"partial": func(value *agentpb.ServingPredecessorRestoration) { value.PriorReleaseId = "" },
		"foreign-prior": func(value *agentpb.ServingPredecessorRestoration) {
			value.PriorArtifactId = "cfg_01K4A1B2C3D4E5F6G7H8J9K0MS"
		},
		"same-candidate": func(value *agentpb.ServingPredecessorRestoration) { value.PriorArtifactId = candidateArtifactID },
		"same-retained":  func(value *agentpb.ServingPredecessorRestoration) { value.RetainedPriorArtifactId = priorArtifactID },
	} {
		t.Run(name, func(t *testing.T) {
			value := proto.Clone(valid).(*agentpb.ServingPredecessorRestoration)
			mutate(value)
			if err := validateCandidateServingPredecessorReferences(value, nativeWitnessService, candidateArtifactID, artifacts); err == nil {
				t.Fatal("accepted invalid explicit prior topology reference")
			}
		})
	}
}

// Rationale: full Blueprint validation must bind current C and inactive B as
// one native pair. The prior artifact is not the source render and the
// inactive artifact must retain its own Release and slot identity.
func TestValidateBlueprintPlanBindsCurrentAndRetainedNativePair(t *testing.T) {
	plan := validBlueprintScriptReconcilePlan(t)
	member := plan.CandidateReleaseProcedure.Members[0]
	currentID := "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	retainedID := "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	currentRelease := "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	retainedRelease := "dep_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	current := nativePlanArtifactForTest(t, plan.Artifacts[0], currentID, currentRelease, "green", 6)
	retained := nativePlanArtifactForTest(t, plan.Artifacts[0], retainedID, retainedRelease, "blue", 5)
	plan.Artifacts = append(plan.Artifacts, current, retained)
	member.ServingPredecessor = &agentpb.ServingPredecessorRestoration{
		ProbeStepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FB0", CompensateStepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FB1",
		PriorArtifactId: currentID, PriorReleaseId: currentRelease, PriorTarget: "green",
		RetainedPriorArtifactId: retainedID,
	}
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatalf("Seal(full native current/inactive Blueprint plan) error = %v", err)
	}
	if _, err := Validate(sealed); err != nil {
		t.Fatalf("Validate(full native current/inactive Blueprint plan) error = %v", err)
	}

	for name, mutate := range map[string]func(*agentpb.ExecutionPlan){
		"missing current reference": func(value *agentpb.ExecutionPlan) {
			value.CandidateReleaseProcedure.Members[0].ServingPredecessor.PriorArtifactId = ""
			value.CandidateReleaseProcedure.Members[0].ServingPredecessor.PriorReleaseId = ""
			value.CandidateReleaseProcedure.Members[0].ServingPredecessor.PriorTarget = ""
		},
		"wrong current release": func(value *agentpb.ExecutionPlan) {
			setNativePlanRelease(value.Artifacts[1].Services[0], retainedRelease)
		},
		"wrong current target": func(value *agentpb.ExecutionPlan) {
			setNativePlanSlot(value.Artifacts[1].Services[0], "blue")
		},
		"inactive uses current slot": func(value *agentpb.ExecutionPlan) {
			setNativePlanSlot(value.Artifacts[2].Services[0], "green")
		},
		"inactive uses current release": func(value *agentpb.ExecutionPlan) {
			setNativePlanRelease(value.Artifacts[2].Services[0], currentRelease)
		},
		"inactive has proxy": func(value *agentpb.ExecutionPlan) {
			value.Artifacts[2].Services = append(value.Artifacts[2].Services, &agentpb.ComposeService{
				ServiceId: member.ServiceId, ComposeName: "api-proxy",
				Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
				ExpectedLabels: []*agentpb.LabelPair{{Key: labelRuntimeRole, Value: "proxy"}},
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(sealed).(*agentpb.ExecutionPlan)
			mutate(changed)
			changed.PlanHash = nil
			encoded, err := marshalHashInput(changed)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(encoded)
			changed.PlanHash = digest[:]
			if _, err := Validate(changed); err == nil {
				t.Fatal("accepted invalid full-plan native current/inactive pair")
			}
		})
	}
}

func nativePlanArtifactForTest(
	t *testing.T,
	source *agentpb.ComposeArtifact,
	artifactID, releaseID, slot string,
	generation uint64,
) *agentpb.ComposeArtifact {
	t.Helper()
	artifact := proto.Clone(source).(*agentpb.ComposeArtifact)
	artifact.ArtifactId = artifactID
	service := proto.Clone(source.Services[0]).(*agentpb.ComposeService)
	service.Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT
	service.Slot = slot
	service.ComposeName = "api-" + slot
	service.ExpectedReplicas = 1
	service.HasHealthcheck = true
	service.ImageReference = "sha256:" + strings.Repeat("a", 64)
	service.OwnerComponentId = ""
	setNativePlanRelease(service, releaseID)
	setNativePlanSlot(service, slot)
	setNativePlanGeneration(service, generation)
	setNativePlanID(service, "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW")
	artifact.Services = []*agentpb.ComposeService{service}
	return artifact
}

func setNativePlanLabel(service *agentpb.ComposeService, key, value string) {
	for _, label := range service.ExpectedLabels {
		if label.GetKey() == key {
			label.Value = value
			return
		}
	}
	service.ExpectedLabels = append(service.ExpectedLabels, &agentpb.LabelPair{Key: key, Value: value})
	sort.Slice(service.ExpectedLabels, func(left, right int) bool {
		return service.ExpectedLabels[left].GetKey() < service.ExpectedLabels[right].GetKey()
	})
}

func setNativePlanRelease(service *agentpb.ComposeService, releaseID string) {
	setNativePlanLabel(service, labelReleaseID, releaseID)
	setNativePlanLabel(service, labelRuntimeRole, "slot")
}

func setNativePlanSlot(service *agentpb.ComposeService, slot string) {
	service.Slot = slot
	setNativePlanLabel(service, labelSlot, slot)
	setNativePlanLabel(service, labelRuntimeRole, "slot")
}

func setNativePlanGeneration(service *agentpb.ComposeService, generation uint64) {
	setNativePlanLabel(service, labelRenderGen, strconv.FormatUint(generation, 10))
}

func setNativePlanID(service *agentpb.ComposeService, planID string) {
	setNativePlanLabel(service, labelPlanID, planID)
}

// Rationale: mutation-bearing plans must not omit their one hash-covered
// candidate Release procedure.
func TestValidateCandidateReleasePlanRequiresProcedure(t *testing.T) {
	t.Parallel()

	plan := validPlan()
	plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
	plan.Steps[0].Payload = &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
		ArtifactId:    plan.Artifacts[0].ArtifactId,
		ServiceIds:    []string{"svc_01K4A1B2C3D4E5F6G7H8J9K0MN"},
		ForceRecreate: true, NoDependencies: true,
	}}
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal() accepted candidate mutation without a candidate Release procedure")
	}
}

// Rationale: candidate forward anchors are execution authority and must bind
// exactly one forward-policy step; absence, ambiguity, or another policy must
// make the sealed candidate plan unusable.
func TestValidateCandidateReleasePlanRequiresExactForwardAnchors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*agentpb.ExecutionPlan)
	}{
		{
			name: "missing",
			mutate: func(plan *agentpb.ExecutionPlan) {
				anchorID := plan.GetCandidateReleaseProcedure().GetMembers()[0].GetForwardStepIds()[1]
				for index, step := range plan.GetSteps() {
					if step.GetStepId() == anchorID {
						plan.Steps = append(plan.Steps[:index], plan.Steps[index+1:]...)
						return
					}
				}
			},
		},
		{
			name: "duplicate",
			mutate: func(plan *agentpb.ExecutionPlan) {
				anchorID := plan.GetCandidateReleaseProcedure().GetMembers()[0].GetForwardStepIds()[0]
				for _, step := range plan.GetSteps() {
					if step.GetStepId() == anchorID {
						plan.Steps = append(plan.Steps, proto.Clone(step).(*agentpb.ExecutionStep))
						return
					}
				}
			},
		},
		{
			name: "wrong policy",
			mutate: func(plan *agentpb.ExecutionPlan) {
				anchorID := plan.GetCandidateReleaseProcedure().GetMembers()[0].GetForwardStepIds()[0]
				for _, step := range plan.GetSteps() {
					if step.GetStepId() == anchorID {
						step.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_UNSPECIFIED
						return
					}
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			plan := validBlueprintScriptReconcilePlan(t)
			test.mutate(plan)
			if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Seal() error = %v, want validation.failed", err)
			}
		})
	}
}

// Rationale: recovery-only dispatch is limited to the exact probe and
// compensation steps sealed by each candidate member. Relabeled, ambiguous,
// or unreferenced restoration payloads must never acquire execution authority.
func TestValidateCandidateReleasePlanRequiresExactRestorationAnchors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*agentpb.ExecutionPlan)
	}{
		{name: "missing probe", mutate: func(plan *agentpb.ExecutionPlan) {
			probe, _ := candidateRestorationStepsForTest(t, plan)
			for index, step := range plan.GetSteps() {
				if step.GetStepId() == probe.GetStepId() {
					plan.Steps = append(plan.Steps[:index], plan.Steps[index+1:]...)
					return
				}
			}
		}},
		{name: "missing compensation", mutate: func(plan *agentpb.ExecutionPlan) {
			_, compensate := candidateRestorationStepsForTest(t, plan)
			for index, step := range plan.GetSteps() {
				if step.GetStepId() == compensate.GetStepId() {
					plan.Steps = append(plan.Steps[:index], plan.Steps[index+1:]...)
					return
				}
			}
		}},
		{name: "probe relabeled forward", mutate: func(plan *agentpb.ExecutionPlan) {
			probe, _ := candidateRestorationStepsForTest(t, plan)
			probe.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
		}},
		{name: "compensation relabeled forward", mutate: func(plan *agentpb.ExecutionPlan) {
			_, compensate := candidateRestorationStepsForTest(t, plan)
			compensate.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
		}},
		{name: "unreferenced probe", mutate: func(plan *agentpb.ExecutionPlan) {
			probe, _ := candidateRestorationStepsForTest(t, plan)
			unreferenced := proto.Clone(probe).(*agentpb.ExecutionStep)
			unreferenced.StepId = "step_01ARZ3NDEKTSV4RRFFQ69G5FB2"
			plan.Steps = append(plan.Steps, unreferenced)
		}},
		{name: "unreferenced compensation", mutate: func(plan *agentpb.ExecutionPlan) {
			_, compensate := candidateRestorationStepsForTest(t, plan)
			unreferenced := proto.Clone(compensate).(*agentpb.ExecutionStep)
			unreferenced.StepId = "step_01ARZ3NDEKTSV4RRFFQ69G5FB3"
			plan.Steps = append(plan.Steps, unreferenced)
		}},
		{name: "duplicate probe identity", mutate: func(plan *agentpb.ExecutionPlan) {
			probe, _ := candidateRestorationStepsForTest(t, plan)
			plan.Steps = append(plan.Steps, proto.Clone(probe).(*agentpb.ExecutionStep))
		}},
		{name: "duplicate compensation identity", mutate: func(plan *agentpb.ExecutionPlan) {
			_, compensate := candidateRestorationStepsForTest(t, plan)
			plan.Steps = append(plan.Steps, proto.Clone(compensate).(*agentpb.ExecutionStep))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := validBlueprintScriptReconcilePlan(t)
			test.mutate(plan)
			if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Seal() error = %v, want validation.failed", err)
			}
		})
	}
}

// Rationale: a restoration payload has no authority when the plan contains no
// candidate mutation and therefore cannot publish a candidate procedure that
// references it.
func TestValidateCandidateReleasePlanRejectsRestorationWithoutProcedure(t *testing.T) {
	t.Parallel()

	const releaseID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	plan := validPlan()
	labels := plan.Artifacts[0].Services[0].ExpectedLabels
	labels = append(labels, nil)
	copy(labels[4:], labels[3:])
	labels[3] = &agentpb.LabelPair{Key: labelReleaseID, Value: releaseID}
	plan.Artifacts[0].Services[0].ExpectedLabels = labels
	plan.Steps = append(plan.Steps, &agentpb.ExecutionStep{
		StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAW", TimeoutSeconds: 30,
		Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
		Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
			CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
				CandidateArtifactId: plan.Artifacts[0].ArtifactId,
				ServiceId:           plan.Artifacts[0].Services[0].ServiceId,
				CandidateReleaseId:  releaseID,
			},
		},
	})
	if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) ||
		!strings.Contains(err.Error(), "not referenced") {
		t.Fatalf("Seal() error = %v, want unreferenced restoration rejection", err)
	}
}

func candidateRestorationStepsForTest(
	t *testing.T,
	plan *agentpb.ExecutionPlan,
) (*agentpb.ExecutionStep, *agentpb.ExecutionStep) {
	t.Helper()
	var probe, compensate *agentpb.ExecutionStep
	for _, step := range plan.GetSteps() {
		if step.GetCandidateRestorationProbe() != nil {
			probe = step
		}
		if step.GetCandidateRestorationCompensate() != nil {
			compensate = step
		}
	}
	if probe == nil || compensate == nil {
		t.Fatal("candidate fixture omitted restoration steps")
	}
	return probe, compensate
}

// Rationale: post-hook and restoration steps carry their own non-forward
// policies and remain lawful when the candidate's apply and health anchors
// are each explicitly marked as forward execution.
func TestValidateCandidateReleasePlanAcceptsHookAndRecoveryPolicies(t *testing.T) {
	t.Parallel()

	plan := validBlueprintScriptReconcilePlan(t)
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	want := map[agentpb.ExecutionStepPolicy]bool{
		agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK:      true,
		agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE: true,
		agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE:     true,
	}
	for _, step := range sealed.GetSteps() {
		delete(want, step.GetPolicy())
	}
	if len(want) != 0 {
		t.Fatalf("sealed plan omitted preserved hook or recovery policies: %v", want)
	}
}

func TestCandidateReleaseDescriptorRejectsAnotherSealedPlan(t *testing.T) {
	t.Parallel()

	plan, err := Seal(validBlueprintScriptReconcilePlan(t))
	if err != nil {
		t.Fatalf("seal candidate plan: %v", err)
	}
	descriptor, err := DescribeCandidateRelease(plan)
	if err != nil {
		t.Fatalf("describe candidate release: %v", err)
	}

	other := proto.Clone(plan).(*agentpb.ExecutionPlan)
	other.Steps[0].TimeoutSeconds++
	other.PlanHash = nil
	other, err = Seal(other)
	if err != nil {
		t.Fatalf("seal other plan: %v", err)
	}

	if err := CandidateReleaseDescriptorMatchesPlan(descriptor, other); err == nil {
		t.Fatal("expected descriptor from another sealed plan to be rejected")
	}
}
