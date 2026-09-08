package executionplan

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	testScriptID          = "scr_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testScriptExecutionID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testScriptSnapshotID  = "01ARZ3NDEKTSV4RRFFQ69G5FAW"
)

func TestSealManualScriptPlanBindsEveryArtifact(t *testing.T) {
	plan := validManualScriptPlan(t)
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	if len(sealed.PlanHash) != sha256.Size {
		t.Fatalf("PlanHash length = %d", len(sealed.PlanHash))
	}
	if !bytes.Equal(
		sealed.Steps[0].GetRunScript().BodySha256,
		sealed.ScriptBodyArtifacts[0].Sha256,
	) {
		t.Fatal("RunScript body digest is not bound to body metadata")
	}
}

func TestSealManualScriptPlanRejectsCrossExecutionArtifact(t *testing.T) {
	plan := validManualScriptPlan(t)
	plan.ScriptBodyArtifacts[0].ScriptExecutionId = "01ARZ3NDEKTSV4RRFFQ69G5FAX"
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(cross-execution body) error = nil")
	}
}

func TestSealManualScriptPlanRejectsSnapshotTampering(t *testing.T) {
	plan := validManualScriptPlan(t)
	plan.ScriptRunnerSnapshots[0].ServiceSource.GetExisting().ModRevision++
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(tampered snapshot) error = nil")
	}
}

func TestValidateResolvedRunnerSnapshotAcceptsExistingAndStagedSourceAuthorities(t *testing.T) {
	for _, test := range []struct {
		name string
		plan func(*testing.T) *agentpb.ExecutionPlan
	}{
		{name: "existing", plan: validManualScriptPlan},
		{name: "staged", plan: validStagedScriptPlan},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateResolvedRunnerSnapshot(test.plan(t).ScriptRunnerSnapshots[0]); err != nil {
				t.Fatalf("validateResolvedRunnerSnapshot() error = %v", err)
			}
		})
	}
}

func TestSealManualScriptPlanRejectsStagedSourceAuthorityAnywhere(t *testing.T) {
	plan := validManualScriptPlan(t)
	snapshot := plan.ScriptRunnerSnapshots[0]
	snapshot.ServiceSource = stagedScriptSourceAuthorityForTest(snapshot, 1)
	if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Seal(staged manual source) error = %v", err)
	}

	snapshot = validManualScriptPlan(t).ScriptRunnerSnapshots[0]
	snapshot.Networks = []*agentpb.ScriptRunnerNetwork{{Source: existingScriptSourceAuthorityForTest(10)}}
	snapshot.Mounts = []*agentpb.ScriptRunnerMount{{Source: existingScriptSourceAuthorityForTest(11)}}
	locations := []struct {
		name string
		get  func(*agentpb.ResolvedRunnerSnapshot) **agentpb.ScriptSourceAuthority
	}{
		{name: "service", get: func(value *agentpb.ResolvedRunnerSnapshot) **agentpb.ScriptSourceAuthority {
			return &value.ServiceSource
		}},
		{name: "topology", get: func(value *agentpb.ResolvedRunnerSnapshot) **agentpb.ScriptSourceAuthority {
			return &value.NetworkTopologySource
		}},
		{name: "applied Environment", get: func(value *agentpb.ResolvedRunnerSnapshot) **agentpb.ScriptSourceAuthority {
			return &value.AppliedEnvironmentSource
		}},
		{name: "network", get: func(value *agentpb.ResolvedRunnerSnapshot) **agentpb.ScriptSourceAuthority {
			return &value.Networks[0].Source
		}},
		{name: "mount", get: func(value *agentpb.ResolvedRunnerSnapshot) **agentpb.ScriptSourceAuthority {
			return &value.Mounts[0].Source
		}},
	}
	for index, location := range locations {
		t.Run(location.name, func(t *testing.T) {
			candidate := proto.Clone(snapshot).(*agentpb.ResolvedRunnerSnapshot)
			*location.get(candidate) = stagedScriptSourceAuthorityForTest(candidate, byte(index+2))
			if err := validateManualScriptSourceAuthorities(candidate); !errors.Is(
				err,
				errs.New(errs.KindValidationFailed, ""),
			) {
				t.Fatalf("validateManualScriptSourceAuthorities(staged %s) error = %v", location.name, err)
			}
		})
	}
}

func TestSealManualScriptPlanRejectsInvalidSourceAuthority(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*agentpb.ScriptSourceAuthority)
	}{
		{name: "empty authority", mutate: func(authority *agentpb.ScriptSourceAuthority) { authority.Staged = nil }},
		{name: "dual authority", mutate: func(authority *agentpb.ScriptSourceAuthority) {
			authority.Existing = &agentpb.ScriptExistingSourceAuthority{ModRevision: 9}
		}},
		{name: "invalid stage identity", mutate: func(authority *agentpb.ScriptSourceAuthority) { authority.GetStaged().RevisionId = "task-invalid" }},
		{name: "zero fixed read revision", mutate: func(authority *agentpb.ScriptSourceAuthority) { authority.GetStaged().FixedReadRevision = 0 }},
		{name: "invalid canonical digest", mutate: func(authority *agentpb.ScriptSourceAuthority) {
			authority.GetStaged().CanonicalValueSha256 = []byte("short")
		}},
		{name: "conflicting fixed read revision", mutate: func(authority *agentpb.ScriptSourceAuthority) { authority.GetStaged().FixedReadRevision++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := validStagedScriptPlan(t)
			test.mutate(plan.ScriptRunnerSnapshots[0].ServiceSource)
			if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Seal() error = %v, want validation.failed", err)
			}
		})
	}
}

func TestValidateScriptSourceAuthorityRejectsRawDualFieldEncoding(t *testing.T) {
	existing, err := proto.Marshal(existingScriptSourceAuthorityForTest(7))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := validManualScriptPlan(t).ScriptRunnerSnapshots[0]
	staged, err := proto.Marshal(stagedScriptSourceAuthorityForTest(snapshot, 0x41))
	if err != nil {
		t.Fatal(err)
	}
	var authority agentpb.ScriptSourceAuthority
	if err = proto.Unmarshal(append(existing, staged...), &authority); err != nil {
		t.Fatal(err)
	}
	if authority.Existing == nil || authority.Staged == nil {
		t.Fatalf("raw dual authority was not preserved: %#v", &authority)
	}
	var claim *agentpb.ScriptStagedSourceAuthority
	if err = validateScriptSourceAuthority(&authority, snapshot.EnvironmentId,
		snapshot.AppliedEnvironmentRevisionId, snapshot.AppliedEnvironmentRenderGeneration, &claim); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("validateScriptSourceAuthority(raw dual) error = %v", err)
	}
}

func TestValidateRejectsStagedSourceDigestTampering(t *testing.T) {
	plan := validBlueprintScriptReconcilePlan(t)
	snapshot := plan.ScriptRunnerSnapshots[0]
	snapshot.ServiceSource = stagedScriptSourceAuthorityForTest(snapshot, 0x11)
	snapshot.NetworkTopologySource = stagedScriptSourceAuthorityForTest(snapshot, 0x12)
	snapshot.AppliedEnvironmentSource = stagedScriptSourceAuthorityForTest(snapshot, 0x13)
	for index, network := range snapshot.Networks {
		network.Source = stagedScriptSourceAuthorityForTest(snapshot, byte(0x20+index))
	}
	for index, mount := range snapshot.Mounts {
		mount.Source = stagedScriptSourceAuthorityForTest(snapshot, byte(0x30+index))
	}
	snapshotDigest, digestErr := scriptMessageDigest(snapshot)
	if digestErr != nil {
		t.Fatal(digestErr)
	}
	for _, step := range plan.Steps {
		if run := step.GetRunScript(); run != nil {
			run.RunnerSnapshotSha256 = snapshotDigest
		}
	}
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatal(err)
	}
	before := append([]byte(nil), sealed.PlanHash...)
	sealed.ScriptRunnerSnapshots[0].ServiceSource.GetStaged().CanonicalValueSha256[0] ^= 0xff
	if _, err = Validate(sealed); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Validate(tampered stage digest) error = %v", err)
	}
	updatedSnapshotDigest, digestErr := scriptMessageDigest(sealed.ScriptRunnerSnapshots[0])
	if digestErr != nil {
		t.Fatal(digestErr)
	}
	for _, step := range sealed.Steps {
		if run := step.GetRunScript(); run != nil {
			run.RunnerSnapshotSha256 = updatedSnapshotDigest
		}
	}
	sealed.PlanHash = nil
	resealed, err := Seal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(before, resealed.PlanHash) {
		t.Fatal("staged source digest tampering did not change the plan hash")
	}
}

func TestSealManualScriptPlanRejectsOwnershipLabelOutsideClosedSet(t *testing.T) {
	plan := validManualScriptPlan(t)
	plan.ScriptRunnerProjections[0].Labels = append(
		plan.ScriptRunnerProjections[0].Labels,
		&agentpb.ScriptStringPair{Key: "com.groundplane.task-id", Value: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
	)
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(extra ownership label) error = nil")
	}
}

// Rationale: one immutable Environment Blueprint candidate may run only its
// post-deploy Script after the candidate procedure and against the candidate
// Release already bound into the exact Compose artifact.
func TestSealAcceptsBlueprintCandidatePostDeployScript(t *testing.T) {
	sealed, err := Seal(validBlueprintScriptReconcilePlan(t))
	if err != nil {
		t.Fatalf("Seal(Blueprint candidate Script) error = %v", err)
	}
	if _, err := Validate(sealed); err != nil {
		t.Fatalf("Validate(Blueprint candidate Script) error = %v", err)
	}
}

// Rationale: candidate and historical Script runs use a presealed local config
// ID, never a mutable tag or a registry manifest digest.
func TestSealScriptUsesPresealedLocalImage(t *testing.T) {
	for _, build := range []struct {
		name string
		plan func(*testing.T) *agentpb.ExecutionPlan
	}{{"historical", validManualScriptPlan}, {"candidate", validBlueprintScriptReconcilePlan}} {
		t.Run(build.name, func(t *testing.T) {
			plan := build.plan(t)
			sealed, err := Seal(plan)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Validate(sealed); err != nil {
				t.Fatal(err)
			}
			if sealed.ScriptRunnerSnapshots[0].LocalImageId != "sha256:"+strings.Repeat("a", 64) ||
				sealed.ScriptRunnerProjections[0].Image != sealed.ScriptRunnerSnapshots[0].LocalImageId {
				t.Fatal("sealed Script changed its local image identity")
			}
		})
	}
}

// Rationale: rehashing a forged candidate snapshot must not grant execution
// against another image, Release, Service, or candidate application.
func TestSealRejectsBlueprintSealedImageAuthorityMismatch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*agentpb.ExecutionPlan)
	}{
		{"wrong local image", func(plan *agentpb.ExecutionPlan) {
			plan.ScriptRunnerSnapshots[0].LocalImageId = "sha256:" + strings.Repeat("b", 64)
		}},
		{"mutable reference", func(plan *agentpb.ExecutionPlan) {
			plan.ScriptRunnerSnapshots[0].LocalImageId = "registry.example/app:candidate"
		}},
		{"registry manifest", func(plan *agentpb.ExecutionPlan) {
			plan.ScriptRunnerSnapshots[0].LocalImageId = "registry.example/app@sha256:" + strings.Repeat("a", 64)
		}},
		{"wrong service", func(plan *agentpb.ExecutionPlan) {
			plan.ScriptRunnerSnapshots[0].ServiceId = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAZ"
		}},
		{"wrong Release", func(plan *agentpb.ExecutionPlan) {
			plan.ScriptRunnerSnapshots[0].ReleaseId = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAZ"
		}},
		{"missing apply prerequisite", func(plan *agentpb.ExecutionPlan) {
			plan.Steps[1].PrerequisiteStepId = ""
		}},
		{"forward prerequisite", func(plan *agentpb.ExecutionPlan) {
			plan.Steps[1].PrerequisiteStepId = plan.Steps[2].StepId
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := validBlueprintScriptReconcilePlan(t)
			if _, err := Seal(plan); err != nil {
				t.Fatalf("invalid baseline: %v", err)
			}
			test.mutate(plan)
			refreshBlueprintSnapshotDigestForTest(t, plan)
			if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Seal(forged candidate authority) = %v", err)
			}
		})
	}
}

func TestSealRejectsNonBlueprintAndUnboundReconcileScripts(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*agentpb.ExecutionPlan)
	}{
		{name: "ordinary reconcile operation", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
		}},
		{name: "unbound candidate Release", mutate: func(plan *agentpb.ExecutionPlan) {
			for _, label := range plan.Artifacts[0].Services[0].ExpectedLabels {
				if label.Key == labelReleaseID {
					label.Value = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
				}
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := validBlueprintScriptReconcilePlan(t)
			test.mutate(plan)
			if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Seal(invalid reconcile Script) error = %v, want validation.failed", err)
			}
		})
	}
}

// Rationale: an unrelated artifact must not authorize a Script when the exact
// predecessor ComposeApply selects a different artifact without the candidate.
func TestSealRejectsBlueprintScriptAuthorizedByDecoyArtifact(t *testing.T) {
	plan := validBlueprintScriptReconcilePlan(t)
	decoy := proto.Clone(plan.Artifacts[0]).(*agentpb.ComposeArtifact)
	decoy.ArtifactId = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	decoy.Services = nil
	plan.Artifacts = append(plan.Artifacts, decoy)
	plan.Steps[0].GetComposeApply().ArtifactId = decoy.ArtifactId

	if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Seal(decoy Blueprint artifact) error = %v, want validation.failed", err)
	}
}

// Rationale: a correctly rehashed forged ordinary reconcile plan must fail
// semantic validation, not merely fail because its digest is stale.
func TestValidateRejectsForgedOrdinaryReconcileScript(t *testing.T) {
	sealed, err := Seal(validBlueprintScriptReconcilePlan(t))
	if err != nil {
		t.Fatalf("Seal(Blueprint candidate Script) error = %v", err)
	}
	sealed.Operation = agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	sealed.PlanHash = nil
	encoded, err := marshalHashInput(sealed)
	if err != nil {
		t.Fatalf("marshal forged reconcile hash input: %v", err)
	}
	digest := sha256.Sum256(encoded)
	sealed.PlanHash = digest[:]
	if _, err := Validate(sealed); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Validate(forged ordinary reconcile Script) error = %v, want validation.failed", err)
	}
}

func validBlueprintScriptReconcilePlan(t *testing.T) *agentpb.ExecutionPlan {
	t.Helper()
	plan := validManualScriptPlan(t)
	run := plan.Steps[0].GetRunScript()
	snapshot := plan.ScriptRunnerSnapshots[0]
	applyStepID := "step_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	healthStepID := "step_01ARZ3NDEKTSV4RRFFQ69G5FAZ"
	probeStepID := "step_01ARZ3NDEKTSV4RRFFQ69G5FB0"
	compensateStepID := "step_01ARZ3NDEKTSV4RRFFQ69G5FB1"
	yaml := []byte("services:\n  api:\n    image: " + snapshot.LocalImageId + "\n")
	yamlDigest := sha256.Sum256(yaml)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId:  "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		OwnerKind:   agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:     run.EnvironmentId,
		ProjectName: "gp-" + strings.ToLower(run.EnvironmentId),
		AuthorizedVolumeDir: "/var/lib/groundplane/volumes/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
			"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + run.EnvironmentId,
		CanonicalYaml: yaml,
		YamlSha256:    yamlDigest[:],
		Services: []*agentpb.ComposeService{{
			ServiceId: run.ServiceId, ComposeName: "api", ExpectedReplicas: 1,
			ImageReference: snapshot.LocalImageId,
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: labelEnvironmentID, Value: run.EnvironmentId},
				{Key: labelKind, Value: "service"},
				{Key: labelManaged, Value: "true"},
				{Key: labelPlanID, Value: plan.PlanId},
				{Key: labelReleaseID, Value: run.ReleaseId},
				{Key: labelRenderGen, Value: "7"},
				{Key: labelServiceID, Value: run.ServiceId},
			},
		}},
	}
	plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
	plan.TargetId = run.EnvironmentId
	plan.Artifacts = []*agentpb.ComposeArtifact{artifact}
	plan.Steps[0].Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK
	plan.Steps[0].PrerequisiteStepId = applyStepID
	plan.Steps = append([]*agentpb.ExecutionStep{{
		StepId: applyStepID, TimeoutSeconds: 30,
		Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
		Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId: artifact.ArtifactId, ServiceIds: []string{run.ServiceId},
			ForceRecreate: true, NoDependencies: true,
		}},
	}}, plan.Steps...)
	plan.Steps = append(plan.Steps, &agentpb.ExecutionStep{
		StepId: healthStepID, TimeoutSeconds: 30, PrerequisiteStepId: plan.Steps[1].GetStepId(),
		Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
		Payload: &agentpb.ExecutionStep_WaitHealthy{WaitHealthy: &agentpb.WaitHealthy{
			ArtifactId: artifact.ArtifactId, ServiceIds: []string{run.ServiceId},
		}},
	})
	plan.Steps = append(plan.Steps,
		&agentpb.ExecutionStep{
			StepId: probeStepID, TimeoutSeconds: 30,
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
			Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
				CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
					CandidateArtifactId: artifact.ArtifactId, ServiceId: run.ServiceId, CandidateReleaseId: run.ReleaseId,
				},
			},
		},
		&agentpb.ExecutionStep{
			StepId: compensateStepID, TimeoutSeconds: 30, PrerequisiteStepId: applyStepID,
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
			Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
				CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
					CandidateArtifactId: artifact.ArtifactId, ServiceId: run.ServiceId, CandidateReleaseId: run.ReleaseId,
				},
			},
		},
	)
	plan.CandidateReleaseProcedure = &agentpb.CandidateReleaseProcedure{Members: []*agentpb.CandidateReleaseMember{{
		ServiceId: run.ServiceId, CandidateReleaseId: run.ReleaseId, CandidateArtifactId: artifact.ArtifactId,
		ForwardStepIds: []string{applyStepID, healthStepID},
		ServingPredecessor: &agentpb.ServingPredecessorRestoration{
			ProbeStepId:      probeStepID,
			CompensateStepId: compensateStepID,
		},
		CandidateAbsence: &agentpb.CandidateAbsenceRestoration{
			ComposeProjectName: artifact.ProjectName, ProbeStepId: probeStepID, CompensateStepId: compensateStepID,
			Services: []*agentpb.CandidateReleaseService{{ServiceId: run.ServiceId, ReleaseId: run.ReleaseId}},
		},
	}}}
	refreshBlueprintSnapshotDigestForTest(t, plan)
	return plan
}

func refreshBlueprintSnapshotDigestForTest(t *testing.T, plan *agentpb.ExecutionPlan) {
	t.Helper()
	digest, err := scriptMessageDigest(plan.ScriptRunnerSnapshots[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range plan.Steps {
		if run := step.GetRunScript(); run != nil {
			run.RunnerSnapshotSha256 = digest
		}
	}
}

func TestSealBlueprintCandidatePostDeployScriptBeforeReadiness(t *testing.T) {
	t.Parallel()
	plan := validBlueprintScriptReconcilePlan(t)
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatalf("Seal(Blueprint post-deploy chain) error = %v", err)
	}
	if sealed.Operation != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY ||
		len(sealed.Steps) != 5 || sealed.Steps[0].GetComposeApply() == nil ||
		sealed.Steps[1].GetRunScript() == nil || sealed.Steps[2].GetWaitHealthy() == nil ||
		sealed.Steps[1].PrerequisiteStepId != sealed.Steps[0].StepId ||
		sealed.Steps[2].PrerequisiteStepId != sealed.Steps[1].StepId {
		t.Fatalf("sealed Blueprint post-deploy chain = %#v", sealed.Steps)
	}
}

func validManualScriptPlan(t *testing.T) *agentpb.ExecutionPlan {
	t.Helper()
	bodyDigest := sha256.Sum256([]byte("php artisan migrate\n"))
	serviceDigest := sha256.Sum256([]byte("service definition"))
	projection := &agentpb.ScriptRunnerProjection{
		SnapshotId: testScriptSnapshotID,
		Name:       "gp-script-" + strings.ToLower(testScriptExecutionID),
		Image:      "sha256:" + strings.Repeat("a", 64),
		Labels: []*agentpb.ScriptStringPair{
			{Key: "com.groundplane.environment-id", Value: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			{Key: "com.groundplane.kind", Value: "script-runner"},
			{Key: "com.groundplane.managed", Value: "true"},
			{Key: "com.groundplane.operation-id", Value: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			{Key: "com.groundplane.plan-id", Value: testPlanID},
			{Key: "com.groundplane.project-id", Value: "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			{Key: "com.groundplane.release-id", Value: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			{Key: "com.groundplane.render-generation", Value: "7"},
			{Key: "com.groundplane.script-execution-id", Value: testScriptExecutionID},
			{Key: "com.groundplane.script-generation", Value: "1"},
			{Key: "com.groundplane.script-id", Value: testScriptID},
			{Key: "com.groundplane.service-id", Value: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			{Key: "com.groundplane.tenant-id", Value: "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		},
		Entrypoint:       []string{"/bin/sh"},
		Command:          []string{"/groundplane-script-body"},
		StopGraceSeconds: 10,
	}
	projectionDigest, err := scriptMessageDigest(projection)
	if err != nil {
		t.Fatalf("projection digest: %v", err)
	}
	snapshot := &agentpb.ResolvedRunnerSnapshot{
		SnapshotId: testScriptSnapshotID, ScriptExecutionId: testScriptExecutionID,
		TenantId: "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV", TenantModRevision: 1,
		ProjectId: "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV", ProjectModRevision: 2,
		EnvironmentId: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", EnvironmentModRevision: 3,
		ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceSource: existingScriptSourceAuthorityForTest(4),
		ServiceDefinitionSha256: serviceDigest[:],
		ReleaseId:               "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", ReleaseModRevision: 5,
		LocalImageId:              "sha256:" + strings.Repeat("a", 64),
		BlueprintBundleGeneration: "task_01ARZ3NDEKTSV4RRFFQ69G5FAX",
		RenderGeneration:          7, NetworkTopologySource: existingScriptSourceAuthorityForTest(6),
		AppliedEnvironmentRevisionId:       "task_01ARZ3NDEKTSV4RRFFQ69G5FAY",
		AppliedEnvironmentRenderGeneration: 8,
		AppliedEnvironmentSource:           existingScriptSourceAuthorityForTest(9),
		RunnerProjectionSha256:             projectionDigest,
	}
	snapshotDigest, err := scriptMessageDigest(snapshot)
	if err != nil {
		t.Fatalf("snapshot digest: %v", err)
	}
	return &agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: testPlanID, RenderGeneration: 7,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_SCRIPT, TargetId: testScriptID,
		ScriptRunnerSnapshots:   []*agentpb.ResolvedRunnerSnapshot{snapshot},
		ScriptRunnerProjections: []*agentpb.ScriptRunnerProjection{projection},
		ScriptBodyArtifacts: []*agentpb.ScriptBodyArtifactMetadata{{
			ScriptExecutionId: testScriptExecutionID, ScriptId: testScriptID,
			Generation: 1, Size: uint32(len("php artisan migrate\n")), Sha256: bodyDigest[:],
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: testStepID, TimeoutSeconds: ScriptExecutionTimeoutSeconds,
			Payload: &agentpb.ExecutionStep_RunScript{RunScript: &agentpb.RunScript{
				ScriptExecutionId: testScriptExecutionID, ScriptId: testScriptID, ScriptGeneration: 1,
				EnvironmentId: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				ServiceId:     "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				ReleaseId:     "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", RenderGeneration: 7,
				ServiceDefinitionSha256: serviceDigest[:], BodySha256: bodyDigest[:],
				RunnerSnapshotId: testScriptSnapshotID, RunnerSnapshotSha256: snapshotDigest,
			}},
		}},
	}
}

func existingScriptSourceAuthorityForTest(revision uint64) *agentpb.ScriptSourceAuthority {
	return &agentpb.ScriptSourceAuthority{
		Existing: &agentpb.ScriptExistingSourceAuthority{ModRevision: revision},
	}
}

func validStagedScriptPlan(t *testing.T) *agentpb.ExecutionPlan {
	t.Helper()
	plan := validBlueprintScriptReconcilePlan(t)
	snapshot := plan.ScriptRunnerSnapshots[0]
	snapshot.ServiceSource = stagedScriptSourceAuthorityForTest(snapshot, 0x11)
	snapshot.NetworkTopologySource = stagedScriptSourceAuthorityForTest(snapshot, 0x12)
	snapshot.AppliedEnvironmentSource = stagedScriptSourceAuthorityForTest(snapshot, 0x13)
	for index, network := range snapshot.Networks {
		network.Source = stagedScriptSourceAuthorityForTest(snapshot, byte(0x20+index))
	}
	for index, mount := range snapshot.Mounts {
		mount.Source = stagedScriptSourceAuthorityForTest(snapshot, byte(0x30+index))
	}
	snapshotDigest, err := scriptMessageDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range plan.Steps {
		if run := step.GetRunScript(); run != nil {
			run.RunnerSnapshotSha256 = snapshotDigest
		}
	}
	return plan
}

func stagedScriptSourceAuthorityForTest(
	snapshot *agentpb.ResolvedRunnerSnapshot,
	seed byte,
) *agentpb.ScriptSourceAuthority {
	return &agentpb.ScriptSourceAuthority{Staged: &agentpb.ScriptStagedSourceAuthority{
		EnvironmentId: snapshot.EnvironmentId, RevisionId: snapshot.AppliedEnvironmentRevisionId,
		RenderGeneration: snapshot.AppliedEnvironmentRenderGeneration, FixedReadRevision: 42,
		CanonicalValueSha256: bytes.Repeat([]byte{seed}, sha256.Size),
	}}
}
