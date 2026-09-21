package agent

import (
	bytes "bytes"
	sha256 "crypto/sha256"
	testing "testing"

	testcomposeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	proto "google.golang.org/protobuf/proto"
)

// Rationale: a completed compensation helper response closes reconciliation
// only when its typed evidence proves the exact sealed proxy restoration.
func TestReleaseRestorationEvidenceProvenForProxy(t *testing.T) {
	step := exactProxyCompensationStep()
	exact := &agentpb.ServiceProxyEvidence{
		ServiceId: "api", Target: "green", ProxyGeneration: 7,
		ConfigSha256: bytes.Repeat([]byte{0x31}, 32), ReleaseId: "prior-api", Compensated: true,
	}
	tests := []struct {
		name   string
		result testcomposeruntime.StepResult
		want   bool
	}{
		{name: "missing proof"},
		{
			name: "wrong proof kind",
			result: testcomposeruntime.StepResult{RecreateEvidence: &agentpb.ServiceRecreateEvidence{
				ServiceId: "api", ReleaseId: "prior-api", ArtifactId: "prior-artifact", Target: "green", Compensated: true,
			}},
		},
		{name: "mismatched proof", result: testcomposeruntime.StepResult{ProxyEvidence: &agentpb.ServiceProxyEvidence{
			ServiceId: "api", Target: "blue", ProxyGeneration: 7,
			ConfigSha256: bytes.Repeat([]byte{0x31}, 32), ReleaseId: "prior-api", Compensated: true,
		}}},
		{name: "exact proof", result: testcomposeruntime.StepResult{ProxyEvidence: exact}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := testcomposeruntime.ReleaseRestorationEvidenceProven(testtaskassignment.Assignment{}, step, test.result); got != test.want {
				t.Fatalf("testcomposeruntime.ReleaseRestorationEvidenceProven() = %t, want %t", got, test.want)
			}
		})
	}
}

// Rationale: singleton restoration is exact only when the helper proves the
// sealed predecessor artifact, Release, target, and Service as compensated.
func TestReleaseRestorationEvidenceProvenForRecreate(t *testing.T) {
	step := exactRecreateCompensationStep()
	exact := &agentpb.ServiceRecreateEvidence{
		ServiceId: "worker", ReleaseId: "prior-worker", ArtifactId: "prior-artifact",
		Target: "singleton", Compensated: true,
	}
	tests := []struct {
		name   string
		result testcomposeruntime.StepResult
		want   bool
	}{
		{name: "missing proof"},
		{name: "wrong proof kind", result: testcomposeruntime.StepResult{ProxyEvidence: &agentpb.ServiceProxyEvidence{
			ServiceId: "worker", Target: "green", ProxyGeneration: 1,
			ConfigSha256: bytes.Repeat([]byte{0x41}, 32), ReleaseId: "prior-worker", Compensated: true,
		}}},
		{
			name: "mismatched proof",
			result: testcomposeruntime.StepResult{RecreateEvidence: &agentpb.ServiceRecreateEvidence{
				ServiceId: "worker", ReleaseId: "prior-worker", ArtifactId: "candidate-artifact",
				Target: "singleton", Compensated: true,
			}},
		},
		{name: "exact proof", result: testcomposeruntime.StepResult{RecreateEvidence: exact}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := testcomposeruntime.ReleaseRestorationEvidenceProven(testtaskassignment.Assignment{}, step, test.result); got != test.want {
				t.Fatalf("testcomposeruntime.ReleaseRestorationEvidenceProven() = %t, want %t", got, test.want)
			}
		})
	}
}

// Rationale: first-deploy compensation restores sealed absence, so proof must
// bind the assignment, plan, restoration authority, artifact, candidate set,
// project, and positive absence result.
func TestReleaseRestorationEvidenceProvenForCandidateAbsence(t *testing.T) {
	assignment, step, exact := exactCandidateAbsenceCompensation()
	tests := []struct {
		name   string
		result testcomposeruntime.StepResult
		want   bool
	}{
		{name: "missing proof"},
		{
			name: "wrong proof kind",
			result: testcomposeruntime.StepResult{RecreateEvidence: &agentpb.ServiceRecreateEvidence{
				ServiceId: "api", ReleaseId: "release-api", ArtifactId: "candidate-artifact",
				Target: "singleton", Compensated: true,
			}},
		},
		{
			name: "mismatched proof",
			result: testcomposeruntime.StepResult{CandidateAbsenceEvidence: &agentpb.CandidateAbsenceEvidence{
				AssignmentId: exact.GetAssignmentId(), PlanHash: append([]byte(nil), exact.GetPlanHash()...),
				AuthoritySha256: append(
					[]byte(nil),
					exact.GetAuthoritySha256()...), ComposeProjectName: "wrong-project",
				CandidateArtifactId: exact.GetCandidateArtifactId(), Candidates: exact.GetCandidates(), AbsenceProven: true,
			}},
		},
		{name: "exact proof", result: testcomposeruntime.StepResult{CandidateAbsenceEvidence: exact}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := testcomposeruntime.ReleaseRestorationEvidenceProven(assignment, step, test.result); got != test.want {
				t.Fatalf("testcomposeruntime.ReleaseRestorationEvidenceProven() = %t, want %t", got, test.want)
			}
		})
	}
}

// Rationale: a helper response proves only its current member; another member
// or an aggregate response cannot stand in for that exact proof.
func TestCandidateAbsenceEvidenceUsesExactMember(t *testing.T) {
	assignment, step, exact := exactCandidateAbsenceCompensation()
	other := proto.CloneOf(exact)
	other.Candidates[0] = &agentpb.CandidateReleaseService{ServiceId: "worker", ReleaseId: "release-worker"}
	duplicate := proto.Clone(exact).(*agentpb.CandidateAbsenceEvidence)
	duplicate.Candidates = append(duplicate.Candidates, proto.CloneOf(duplicate.Candidates[0]))
	missing := proto.Clone(exact).(*agentpb.CandidateAbsenceEvidence)
	missing.Candidates = nil
	extra := proto.Clone(exact).(*agentpb.CandidateAbsenceEvidence)
	extra.Candidates = append(
		extra.Candidates,
		&agentpb.CandidateReleaseService{ServiceId: "web", ReleaseId: "release-web"},
	)
	if !testcomposeruntime.ReleaseRestorationEvidenceProven(
		assignment,
		step,
		testcomposeruntime.StepResult{CandidateAbsenceEvidence: exact},
	) {
		t.Fatal("exact member proof rejected")
	}
	if testcomposeruntime.ReleaseRestorationEvidenceProven(
		assignment,
		step,
		testcomposeruntime.StepResult{CandidateAbsenceEvidence: duplicate},
	) {
		t.Fatal("canonical candidate membership accepted a duplicate")
	}
	for name, evidence := range map[string]*agentpb.CandidateAbsenceEvidence{"missing": missing, "extra": extra, "other": other} {
		if testcomposeruntime.ReleaseRestorationEvidenceProven(
			assignment,
			step,
			testcomposeruntime.StepResult{CandidateAbsenceEvidence: evidence},
		) {
			t.Fatalf("canonical candidate membership accepted %s membership", name)
		}
	}
}

// Rationale: serving evidence must match the selected native predecessor's
// exact recreate or proxy shape without consulting the applied witness.
func TestCandidateServingPredecessorAcceptsExactTypedRestorationVariant(t *testing.T) {
	assignment, artifact, _ := nativeServingAssignment(t)
	if err := testtaskassignment.ValidateCandidateReleaseAuthority(assignment, assignment.Plan); err != nil {
		t.Fatalf("valid native recreate authority: %v", err)
	}
	member := assignment.Plan.CandidateReleaseProcedure.Members[0]
	step := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
		CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
			ServiceId: member.ServiceId, CandidateReleaseId: member.CandidateReleaseId,
			CandidateArtifactId: member.CandidateArtifactId,
		},
	}}
	exact := testcomposeruntime.StepResult{RecreateEvidence: &agentpb.ServiceRecreateEvidence{
		ServiceId: member.ServiceId, ArtifactId: artifact.ArtifactId,
		ReleaseId: nativeAssignmentPriorRelease, Target: "blue", Compensated: true,
	}}
	if !testcomposeruntime.ReleaseRestorationEvidenceProven(assignment, step, exact) {
		t.Fatal("exact serving predecessor recreate evidence was rejected")
	}
	exact.RecreateEvidence.ArtifactId = "wrong-artifact"
	if testcomposeruntime.ReleaseRestorationEvidenceProven(assignment, step, exact) {
		t.Fatal("mismatched serving predecessor recreate evidence was accepted")
	}
	proxyAssignment, proxyArtifact, _ := nativeServingAssignment(t)
	appliedBefore := proto.CloneOf(proxyAssignment.RestorationAuthority.AppliedPredecessor)
	config := []byte(`{"apps":{"http":{"servers":{"gp_g7_dep_01arz3ndektsv4rrffq69g5faw_p":{}}}}}`)
	configDigest := sha256.Sum256(config)
	proxyArtifact.Services = append(proxyArtifact.Services, &agentpb.ComposeService{
		ServiceId: member.ServiceId, ComposeName: "api-proxy",
		Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
		ExpectedLabels: []*agentpb.LabelPair{
			{Key: "com.groundplane.runtime-role", Value: "proxy"},
		},
		ProxyConfigJson: config, ProxyConfigSha256: configDigest[:],
	})
	proxyAssignment.Plan.Artifacts[0] = proto.CloneOf(proxyArtifact)
	sealNativeAssignmentArtifact(t, proxyAssignment.RestorationAuthority.NativePredecessors[0], proxyArtifact, nil)
	if err := testtaskassignment.ValidateCandidateReleaseAuthority(proxyAssignment, proxyAssignment.Plan); err != nil {
		t.Fatalf("valid native proxy authority: %v", err)
	}
	if !proto.Equal(appliedBefore, proxyAssignment.RestorationAuthority.AppliedPredecessor) {
		t.Fatal("native proxy fixture rewrote the independently sealed applied witness")
	}
	proxy := testcomposeruntime.StepResult{ProxyEvidence: &agentpb.ServiceProxyEvidence{
		ServiceId: member.ServiceId, Target: "blue", ProxyGeneration: 7, ConfigSha256: configDigest[:],
		ReleaseId: nativeAssignmentPriorRelease, Compensated: true,
	}}
	if !testcomposeruntime.ReleaseRestorationEvidenceProven(proxyAssignment, step, proxy) {
		t.Fatal("exact serving predecessor proxy evidence was rejected")
	}
}

func TestReleaseProbeEvidenceStatusRequiresExactSealedAuthority(t *testing.T) {
	proxyStep := releaseProbe("probe-api")
	proxyAssignment := testtaskassignment.Assignment{Plan: &agentpb.ExecutionPlan{Artifacts: []*agentpb.ComposeArtifact{
		releaseTestArtifact("candidate-artifact"), releaseTestArtifact("prior-artifact"),
	}}}
	proxy := testcomposeruntime.StepResult{ProxyEvidence: releaseExecutionSuccess("api", false).GetProxyEvidence()}
	required, err := testcomposeruntime.ReleaseProbeEvidenceStatus(proxyAssignment, proxyStep, proxy)
	if err != nil || !required {
		t.Fatalf("exact proxy probe = %t/%v, want compensation", required, err)
	}
	proxy.ProxyEvidence.ProxyGeneration++
	if _, err := testcomposeruntime.ReleaseProbeEvidenceStatus(proxyAssignment, proxyStep, proxy); err == nil {
		t.Fatal("proxy probe accepted mismatched generation")
	}

	recreateStep := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_ServiceRecreateProbe{
		ServiceRecreateProbe: &agentpb.ServiceRecreateProbe{CandidateArtifactId: "candidate-artifact",
			PriorArtifactId: "prior-artifact", ServiceId: "worker", CandidateReleaseId: "candidate-worker",
			PriorReleaseId: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
	}}
	recreate := testcomposeruntime.StepResult{RecreateEvidence: &agentpb.ServiceRecreateEvidence{ServiceId: "worker",
		ArtifactId: "prior-artifact", ReleaseId: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", Target: "singleton", Compensated: true}}
	if required, err := testcomposeruntime.ReleaseProbeEvidenceStatus(proxyAssignment, recreateStep, recreate); err != nil ||
		required {
		t.Fatalf("exact recreate probe = %t/%v, want restored", required, err)
	}
	recreate.RecreateEvidence.Target = "blue"
	if _, err := testcomposeruntime.ReleaseProbeEvidenceStatus(proxyAssignment, recreateStep, recreate); err == nil {
		t.Fatal("recreate probe accepted mismatched target")
	}

	absenceAssignment, _, absence := exactCandidateAbsenceCompensation()
	absenceStep := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
		CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{CandidateArtifactId: "candidate-artifact",
			ServiceId: "api", CandidateReleaseId: "release-api"},
	}}
	if required, err := testcomposeruntime.ReleaseProbeEvidenceStatus(absenceAssignment, absenceStep,
		testcomposeruntime.StepResult{CandidateAbsenceEvidence: absence}); err != nil || required {
		t.Fatalf("exact absence probe = %t/%v, want restored", required, err)
	}
	absence = proto.Clone(absence).(*agentpb.CandidateAbsenceEvidence)
	absence.AuthoritySha256[0]++
	if _, err := testcomposeruntime.ReleaseProbeEvidenceStatus(absenceAssignment, absenceStep,
		testcomposeruntime.StepResult{CandidateAbsenceEvidence: absence}); err == nil {
		t.Fatal("absence probe accepted mismatched authority")
	}
}

func exactProxyCompensationStep() *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_ServiceProxyCompensate{
		ServiceProxyCompensate: &agentpb.ServiceProxyCompensate{
			ServiceId: "api", PriorTarget: "green", ProxyGeneration: 7,
			ConfigSha256: bytes.Repeat([]byte{0x31}, 32), PriorReleaseId: "prior-api", Enabled: true,
		},
	}}
}

func exactRecreateCompensationStep() *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_ServiceRecreateCompensate{
		ServiceRecreateCompensate: &agentpb.ServiceRecreateCompensate{
			ServiceId: "worker", ArtifactId: "prior-artifact", PriorReleaseId: "prior-worker",
			PriorTarget: "singleton", Enabled: true,
		},
	}}
}

func exactCandidateAbsenceCompensation() (testtaskassignment.Assignment, *agentpb.ExecutionStep, *agentpb.CandidateAbsenceEvidence) {
	planHash := bytes.Repeat([]byte{0x51}, 32)
	authorityDigest := bytes.Repeat([]byte{0x61}, 32)
	candidates := []*agentpb.ReleaseRestorationCandidate{
		{
			ServiceId: "api",
			ReleaseId: "release-api",
			Target:    agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE,
		},
		{
			ServiceId: "worker",
			ReleaseId: "release-worker",
			Target:    agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE,
		},
	}
	services := []*agentpb.CandidateReleaseService{
		{ServiceId: "api", ReleaseId: "release-api"},
		{ServiceId: "worker", ReleaseId: "release-worker"},
	}
	assignment := testtaskassignment.Assignment{
		AssignmentID: "assignment-api", Plan: &agentpb.ExecutionPlan{
			PlanHash: planHash,
			CandidateReleaseProcedure: &agentpb.CandidateReleaseProcedure{Members: []*agentpb.CandidateReleaseMember{
				{
					ServiceId: "api", CandidateReleaseId: "release-api", CandidateArtifactId: "candidate-artifact",
					CandidateAbsence: &agentpb.CandidateAbsenceRestoration{
						ComposeProjectName: "gp-project",
						Services:           services,
					},
				},
				{
					ServiceId: "worker", CandidateReleaseId: "release-worker", CandidateArtifactId: "candidate-artifact",
					CandidateAbsence: &agentpb.CandidateAbsenceRestoration{
						ComposeProjectName: "gp-project",
						Services:           services,
					},
				},
			}},
		},
		RestorationAuthority: &agentpb.ReleaseRestorationAuthority{
			PlanHash: planHash, CandidateArtifactId: "candidate-artifact", AuthoritySha256: authorityDigest,
			Candidates: candidates,
		},
	}
	step := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
		CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
			ServiceId: "api", CandidateReleaseId: "release-api", CandidateArtifactId: "candidate-artifact",
		},
	}}
	evidence := &agentpb.CandidateAbsenceEvidence{
		AssignmentId: assignment.AssignmentID, PlanHash: planHash, AuthoritySha256: authorityDigest,
		ComposeProjectName: "gp-project", CandidateArtifactId: "candidate-artifact",
		Candidates: []*agentpb.CandidateReleaseService{proto.CloneOf(services[0])}, AbsenceProven: true,
	}
	return assignment, step, evidence
}
