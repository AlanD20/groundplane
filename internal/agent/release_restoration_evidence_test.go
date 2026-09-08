package agent

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
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
		result composeStepResult
		want   bool
	}{
		{name: "missing proof"},
		{name: "wrong proof kind", result: composeStepResult{RecreateEvidence: &agentpb.ServiceRecreateEvidence{
			ServiceId: "api", ReleaseId: "prior-api", ArtifactId: "prior-artifact", Target: "green", Compensated: true,
		}}},
		{name: "mismatched proof", result: composeStepResult{ProxyEvidence: &agentpb.ServiceProxyEvidence{
			ServiceId: "api", Target: "blue", ProxyGeneration: 7,
			ConfigSha256: bytes.Repeat([]byte{0x31}, 32), ReleaseId: "prior-api", Compensated: true,
		}}},
		{name: "exact proof", result: composeStepResult{ProxyEvidence: exact}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := releaseRestorationEvidenceProven(Assignment{}, step, test.result); got != test.want {
				t.Fatalf("releaseRestorationEvidenceProven() = %t, want %t", got, test.want)
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
		result composeStepResult
		want   bool
	}{
		{name: "missing proof"},
		{name: "wrong proof kind", result: composeStepResult{ProxyEvidence: &agentpb.ServiceProxyEvidence{
			ServiceId: "worker", Target: "green", ProxyGeneration: 1,
			ConfigSha256: bytes.Repeat([]byte{0x41}, 32), ReleaseId: "prior-worker", Compensated: true,
		}}},
		{name: "mismatched proof", result: composeStepResult{RecreateEvidence: &agentpb.ServiceRecreateEvidence{
			ServiceId: "worker", ReleaseId: "prior-worker", ArtifactId: "candidate-artifact",
			Target: "singleton", Compensated: true,
		}}},
		{name: "exact proof", result: composeStepResult{RecreateEvidence: exact}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := releaseRestorationEvidenceProven(Assignment{}, step, test.result); got != test.want {
				t.Fatalf("releaseRestorationEvidenceProven() = %t, want %t", got, test.want)
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
		result composeStepResult
		want   bool
	}{
		{name: "missing proof"},
		{name: "wrong proof kind", result: composeStepResult{RecreateEvidence: &agentpb.ServiceRecreateEvidence{
			ServiceId: "api", ReleaseId: "release-api", ArtifactId: "candidate-artifact",
			Target: "singleton", Compensated: true,
		}}},
		{
			name: "mismatched proof",
			result: composeStepResult{CandidateAbsenceEvidence: &agentpb.CandidateAbsenceEvidence{
				AssignmentId: exact.GetAssignmentId(), PlanHash: append([]byte(nil), exact.GetPlanHash()...),
				AuthoritySha256: append(
					[]byte(nil),
					exact.GetAuthoritySha256()...), ComposeProjectName: "wrong-project",
				CandidateArtifactId: exact.GetCandidateArtifactId(), Candidates: exact.GetCandidates(), AbsenceProven: true,
			}},
		},
		{name: "exact proof", result: composeStepResult{CandidateAbsenceEvidence: exact}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := releaseRestorationEvidenceProven(assignment, step, test.result); got != test.want {
				t.Fatalf("releaseRestorationEvidenceProven() = %t, want %t", got, test.want)
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
	if !releaseRestorationEvidenceProven(assignment, step, composeStepResult{CandidateAbsenceEvidence: exact}) {
		t.Fatal("exact member proof rejected")
	}
	if releaseRestorationEvidenceProven(assignment, step, composeStepResult{CandidateAbsenceEvidence: duplicate}) {
		t.Fatal("canonical candidate membership accepted a duplicate")
	}
	for name, evidence := range map[string]*agentpb.CandidateAbsenceEvidence{"missing": missing, "extra": extra, "other": other} {
		if releaseRestorationEvidenceProven(assignment, step, composeStepResult{CandidateAbsenceEvidence: evidence}) {
			t.Fatalf("canonical candidate membership accepted %s membership", name)
		}
	}
}

func TestCandidateServingPredecessorAcceptsExactTypedRestorationVariant(t *testing.T) {
	releaseID := "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	artifact := &agentpb.ComposeArtifact{
		ArtifactId:  "prior-artifact",
		ProjectName: "gp-release",
		Services: []*agentpb.ComposeService{{
			ServiceId: "api", ComposeName: "api", ExpectedReplicas: 2,
			Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			ExpectedLabels: []*agentpb.LabelPair{{Key: "com.groundplane.release-id", Value: releaseID}},
		}},
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	assignment := Assignment{RestorationAuthority: &agentpb.ReleaseRestorationAuthority{
		Candidates: []*agentpb.ReleaseRestorationCandidate{
			{ServiceId: "api", Target: agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR},
		},
		AppliedPredecessor: &agentpb.ReleaseAppliedPredecessorAuthority{
			ComposeArtifact:       encoded,
			ComposeArtifactSha256: digest[:],
		},
	}}
	step := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
		CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{ServiceId: "api"},
	}}
	exact := composeStepResult{RecreateEvidence: &agentpb.ServiceRecreateEvidence{
		ServiceId: "api", ArtifactId: "prior-artifact", ReleaseId: releaseID, Target: "singleton", Compensated: true,
	}}
	if !releaseRestorationEvidenceProven(assignment, step, exact) {
		t.Fatal("exact serving predecessor recreate evidence was rejected")
	}
	exact.RecreateEvidence.ArtifactId = "wrong-artifact"
	if releaseRestorationEvidenceProven(assignment, step, exact) {
		t.Fatal("mismatched serving predecessor recreate evidence was accepted")
	}
	config := []byte(`{"apps":{"http":{"servers":{"gp_g7_dep_01arz3ndektsv4rrffq69g5fav_p":{}}}}}`)
	configDigest := sha256.Sum256(config)
	artifact.Services = append(artifact.Services, &agentpb.ComposeService{
		ServiceId: "api", ComposeName: "api-proxy", Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
		ProxyConfigJson: config, ProxyConfigSha256: configDigest[:],
	})
	encoded, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	digest = sha256.Sum256(encoded)
	assignment.RestorationAuthority.AppliedPredecessor.ComposeArtifact = encoded
	assignment.RestorationAuthority.AppliedPredecessor.ComposeArtifactSha256 = digest[:]
	proxy := composeStepResult{ProxyEvidence: &agentpb.ServiceProxyEvidence{
		ServiceId: "api", Target: "singleton", ProxyGeneration: 7, ConfigSha256: configDigest[:],
		ReleaseId: releaseID, Compensated: true,
	}}
	if !releaseRestorationEvidenceProven(assignment, step, proxy) {
		t.Fatal("exact serving predecessor proxy evidence was rejected")
	}
}

func TestReleaseProbeEvidenceStatusRequiresExactSealedAuthority(t *testing.T) {
	proxyStep := releaseProbe("probe-api")
	proxyAssignment := Assignment{Plan: &agentpb.ExecutionPlan{Artifacts: []*agentpb.ComposeArtifact{
		releaseTestArtifact("candidate-artifact"), releaseTestArtifact("prior-artifact"),
	}}}
	proxy := composeStepResult{ProxyEvidence: releaseExecutionSuccess("api", false).GetProxyEvidence()}
	required, err := releaseProbeEvidenceStatus(proxyAssignment, proxyStep, proxy)
	if err != nil || !required {
		t.Fatalf("exact proxy probe = %t/%v, want compensation", required, err)
	}
	proxy.ProxyEvidence.ProxyGeneration++
	if _, err := releaseProbeEvidenceStatus(proxyAssignment, proxyStep, proxy); err == nil {
		t.Fatal("proxy probe accepted mismatched generation")
	}

	recreateStep := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_ServiceRecreateProbe{
		ServiceRecreateProbe: &agentpb.ServiceRecreateProbe{CandidateArtifactId: "candidate-artifact",
			PriorArtifactId: "prior-artifact", ServiceId: "worker", CandidateReleaseId: "candidate-worker",
			PriorReleaseId: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
	}}
	recreate := composeStepResult{RecreateEvidence: &agentpb.ServiceRecreateEvidence{ServiceId: "worker",
		ArtifactId: "prior-artifact", ReleaseId: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", Target: "singleton", Compensated: true}}
	if required, err := releaseProbeEvidenceStatus(proxyAssignment, recreateStep, recreate); err != nil || required {
		t.Fatalf("exact recreate probe = %t/%v, want restored", required, err)
	}
	recreate.RecreateEvidence.Target = "blue"
	if _, err := releaseProbeEvidenceStatus(proxyAssignment, recreateStep, recreate); err == nil {
		t.Fatal("recreate probe accepted mismatched target")
	}

	absenceAssignment, _, absence := exactCandidateAbsenceCompensation()
	absenceStep := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
		CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{CandidateArtifactId: "candidate-artifact",
			ServiceId: "api", CandidateReleaseId: "release-api"},
	}}
	if required, err := releaseProbeEvidenceStatus(absenceAssignment, absenceStep,
		composeStepResult{CandidateAbsenceEvidence: absence}); err != nil || required {
		t.Fatalf("exact absence probe = %t/%v, want restored", required, err)
	}
	absence = proto.Clone(absence).(*agentpb.CandidateAbsenceEvidence)
	absence.AuthoritySha256[0]++
	if _, err := releaseProbeEvidenceStatus(absenceAssignment, absenceStep,
		composeStepResult{CandidateAbsenceEvidence: absence}); err == nil {
		t.Fatal("absence probe accepted mismatched authority")
	}
}

// Rationale: recreate recovery may prove a sealed blue-green predecessor, but only
// from one exact valid slot; missing, unsupported, or replicated slots are not authority.
func TestRecreateEvidenceExpectationAcceptsOnlyExactSealedPriorSlot(t *testing.T) {
	plan := &agentpb.ExecutionPlan{Artifacts: []*agentpb.ComposeArtifact{releaseTestArtifact("prior-artifact")}}
	expected, ok := recreateEvidenceExpectation(plan, "prior-artifact", "api", "prior-api", true)
	if !ok || expected.target != "green" {
		t.Fatalf("sealed prior expectation = %#v, valid = %t", expected, ok)
	}
	for _, test := range []struct {
		name   string
		mutate func(*agentpb.ComposeService)
	}{
		{name: "missing slot", mutate: func(service *agentpb.ComposeService) { service.Slot = "" }},
		{name: "unsupported slot", mutate: func(service *agentpb.ComposeService) { service.Slot = "red" }},
		{name: "replicated slot", mutate: func(service *agentpb.ComposeService) { service.ExpectedReplicas = 2 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			owned := proto.Clone(plan).(*agentpb.ExecutionPlan)
			test.mutate(owned.Artifacts[0].Services[1])
			if _, valid := recreateEvidenceExpectation(
				owned, "prior-artifact", "api", "prior-api", true,
			); valid {
				t.Fatal("invalid prior slot produced recreate evidence authority")
			}
		})
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

func exactCandidateAbsenceCompensation() (Assignment, *agentpb.ExecutionStep, *agentpb.CandidateAbsenceEvidence) {
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
	assignment := Assignment{
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
