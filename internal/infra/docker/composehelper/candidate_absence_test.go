package composehelper

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const candidateAbsenceReleaseID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"

// Rationale: each compensation owns one member, including when another
// candidate in the same assignment must restore a serving predecessor.
func TestCandidateAbsenceDoesNotRemoveAnotherMember(t *testing.T) {
	for _, otherTarget := range []agentpb.ReleaseRestorationTarget{
		agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE,
		agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR,
	} {
		t.Run(otherTarget.String(), func(t *testing.T) {
			request, artifact, step := candidateAbsenceFixture(false)
			const otherID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
			other := proto.CloneOf(artifact.Services[0])
			other.ServiceId = otherID
			for _, label := range other.ExpectedLabels {
				if label.Key == "com.groundplane.service-id" {
					label.Value = otherID
				}
			}
			artifact.Services = append(artifact.Services, other)
			request.RestorationAuthority.Candidates = append(
				request.RestorationAuthority.Candidates,
				&agentpb.ReleaseRestorationCandidate{
					ServiceId: otherID,
					ReleaseId: candidateAbsenceReleaseID,
					Target:    otherTarget,
				},
			)
			member := proto.CloneOf(request.Plan.CandidateReleaseProcedure.Members[0])
			member.ServiceId = otherID
			request.Plan.CandidateReleaseProcedure.Members = append(
				request.Plan.CandidateReleaseProcedure.Members,
				member,
			)
			fake := runner.NewFake()
			fake.RunFunc = func(_ context.Context, options runner.RunCmdOpts) (runner.Result, error) {
				if len(options.Args) < 2 {
					t.Fatal("unexpected command")
				}
				switch options.Args[1] {
				case "ls":
					return runner.Result{Stdout: []byte("0123456789abcdef\n")}, nil
				case "inspect":
					return runner.Result{
						Stdout: []byte(
							`{"com.docker.compose.project":"gp-candidate","com.groundplane.managed":"true","com.groundplane.artifact-id":"` + helperArtifactID + `","com.groundplane.service-id":"` + otherID + `","com.groundplane.release-id":"` + candidateAbsenceReleaseID + `"}`,
						),
					}, nil
				default:
					t.Fatalf("compensation tried to mutate another member: %v", options.Args)
					return runner.Result{}, nil
				}
			}
			response, err := executeCandidateAbsence(context.Background(), fake, 30, request, artifact, step)
			evidence := response.GetCandidateAbsenceEvidence()
			if err != nil || !evidence.GetAbsenceProven() || len(evidence.GetCandidates()) != 1 ||
				evidence.GetCandidates()[0].GetServiceId() != helperServiceID {
				t.Fatalf("member-scoped absence = %v, %v", response, err)
			}
		})
	}
}

// Rationale: absence compensation may remove only the exact plan-owned
// candidate container, and completion requires a second exact-project probe.
func TestCandidateAbsenceCompensationRemovesExactCandidateThenProvesAbsence(t *testing.T) {
	request, artifact, step := candidateAbsenceFixture(false)
	fake := runner.NewFake()
	call := 0
	fake.RunFunc = func(_ context.Context, options runner.RunCmdOpts) (runner.Result, error) {
		call++
		switch call {
		case 1:
			return runner.Result{Stdout: []byte("0123456789abcdef\n")}, nil
		case 2:
			return runner.Result{
				Stdout: []byte(
					`{"com.docker.compose.project":"gp-candidate","com.groundplane.managed":"true","com.groundplane.artifact-id":"` + helperArtifactID + `","com.groundplane.service-id":"` + helperServiceID + `","com.groundplane.release-id":"` + candidateAbsenceReleaseID + `"}`,
				),
			}, nil
		case 3:
			if !strings.Contains(strings.Join(options.Args, " "), "container rm --force -- 0123456789abcdef") {
				t.Fatalf("candidate removal = %#v", options.Args)
			}
			return runner.Result{}, nil
		case 4:
			return runner.Result{}, nil
		default:
			t.Fatalf("unexpected runner call %d: %#v", call, options.Args)
			return runner.Result{}, nil
		}
	}

	response, err := executeCandidateAbsence(context.Background(), fake, 30, request, artifact, step)
	if err != nil || response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
		!response.GetCandidateAbsenceEvidence().GetAbsenceProven() || call != 4 {
		t.Fatalf("executeCandidateAbsence() = %#v, %v; calls=%d", response, err, call)
	}
}

// Rationale: a crash replay probes first and treats an already absent exact
// candidate as proven without issuing another mutation.
func TestCandidateAbsenceCompensationAlreadyAbsentIsProbeOnly(t *testing.T) {
	request, artifact, step := candidateAbsenceFixture(false)
	fake := runner.NewFake()
	fake.RunFunc = func(context.Context, runner.RunCmdOpts) (runner.Result, error) {
		return runner.Result{}, nil
	}
	response, err := executeCandidateAbsence(context.Background(), fake, 30, request, artifact, step)
	if err != nil || !response.GetCandidateAbsenceEvidence().GetAbsenceProven() || len(fake.Calls) != 1 {
		t.Fatalf("executeCandidateAbsence(already absent) = %#v, %v; calls=%d", response, err, len(fake.Calls))
	}
}

// Rationale: a selected service carrying a foreign Release identity is
// ambiguous host state, never proof that the sealed candidate is absent.
func TestCandidateAbsenceRejectsForeignSelectedService(t *testing.T) {
	request, artifact, step := candidateAbsenceFixture(true)
	fake := runner.NewFake()
	call := 0
	fake.RunFunc = func(context.Context, runner.RunCmdOpts) (runner.Result, error) {
		call++
		if call == 1 {
			return runner.Result{Stdout: []byte("0123456789abcdef\n")}, nil
		}
		return runner.Result{
			Stdout: []byte(
				`{"com.docker.compose.project":"gp-candidate","com.groundplane.managed":"true","com.groundplane.artifact-id":"` + helperArtifactID + `","com.groundplane.service-id":"` + helperServiceID + `","com.groundplane.release-id":"dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"}`,
			),
		}, nil
	}
	response, err := executeCandidateAbsence(context.Background(), fake, 30, request, artifact, step)
	if err != nil || response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED ||
		len(fake.Calls) != 2 {
		t.Fatalf("executeCandidateAbsence(foreign) = %#v, %v; calls=%d", response, err, len(fake.Calls))
	}
}

func candidateAbsenceFixture(
	probe bool,
) (*agentpb.ComposeHelperRequest, *agentpb.ComposeArtifact, *agentpb.ExecutionStep) {
	labels := []*agentpb.LabelPair{
		{Key: "com.groundplane.managed", Value: "true"},
		{Key: "com.groundplane.artifact-id", Value: helperArtifactID},
		{Key: "com.groundplane.service-id", Value: helperServiceID},
		{Key: "com.groundplane.release-id", Value: candidateAbsenceReleaseID},
	}
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: helperArtifactID, ProjectName: "gp-candidate",
		Services: []*agentpb.ComposeService{{ServiceId: helperServiceID, ExpectedLabels: labels}},
	}
	step := &agentpb.ExecutionStep{StepId: helperStepID}
	if probe {
		step.Payload = &agentpb.ExecutionStep_CandidateRestorationProbe{
			CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
				CandidateArtifactId: helperArtifactID, ServiceId: helperServiceID, CandidateReleaseId: candidateAbsenceReleaseID,
			},
		}
	} else {
		step.Payload = &agentpb.ExecutionStep_CandidateRestorationCompensate{CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
			CandidateArtifactId: helperArtifactID, ServiceId: helperServiceID, CandidateReleaseId: candidateAbsenceReleaseID,
		}}
	}
	planHash := make([]byte, 32)
	authorityHash := make([]byte, 32)
	plan := &agentpb.ExecutionPlan{
		PlanHash: planHash,
		CandidateReleaseProcedure: &agentpb.CandidateReleaseProcedure{Members: []*agentpb.CandidateReleaseMember{{
			ServiceId: helperServiceID, CandidateReleaseId: candidateAbsenceReleaseID, CandidateArtifactId: helperArtifactID,
			CandidateAbsence: &agentpb.CandidateAbsenceRestoration{ComposeProjectName: artifact.ProjectName},
		}}},
	}
	request := &agentpb.ComposeHelperRequest{
		AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV", TaskId: helperTaskID, OperationId: helperOperationID,
		Plan: plan, RestorationAuthority: &agentpb.ReleaseRestorationAuthority{
			TaskId: helperTaskID, OperationId: helperOperationID, PlanHash: planHash,
			AuthoritySha256:     authorityHash,
			CandidateArtifactId: helperArtifactID,
			Candidates: []*agentpb.ReleaseRestorationCandidate{
				{
					ServiceId: helperServiceID,
					ReleaseId: candidateAbsenceReleaseID,
					Target:    agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE,
				},
			},
		},
	}
	return request, artifact, step
}
