package composehelper

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const servingRecoveryCandidateID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAX"

// Rationale: a failed candidate is not foreign runtime. Its exact sealed
// identity authorizes cleanup, followed by independently observed restoration.
func TestServingRecoveryRemovesExactFailedCandidateAndRestoresPrior(t *testing.T) {
	for _, scenario := range []string{"restored", "unrelated member", "foreign selected name", "cleanup failed", "candidate remains", "actual image differs", "probe only"} {
		t.Run(scenario, func(t *testing.T) { testServingCandidateCleanup(t, scenario) })
	}
}

func testServingCandidateCleanup(t *testing.T, scenario string) {
	t.Helper()
	old := &agentpb.ComposeService{
		ServiceId: helperServiceID, ComposeName: "api", ExpectedReplicas: 1,
		Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
		ImageReference: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ExpectedLabels: []*agentpb.LabelPair{
			{Key: "com.groundplane.service-id", Value: helperServiceID},
			{Key: "com.groundplane.managed", Value: "true"},
			{Key: "com.groundplane.runtime-role", Value: "singleton"},
			{Key: "com.groundplane.release-id", Value: candidateAbsenceReleaseID},
		},
	}
	predecessor := &agentpb.ComposeArtifact{ArtifactId: helperArtifactID, ProjectName: "gp-serving",
		CanonicalYaml: []byte("services:\n  api: {}\n"), Services: []*agentpb.ComposeService{old}}
	candidate := proto.CloneOf(predecessor)
	candidate.Services[0].ExpectedLabels[3].Value = servingRecoveryCandidateID
	candidate.Services[0].ImageReference = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	request, _ := sealedServingPredecessorRequest(t, predecessor)
	request.RestorationAuthority.Candidates[0].ReleaseId = servingRecoveryCandidateID
	step := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
		CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
			ServiceId: helperServiceID, CandidateReleaseId: servingRecoveryCandidateID,
		},
	}}
	if scenario == "probe only" {
		step.Payload = &agentpb.ExecutionStep_CandidateRestorationProbe{
			CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
				ServiceId: helperServiceID, CandidateReleaseId: servingRecoveryCandidateID,
			},
		}
	}
	removed, restored := false, false
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, options runner.RunCmdOpts) (runner.Result, error) {
		if options.Args[0] == "compose" {
			if slices.Contains(options.Args, "up") {
				if !removed {
					t.Fatal("predecessor started before exact candidate cleanup")
				}
				restored = true
			}
			return runner.Result{}, nil
		}
		switch options.Args[1] {
		case "ls":
			extra := ""
			if scenario == "unrelated member" || scenario == "foreign selected name" {
				extra = "cccccccccccccccc\n"
			}
			if restored {
				return runner.Result{Stdout: []byte("aaaaaaaaaaaaaaaa\n" + extra)}, nil
			}
			if removed && scenario != "candidate remains" {
				return runner.Result{Stdout: []byte(extra)}, nil
			}
			return runner.Result{Stdout: []byte("bbbbbbbbbbbbbbbb\n" + extra)}, nil
		case "rm":
			if scenario == "probe only" || scenario == "actual image differs" {
				t.Fatal("observation-only case removed runtime")
			}
			if !slices.Equal(options.Args, []string{"container", "rm", "--force", "--", "bbbbbbbbbbbbbbbb"}) {
				t.Fatalf("wrong cleanup target: %v", options.Args)
			}
			if scenario == "cleanup failed" {
				return runner.Result{ExitCode: 1}, nil
			}
			removed = true
			return runner.Result{}, nil
		case "inspect":
			if options.Args[len(options.Args)-1] == "cccccccccccccccc" {
				if options.Args[3] != "{{json .Config.Labels}}" {
					t.Fatal("inspected another member beyond scope classification")
				}
				name := "worker"
				if scenario == "foreign selected name" {
					name = "api"
				}
				encoded, err := json.Marshal(
					map[string]string{
						"com.docker.compose.project": predecessor.ProjectName,
						"com.docker.compose.service": name,
						"com.groundplane.service-id": "another-service",
					},
				)
				if err != nil {
					t.Fatal(err)
				}
				return runner.Result{Stdout: encoded}, nil
			}
			service := candidate.Services[0]
			if restored {
				service = old
			}
			switch options.Args[3] {
			case "{{json .Config.Labels}}":
				labels := map[string]string{
					"com.docker.compose.project": predecessor.ProjectName,
					"com.docker.compose.service": service.ComposeName,
				}
				for _, label := range service.ExpectedLabels {
					labels[label.Key] = label.Value
				}
				encoded, err := json.Marshal(labels)
				if err != nil {
					t.Fatal(err)
				}
				return runner.Result{Stdout: encoded}, nil
			case "{{.Config.Image}}":
				return runner.Result{Stdout: []byte(service.ImageReference)}, nil
			case "{{.Config.Image}}\n{{.Image}}":
				if scenario == "actual image differs" {
					return runner.Result{
						Stdout: []byte(
							service.ImageReference + "\nsha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
						),
					}, nil
				}
				return runner.Result{Stdout: []byte(service.ImageReference + "\n" + service.ImageReference)}, nil
			case "{{json .State}}":
				return runner.Result{Stdout: []byte(`{"Running":true}`)}, nil
			}
		}
		t.Fatalf("unexpected command: %v", options.Args)
		return runner.Result{}, nil
	}
	response, err := executeServingPredecessor(context.Background(), fake, 30, request, candidate, step)
	if scenario == "probe only" {
		if err != nil || removed || restored ||
			response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_RESTORATION_REQUIRED ||
			response.GetProxyEvidence() != nil ||
			response.GetRecreateEvidence() != nil ||
			response.GetCandidateAbsenceEvidence() != nil {
			t.Fatalf("owned unrestored probe = %v, %v", response, err)
		}
		return
	}
	if scenario != "restored" && scenario != "unrelated member" {
		if err != nil || restored ||
			response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED {
			t.Fatalf("unproven candidate recovery = %v, %v, restored=%t", response, err, restored)
		}
		return
	}
	if err != nil || response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
		response.GetRecreateEvidence().GetReleaseId() != candidateAbsenceReleaseID || !removed || !restored {
		t.Fatalf("exact recovery = %v, %v, removed=%t restored=%t", response, err, removed, restored)
	}
}
