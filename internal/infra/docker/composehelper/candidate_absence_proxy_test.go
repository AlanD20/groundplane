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

// Rationale: a first addressable candidate owns a stable proxy without a
// Release label. Absence must remove that exact proxy but reject foreign ones.
func TestCandidateAbsenceRemovesOnlyItsFirstProxy(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		name := "owned proxy"
		if foreign {
			name = "foreign proxy"
		}
		t.Run(name, func(t *testing.T) {
			request, artifact, step := candidateAbsenceFixture(false)
			proxy := proto.CloneOf(artifact.Services[0])
			proxy.Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY
			proxy.ExpectedLabels = proxy.ExpectedLabels[:3]
			proxy.ExpectedLabels = append(
				proxy.ExpectedLabels,
				&agentpb.LabelPair{Key: "com.groundplane.runtime-role", Value: "proxy"},
			)
			artifact.Services = append(artifact.Services, proxy)
			labels := map[string]string{"com.docker.compose.project": artifact.ProjectName}
			for _, label := range proxy.ExpectedLabels {
				labels[label.Key] = label.Value
			}
			if foreign {
				labels["com.groundplane.artifact-id"] = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
			}
			encoded, err := json.Marshal(labels)
			if err != nil {
				t.Fatal(err)
			}
			removed := false
			fake := runner.NewFake()
			fake.RunFunc = func(_ context.Context, options runner.RunCmdOpts) (runner.Result, error) {
				switch options.Args[1] {
				case "ls":
					if removed {
						return runner.Result{}, nil
					}
					return runner.Result{Stdout: []byte("0123456789abcdef\n")}, nil
				case "inspect":
					return runner.Result{Stdout: encoded}, nil
				case "rm":
					if foreign ||
						!slices.Equal(options.Args, []string{"container", "rm", "--force", "--", "0123456789abcdef"}) {
						t.Fatalf("unexpected proxy removal: %v", options.Args)
					}
					removed = true
					return runner.Result{}, nil
				default:
					t.Fatalf("unexpected Docker argv: %v", options.Args)
					return runner.Result{}, nil
				}
			}
			response, err := executeCandidateAbsence(context.Background(), fake, 30, request, artifact, step)
			if err != nil {
				t.Fatal(err)
			}
			if foreign {
				if removed || response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED {
					t.Fatal("foreign proxy accepted")
				}
			} else if !removed || !response.GetCandidateAbsenceEvidence().GetAbsenceProven() {
				t.Fatalf("first proxy absence not proven: %v", response)
			}
		})
	}
}
