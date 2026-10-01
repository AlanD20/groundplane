package taskplanning

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// NET-04 / SVC-04: removing a Zone must disconnect the actual singleton,
// blue-green proxy and retained slots without creating a desired-state workload.
func TestZoneRemovalDisconnectsExistingRuntimeWithoutDeploy(t *testing.T) {
	for _, members := range [][]string{nil, {"singleton"}, {"proxy", "blue", "green"}} {
		t.Run(strings.Join(members, "+"), func(t *testing.T) {
			now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
			environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
			zoneID := ids.NewAt(ids.KindNetwork, now, 2)
			taskID := ids.NewAt(ids.KindTask, now, 3)
			task := etcd.TaskRecord{
				ID: taskID, Executor: testtaskjournal.TaskExecutorAgent,
				PlanID: ids.NewAt(ids.KindPlan, now, 4), Type: testtaskjournal.TaskRemove,
				Target: zoneID, TimeoutSeconds: 120,
			}
			intent := testenvironmentchanges.ZoneRemovalIntent{
				OperationID: ids.NewAt(ids.KindOperation, now, 5), ActiveTaskID: taskID,
				EnvironmentID: environmentID, ZoneID: zoneID, Status: testtaskjournal.TaskStatusPending,
				AffectedServiceIDs: []string{ids.NewAt(ids.KindService, now, 6)},
				Claim:              testblueprints.EnvironmentBlueprintStageClaim{RevisionID: taskID},
				CandidateProjection: testenvironmentprojection.EnvironmentComposeProjection{
					RenderGeneration: 2,
				},
			}
			resolver := &TaskPlanResolver{volumeRoot: "/var/lib/groundplane/vol"}
			prepared, err := resolver.PrepareZoneRemovalTask(
				t.Context(), task, intent, ids.NewAt(ids.KindStep, now, 7),
			)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := resolver.buildZoneRemovalPlan(prepared, intent)
			if err != nil || hex.EncodeToString(plan.GetPlanHash()) != prepared.PlanHash {
				t.Fatalf("rebuilding sealed plan = %v", err)
			}
			host := &zoneRemovalRuntimeHost{
				name: "gp_net_" + zoneID, environmentID: environmentID,
				members: append([]string(nil), members...), networkPresent: true,
			}
			request := &agentpb.ComposeHelperRequest{
				Schema: composehelper.SchemaVersion, AssignmentId: ids.NewAt(ids.KindAssignment, now, 8),
				TaskId: taskID, OperationId: intent.OperationID, Plan: plan, TimeoutSeconds: 120,
			}
			for range 2 {
				for _, step := range plan.Steps {
					request.StepId = step.StepId
					result, err := composehelper.Execute(t.Context(), host, request)
					if err != nil ||
						result.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED {
						t.Fatalf("removing/replaying Zone = %v, %v", result, err)
					}
				}
			}
			if host.networkPresent || len(host.disconnected) != len(members) {
				t.Fatalf("physical network cleanup incomplete: %#v", host)
			}
		})
	}
}

type zoneRemovalRuntimeHost struct {
	name, environmentID   string
	members, disconnected []string
	networkPresent        bool
}

func (host *zoneRemovalRuntimeHost) Run(_ context.Context, input runner.RunCmdOpts) (runner.Result, error) {
	args := input.Args
	// Any Compose or container mutation is an unwanted deployment, not Zone removal.
	if input.Name != composehelper.DockerExecutable || len(args) < 2 || args[0] != "network" {
		return runner.Result{ExitCode: 1}, nil
	}
	switch args[1] {
	case "ls":
		if host.networkPresent {
			return runner.Result{Stdout: []byte(host.name + "\n")}, nil
		}
	case "inspect":
		if args[3] == "{{json .Labels}}" {
			return runner.Result{Stdout: []byte(
				`{"com.groundplane.managed":"true","com.groundplane.kind":"network","com.groundplane.environment-id":"` +
					host.environmentID + `"}`,
			)}, nil
		}
		return runner.Result{Stdout: []byte(strings.Join(host.members, "\n"))}, nil
	case "disconnect":
		host.disconnected = append(host.disconnected, args[len(args)-1])
	case "rm":
		host.networkPresent = false
	default:
		return runner.Result{ExitCode: 1}, nil
	}
	return runner.Result{}, nil
}

func (*zoneRemovalRuntimeHost) Stream(context.Context, runner.RunCmdOpts, func(bool, string)) (runner.Result, error) {
	return runner.Result{ExitCode: 1}, nil
}
