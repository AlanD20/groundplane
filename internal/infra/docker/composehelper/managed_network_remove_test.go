package composehelper

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type managedNetworkRunner struct {
	results  []runner.Result
	commands [][]string
}

func (fake *managedNetworkRunner) Run(_ context.Context, input runner.RunCmdOpts) (runner.Result, error) {
	fake.commands = append(fake.commands, append([]string(nil), input.Args...))
	result := fake.results[0]
	fake.results = fake.results[1:]
	return result, nil
}

func (*managedNetworkRunner) Stream(
	context.Context,
	runner.RunCmdOpts,
	func(bool, string),
) (runner.Result, error) {
	return runner.Result{}, nil
}

// Rationale: retries after a lost acknowledgement must treat an already
// absent managed network as success, while present networks must be ownership
// checked, disconnected deterministically, and removed through closed argv.
func TestManagedNetworkRemoveIsIdempotentAndOwnershipChecked(t *testing.T) {
	request, dockerName, environmentID := managedNetworkRequest(t)
	absent := &managedNetworkRunner{results: []runner.Result{{}}}
	response, err := Execute(context.Background(), absent, request)
	if err != nil || response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
		len(absent.commands) != 1 {
		t.Fatalf("Execute(absent) = %#v, %v; commands %#v", response, err, absent.commands)
	}

	present := &managedNetworkRunner{results: []runner.Result{
		{Stdout: []byte(dockerName + "\n")},
		{
			Stdout: []byte(
				`{"com.groundplane.managed":"true","com.groundplane.kind":"network","com.groundplane.environment-id":"` + environmentID + `"}`,
			),
		},
		{Stdout: []byte("bbbb\naaaa\n")},
		{},
		{},
		{},
	}}
	response, err = Execute(context.Background(), present, request)
	if err != nil || response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED {
		t.Fatalf("Execute(present) = %#v, %v", response, err)
	}
	wantTail := [][]string{
		{"network", "disconnect", "--force", dockerName, "aaaa"},
		{"network", "disconnect", "--force", dockerName, "bbbb"},
		{"network", "rm", dockerName},
	}
	if !reflect.DeepEqual(present.commands[len(present.commands)-3:], wantTail) {
		t.Fatalf("managed network mutation commands = %#v", present.commands)
	}
}

func managedNetworkRequest(t *testing.T) (*agentpb.ComposeHelperRequest, string, string) {
	t.Helper()
	now := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)
	zoneID := ids.NewAt(ids.KindNetwork, now, 1)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 2)
	dockerName := "gp_net_" + stringLower(zoneID)
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: ids.NewAt(ids.KindPlan, now, 3), RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetId: zoneID,
		Steps: []*agentpb.ExecutionStep{{
			StepId: ids.NewAt(ids.KindStep, now, 4), TimeoutSeconds: 120,
			Payload: &agentpb.ExecutionStep_ManagedNetworkRemove{ManagedNetworkRemove: &agentpb.ManagedNetworkRemove{
				NetworkId: zoneID, EnvironmentId: environmentID, DockerName: dockerName,
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	return &agentpb.ComposeHelperRequest{
		Schema: SchemaVersion, TaskId: ids.NewAt(ids.KindTask, now, 5),
		OperationId: ids.NewAt(ids.KindOperation, now, 6), Plan: plan,
		StepId: plan.Steps[0].StepId, TimeoutSeconds: 120,
	}, dockerName, environmentID
}

func stringLower(value string) string {
	result := []byte(value)
	for index, character := range result {
		if character >= 'A' && character <= 'Z' {
			result[index] = character + ('a' - 'A')
		}
	}
	return string(result)
}
