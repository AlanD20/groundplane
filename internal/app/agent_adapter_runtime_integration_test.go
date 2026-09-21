package app

import (
	"bytes"
	"context"
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/agent"
	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	adapterRuntimeAssignmentID = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	adapterRuntimeTaskID       = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	adapterRuntimeOperationID  = "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	adapterRuntimePlanID       = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	adapterRuntimeStepID       = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	adapterRuntimeAttachID     = "att_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	adapterRuntimeServiceID    = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	adapterRuntimeContainerID  = "0123456789ab"
)

// Rationale: selectable Valkey authentication may not fall back to an implied
// mode at the Agent boundary or reach container discovery.
func TestAdapterRuntimeRejectsUnsetValkeyAuthenticationBeforeEffects(t *testing.T) {
	fake := &adapterRuntimeRunner{}
	result := runAdapterProcedure(t, fake, &agentpb.AdapterProcedure{
		AdapterKey:       "valkey:9",
		Phase:            agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION,
		AttachId:         adapterRuntimeAttachID,
		BackingServiceId: adapterRuntimeServiceID,
		Role:             "api_5d3f9a",
		Database:         "api_5d3f9a",
		Password:         []byte("owner-password"),
	})
	if result.Terminal != agent.TaskTerminalFailed || len(fake.calls) != 0 {
		t.Fatalf("unset Valkey authentication terminal = %v, runner calls = %d", result.Terminal, len(fake.calls))
	}
}

// Rationale: non-secret Valkey operations are discrete argv and legitimately
// carry no stdin, while SQL continues to require a body.
func TestAdapterCommandAllowsArgumentOnlyCompiledExec(t *testing.T) {
	fake := &adapterRuntimeRunner{results: []runner.Result{
		{Stdout: []byte(adapterRuntimeContainerID + "\n")},
		{},
		{},
	}}
	result := runAdapterProcedure(t, fake, &agentpb.AdapterProcedure{
		AdapterKey:       "valkey:9",
		Phase:            agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION,
		AttachId:         adapterRuntimeAttachID,
		BackingServiceId: adapterRuntimeServiceID,
		Authentication:   agentpb.BackingAuthentication_BACKING_AUTHENTICATION_USERNAME_PASSWORD,
		Role:             "api_5d3f9a",
		Database:         "api_5d3f9a",
		Password:         []byte("owner-password"),
	})
	if result.Terminal != agent.TaskTerminalCompleted || len(fake.calls) != 3 {
		t.Fatalf("compiled Valkey terminal = %v, runner calls = %d", result.Terminal, len(fake.calls))
	}
	save := fake.calls[2]
	if save.Name != "docker" || !slices.Equal(save.Args, []string{
		"container", "exec", "-i", adapterRuntimeContainerID,
		"valkey-cli", "--user", "groundplane", "-e", "ACL", "SAVE",
	}) || save.Stdin != nil {
		t.Fatalf("compiled Valkey ACL SAVE call = %#v", save)
	}
}

// Rationale: the Agent must select one stable labeled container, reconstruct only its compiled
// adapter, keep secrets out of argv, and leave the WorkerPool-owned immutable plan valid for later steps.
func TestAdapterRuntimeExecutesCompiledPostgresProcedure(t *testing.T) {
	fake := &adapterRuntimeRunner{results: []runner.Result{
		{Stdout: []byte(adapterRuntimeContainerID + "\n")},
		{},
		{},
		{},
	}}
	procedure := &agentpb.AdapterProcedure{
		AdapterKey:       "postgres:16",
		Phase:            agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION,
		AttachId:         adapterRuntimeAttachID,
		BackingServiceId: adapterRuntimeServiceID,
		Role:             "api_5d3f9a",
		Database:         "api_5d3f9a",
		Password:         []byte("URL_safe-1"),
	}
	result := runAdapterProcedure(t, fake, procedure)
	if result.Terminal != agent.TaskTerminalCompleted {
		t.Fatalf("compiled Postgres result = %#v", result)
	}
	if len(fake.calls) != 4 || fake.calls[0].Name != "docker" ||
		!containsArgument(fake.calls[0].Args, "label=com.groundplane.service-id="+procedure.BackingServiceId) {
		t.Fatalf("container lookup runner calls = %d", len(fake.calls))
	}
	for index, call := range fake.calls[1:] {
		if call.Name != "docker" || !containsArgument(call.Args, "psql") ||
			containsArgument(call.Args, "URL_safe-1") {
			t.Fatalf("adapter exec call %d did not preserve the compiled secret boundary", index+1)
		}
	}
	if !bytes.Contains(fake.calls[1].Stdin, []byte("URL_safe-1")) {
		t.Fatal("compiled Postgres stdin omitted the procedure secret")
	}
	if string(procedure.Password) != "URL_safe-1" {
		t.Fatal("WorkerPool mutated its input procedure password")
	}
}

func runAdapterProcedure(
	t *testing.T,
	fake *adapterRuntimeRunner,
	procedure *agentpb.AdapterProcedure,
) agent.TaskResult {
	t.Helper()
	registerAdapters()
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema:           executionplan.SchemaVersion,
		PlanId:           adapterRuntimePlanID,
		RenderGeneration: 1,
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_ATTACH,
		TargetId:         procedure.AttachId,
		Steps: []*agentpb.ExecutionStep{{
			StepId:         adapterRuntimeStepID,
			TimeoutSeconds: 1,
			Payload: &agentpb.ExecutionStep_AdapterProcedure{
				AdapterProcedure: procedure,
			},
		}},
	})
	if err != nil {
		t.Fatalf("executionplan.Seal() error = %v", err)
	}

	pool := agent.NewWorkerPool(1, "/var/lib/groundplane/volumes", fake, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for WorkerPool shutdown")
		}
	}
	defer stop()

	now := time.Now()
	if err := pool.Submit(ctx, testtaskassignment.Assignment{
		AssignmentID:     adapterRuntimeAssignmentID,
		TaskID:           adapterRuntimeTaskID,
		OperationID:      adapterRuntimeOperationID,
		Plan:             plan,
		ExecutionEpoch:   1,
		ExecutionMode:    agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
		ForwardDeadline:  now.Add(time.Minute),
		RecoveryDeadline: now.Add(2 * time.Minute),
	}); err != nil {
		t.Fatalf("WorkerPool.Submit() error = %v", err)
	}

	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case output := <-pool.Outputs():
			if output.Result != nil {
				return *output.Result
			}
		case <-timer.C:
			t.Fatal("timed out waiting for WorkerPool result")
		}
	}
}

type adapterRuntimeRunner struct {
	results []runner.Result
	calls   []runner.RunCmdOpts
}

func (fake *adapterRuntimeRunner) Run(_ context.Context, options runner.RunCmdOpts) (runner.Result, error) {
	owned := options
	owned.Args = append([]string(nil), options.Args...)
	owned.Stdin = append([]byte(nil), options.Stdin...)
	fake.calls = append(fake.calls, owned)
	if len(fake.results) == 0 {
		return runner.Result{}, nil
	}
	result := fake.results[0]
	fake.results = fake.results[1:]
	return result, nil
}

func (fake *adapterRuntimeRunner) Stream(
	context.Context,
	runner.RunCmdOpts,
	func(bool, string),
) (runner.Result, error) {
	return runner.Result{}, nil
}

func containsArgument(arguments []string, expected string) bool {
	for _, argument := range arguments {
		if argument == expected {
			return true
		}
	}
	return false
}
