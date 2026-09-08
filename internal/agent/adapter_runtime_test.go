package agent

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/adapters/postgres16"
	"github.com/AlanD20/groundplane/internal/adapters/valkey9"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: selectable Valkey authentication may not fall back to an implied
// mode at the Agent boundary or reach container discovery.
func TestAdapterRuntimeRejectsUnsetValkeyAuthenticationBeforeEffects(t *testing.T) {
	valkey9.Register()
	fake := &adapterRuntimeRunner{}
	runtime := NewAdapterRuntime(fake)
	_, err := runtime.executeStep(context.Background(), &agentpb.ExecutionStep{
		Payload: &agentpb.ExecutionStep_AdapterProcedure{AdapterProcedure: &agentpb.AdapterProcedure{
			AdapterKey:       "valkey:9",
			Phase:            agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION,
			AttachId:         "att_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			BackingServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW",
			Role:             "default", Password: []byte("owner-password"),
		}},
	})
	if err == nil || len(fake.calls) != 0 {
		t.Fatalf("executeStep(unset Valkey mode) = %v, calls %#v", err, fake.calls)
	}
}

// Rationale: non-secret Valkey operations are discrete argv and legitimately
// carry no stdin, while SQL continues to require a body.
func TestAdapterCommandAllowsArgumentOnlyCompiledExec(t *testing.T) {
	t.Parallel()
	command, arguments, input, err := adapterCommand(adapters.Step{
		Op: adapters.StepExec, Program: "valkey-cli", Args: []string{"ACL", "SAVE"},
	})
	if err != nil || command != "valkey-cli" || !slices.Equal(arguments, []string{"ACL", "SAVE"}) || input != nil {
		t.Fatalf("adapterCommand(argument-only) = %q, %#v, %#v, %v", command, arguments, input, err)
	}
}

// Rationale: the Agent must select one stable labeled container, reconstruct only its compiled
// adapter, keep secrets out of argv, and leave the WorkerPool-owned immutable plan valid for later steps.
func TestAdapterRuntimeExecutesCompiledPostgresProcedure(t *testing.T) {
	postgres16.Register()
	fake := &adapterRuntimeRunner{results: []runner.Result{
		{Stdout: []byte("0123456789ab\n"), ExitCode: 0},
		{ExitCode: 0}, {ExitCode: 0}, {ExitCode: 0},
	}}
	procedure := &agentpb.AdapterProcedure{
		AdapterKey: "postgres:16", Phase: agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION,
		AttachId: "att_01ARZ3NDEKTSV4RRFFQ69G5FAV", BackingServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		Role: "api_5d3f9a", Database: "api_5d3f9a", Password: []byte("URL_safe-1"),
	}
	runtime := NewAdapterRuntime(fake)
	_, err := runtime.executeStep(context.Background(), &agentpb.ExecutionStep{
		Payload: &agentpb.ExecutionStep_AdapterProcedure{AdapterProcedure: procedure},
	})
	if err != nil {
		t.Fatalf("executeStep() error = %v", err)
	}
	if len(fake.calls) != 4 || fake.calls[0].Name != "docker" ||
		!containsArgument(fake.calls[0].Args, "label=com.groundplane.service-id="+procedure.BackingServiceId) {
		t.Fatalf("container lookup calls = %#v", fake.calls)
	}
	for _, call := range fake.calls[1:] {
		if call.Name != "docker" || !containsArgument(call.Args, "psql") ||
			containsArgument(call.Args, "URL_safe-1") {
			t.Fatalf("adapter exec call = %#v", call)
		}
	}
	if !bytes.Contains(fake.calls[1].Stdin, []byte("URL_safe-1")) {
		t.Fatalf("compiled stdin = %q", fake.calls[1].Stdin)
	}
	if string(procedure.Password) != "URL_safe-1" {
		t.Fatalf("adapter runtime mutated its WorkerPool-owned plan password: %q", procedure.Password)
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
