package agent

import (
	"bytes"
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/adapters/postgres16"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: the Agent must select one stable labeled container, reconstruct
// only its compiled adapter, keep secrets out of argv, and clear plan bytes.
func TestAdapterRuntimeExecutesCompiledPostgresProcedure(t *testing.T) {
	postgres16.Register()
	fake := &adapterRuntimeRunner{results: []runner.Result{
		{Stdout: []byte("0123456789ab\n"), ExitCode: 0},
		{ExitCode: 0}, {ExitCode: 0}, {ExitCode: 0},
	}}
	procedure := &agentpb.AdapterProcedure{
		AdapterKey: "postgres:16", Phase: agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION,
		AttachId: "att_01ARZ3NDEKTSV4RRFFQ69G5FAV", BackingServiceId: "bks_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		RuntimeServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		Role:             "api_5d3f9a", Database: "api_5d3f9a", Password: []byte("URL_safe-1"),
	}
	password := procedure.Password
	runtime := NewAdapterRuntime(fake)
	_, err := runtime.executeStep(context.Background(), &agentpb.ExecutionStep{
		Payload: &agentpb.ExecutionStep_AdapterProcedure{AdapterProcedure: procedure},
	})
	if err != nil {
		t.Fatalf("executeStep() error = %v", err)
	}
	if len(fake.calls) != 4 || fake.calls[0].Name != "docker" ||
		!containsArgument(fake.calls[0].Args, "label=com.groundplane.service-id="+procedure.RuntimeServiceId) {
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
	for _, character := range password {
		if character != 0 {
			t.Fatal("adapter procedure password was not cleared")
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
