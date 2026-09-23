package adaptercompiler

import (
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/agent/backingadapter"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Compile is the application composition boundary between
// the compiled adapter catalog and the Agent's closed operation executor.
func Compile(procedure *agentpb.AdapterProcedure) ([]backingadapter.Step, error) {
	if procedure == nil {
		return nil, errs.New(errs.KindValidationFailed, "agent: adapter procedure is required")
	}
	adapter, found := adapters.Get(procedure.AdapterKey)
	if !found || adapter.Custom() {
		return nil, errs.New(errs.KindValidationFailed, "agent: adapter procedure is not registered")
	}
	authentication, err := backingadapter.DecodeBackingAuthentication(
		procedure.Authentication, adapter.SupportsAuthenticationModes(),
	)
	if err != nil || authentication == core.BackingAuthenticationNone {
		return nil, errs.New(errs.KindValidationFailed, "agent: adapter authentication mode is invalid")
	}
	params := adapters.Input{
		Authentication: authentication,
		Database:       procedure.Database,
		Role:           procedure.Role,
		Password:       append([]byte(nil), procedure.Password...),
		GrantOn:        procedure.GrantOn,
	}
	defer clear(params.Password)
	var compiled []adapters.Step
	switch procedure.Phase {
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION:
		compiled = adapter.ProvisionSteps(params)
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_GRANT:
		compiled = adapter.GrantSteps(params)
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_REVOKE:
		compiled = adapter.RevokeSteps(params)
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_DETACH:
		compiled = adapter.DetachSteps(params)
	}
	defer adapters.ClearSteps(compiled)
	operations := make([]backingadapter.Step, len(compiled))
	for index, step := range compiled {
		operations[index] = backingadapter.Step{
			Op: string(step.Op), Database: step.Database, Program: step.Program,
			Args: append([]string(nil), step.Args...), Stdin: append([]byte(nil), step.Stdin...),
		}
	}
	return operations, nil
}
