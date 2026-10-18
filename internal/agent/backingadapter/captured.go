package backingadapter

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/adapterprocedure"
	"github.com/AlanD20/groundplane/internal/infra/docker/backingcommand"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ExecuteCapturedProcedure runs a freshly authorized procedure. Reconnection
// inspects its recorded execs elsewhere and must never call this method again.
func (runtime *Runtime) ExecuteCapturedProcedure(ctx context.Context, procedure *agentpb.AdapterProcedure,
	containerID string, executor *backingcommand.Executor, recorder backingcommand.Recorder,
) error {
	if runtime == nil || runtime.compiler == nil || executor == nil || recorder == nil {
		return errs.New(errs.KindInternal, "captured adapter execution is not configured")
	}
	if err := adapterprocedure.Validate(procedure); err != nil {
		return err
	}
	compiled, err := runtime.compiler(procedure)
	defer clearSteps(compiled)
	if err != nil {
		return err
	}
	if len(compiled) == 0 || len(compiled) > maximumCompiledAdapterSteps {
		return errs.New(errs.KindValidationFailed, "compiled adapter procedure is empty or oversized")
	}
	for _, operation := range compiled {
		if err := runtime.VerifyBackingContainer(ctx, containerID, procedure.BackingServiceId); err != nil {
			return err
		}
		program, args, input, err := adapterCommand(operation)
		if err != nil {
			return err
		}
		exitCode, err := executor.Execute(ctx, recorder, containerID, append([]string{program}, args...), nil, input, io.Discard)
		if err != nil {
			return err
		}
		if exitCode != 0 {
			return errs.New(errs.KindStateConflict, "Attach credential command failed")
		}
	}
	return nil
}
