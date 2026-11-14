package transport

import (
	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (runtime *Runtime) ConfigureCheckpointDiagnostics(diagnostics agentchannel.CheckpointDiagnostics) error {
	if runtime == nil || diagnostics == nil || runtime.checkpointDiagnostics != nil {
		return errs.New(errs.KindInternal, "checkpoint refusal diagnostics configuration is invalid")
	}
	runtime.checkpointDiagnostics = diagnostics
	return nil
}
