package agent

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// WorkerOutput is a closed ordered union. Exactly one member is non-nil, and
// each Task's terminal step progress is emitted before its final result.
type WorkerOutput struct {
	Error                 error
	Progress              *TaskProgress
	Result                *TaskResult
	BackupConfigCredit    *agentpb.BackupConfigCredit
	ScriptCheckpoint      *agentpb.ScriptCheckpointRequest
	BackingHookCheckpoint *agentpb.BackingHookCheckpointRequest
}

func (output WorkerOutput) validate() error {
	var count int
	for _, present := range []bool{output.Error != nil, output.Progress != nil, output.Result != nil,
		output.BackupConfigCredit != nil,
		output.ScriptCheckpoint != nil,
		output.BackingHookCheckpoint != nil} {
		if present {
			count++
		}
	}
	if count != 1 {
		return errs.New(errs.KindInternal, "agent: worker returned an invalid output union")
	}
	return nil
}
