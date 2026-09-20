package backup

import (
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BackupRunPlanBuilder seals a task/run pair without adding caller-controlled
// source selection or any plaintext materialization.
type BackupRunPlanBuilder interface {
	BuildBackupRunPlan(BackupRunPlanInput) (*agentpb.ExecutionPlan, error)
}

// BackupRunPlanBuilderFunc adapts the package-level Controller builder to the
// application service.
type BackupRunPlanBuilderFunc func(BackupRunPlanInput) (*agentpb.ExecutionPlan, error)

func (builder BackupRunPlanBuilderFunc) BuildBackupRunPlan(
	input BackupRunPlanInput,
) (*agentpb.ExecutionPlan, error) {
	return builder(input)
}
