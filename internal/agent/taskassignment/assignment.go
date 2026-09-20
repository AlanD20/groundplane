package taskassignment

import (
	"github.com/AlanD20/groundplane/proto/agentpb"
	"time"
)

type Assignment struct {
	AssignmentID                string
	TaskID                      string
	OperationID                 string
	RetryOf                     string
	Plan                        *agentpb.ExecutionPlan
	ScriptArtifacts             *agentpb.ScriptAssignmentArtifacts
	ScriptCheckpoints           []*agentpb.ScriptExecutionCheckpoint
	AutomaticReconcile          bool
	ExecutionEpoch              uint32
	ExecutionMode               agentpb.TaskExecutionMode
	ForwardDeadline             time.Time
	RecoveryDeadline            time.Time
	Deadline                    time.Time
	RecoveryProofRequired       bool
	RestorationAuthority        *agentpb.ReleaseRestorationAuthority
	ReleaseRecoveryDirective    *agentpb.ReleaseRecoveryDirective
	ReleaseRecoveryRecordSHA256 []byte
}
