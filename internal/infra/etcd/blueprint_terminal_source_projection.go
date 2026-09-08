package etcd

import (
	"context"
	"math"
	"time"

	ref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
)

// blueprintTerminalSourceAdvance retains the fixed-revision source authority
// while the caller composes and validates the entire terminal transaction.
type blueprintTerminalSourceAdvance struct {
	task                 TaskRecord
	taskValue            *KeyValue
	assignment           TaskAssignmentRecord
	assignmentValue      *KeyValue
	assignmentIndexValue *KeyValue
	recovery             releaseRecoveryAcknowledgement
	terminalStatus       TaskStatus
	terminalAt           time.Time
	revision             int64
	submittedStatus      TaskStatus
	submittedResult      TaskResultRecord
}

func (advance *blueprintTerminalSourceAdvance) execute(ctx context.Context, repository *TaskRepository) error {
	change, _, err := repository.prepareTerminalScriptSourceRelease(ctx,
		advance.task, advance.taskValue, advance.assignment, advance.assignmentValue, advance.assignmentIndexValue,
		advance.recovery, advance.terminalStatus, &advance.terminalAt, advance.revision,
		advance.submittedStatus, advance.submittedResult, true)
	change.clear()
	return err
}

// projection reserves the widest possible positive revision encoding for keys
// changed by release. Prefix-absence guards remain zero. The enclosing envelope
// is budget-only and cannot be passed to a terminal commit.
func (advance *blueprintTerminalSourceAdvance) projection(
	executionGuards []Condition,
) scriptTerminalSourceRelease {
	rootKey, reportKey := scriptSourceRootKey(advance.task.OperationID), blueprintClosingReportKey(advance.task.ID)
	conditions := []Condition{
		{Key: rootKey, ModRevision: math.MaxInt64},
		{Key: ref.ReversePrefix(advance.task.OperationID), Prefix: true},
		{Key: reportKey, ModRevision: math.MaxInt64},
	}
	for _, condition := range executionGuards {
		conditions = append(conditions, Condition{Key: condition.Key, ModRevision: math.MaxInt64})
	}
	return scriptTerminalSourceRelease{
		conditions: conditions,
		mutations:  []Mutation{{Type: MutationDelete, Key: rootKey}, {Type: MutationDelete, Key: reportKey}},
		advance:    advance,
	}
}
