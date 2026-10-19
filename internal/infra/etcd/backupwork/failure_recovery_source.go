package backupwork

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// retainFailedExecution freezes the exact failed attempt and checkpoint for
// subsequent cleanup. It is committed with failure settlement, not reconstructed
// from a later resource snapshot or the diagnostic journal.
func retainFailedExecution(change *workqueue.Change, item workexecution.Item, attempt workexecution.Attempt,
	result workexecution.Completion, raw []byte, at time.Time,
) error {
	_, terminal, err := workexecution.Finish(item, attempt, result, at)
	if err != nil {
		return err
	}
	failure, err := executionplan.DecodeBackupWorkFailure(terminal, raw, result.Evidence)
	if err != nil {
		return err
	}
	recovery, err := executionplan.EncodeBackupWorkRecovery(executionplan.BackupWorkRecovery{
		Forward: terminal, Failure: failure, Evidence: result.Evidence,
	})
	if err != nil {
		return err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: RecoveryKey(item.ID)})
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: RecoveryKey(item.ID), Value: recovery},
	)
	return nil
}
