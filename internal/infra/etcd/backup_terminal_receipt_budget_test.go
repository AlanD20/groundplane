package etcd

import (
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	strconv "strconv"
	testing "testing"
	time "time"
)

func TestBackupTerminalReceiptWorstPruneRecordFitsDurableBound(t *testing.T) {

	t.Parallel()
	terminal, dispatch, assigned, _ := backupTerminalReceiptPruneFixture(t)
	dispatch.RecoveryPointIDs = make([]string, testbackupruntime.MaximumBackupPruneDispatchPoints)
	prunes := make(
		[]testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord],
		len(dispatch.RecoveryPointIDs),
	)
	for index := range prunes {
		point := assigned.Record.Point
		point.ID = ids.NewAt(
			ids.KindRecoveryPoint,
			assigned.Record.Point.CreatedAt.Add(time.Duration(index)*time.Millisecond),
			int64(7600+index),
		)
		point.CreatedAt = assigned.Record.Point.CreatedAt.Add(time.Duration(index) * time.Millisecond)
		point.ObjectKey = "production/" + point.EnvironmentID + "/" + point.SourceID + "/" +
			point.ID + "/artifact.bin"
		dispatch.RecoveryPointIDs[index] = point.ID
		prune := assigned.Record
		prune.Point = point
		prunes[index] = testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{
			Record: prune, Revision: int64(index + 1),
		}
	}
	plan, err := prepareBackupPruneTerminalReceipt(
		testkeyvalue.Versioned[TaskRecord]{Record: terminal, Revision: 19},
		terminal,
		dispatch,
		prunes,
	)
	if err != nil {
		t.Fatalf("prepareBackupPruneTerminalReceipt() error = %v", err)
	}
	defer plan.clear()
	if len(plan.conditions) != 0 {
		t.Fatalf("worst terminal receipt conditions = %d, want 0", len(plan.conditions))
	}
	const recordLimit = 256 * 1024
	if size := len(plan.mutations[0].Value); size > recordLimit {
		t.Fatalf("worst terminal receipt size = %d, limit %d", size, recordLimit)
	}
}

func TestBackupTerminalReceiptWorstPruneCompositionUsesExactlyNinetySixOperations(t *testing.T) {

	t.Parallel()
	_, _, _, receiptPlan := backupTerminalReceiptPruneFixture(t)
	defer receiptPlan.clear()
	taskPlan := backupTaskTerminalPlan{}
	for index := range 9 {
		taskPlan.conditions = append(taskPlan.conditions, testkeyvalue.Condition{
			Key: "/test/terminal-task-condition/" + strconv.Itoa(index), ModRevision: 1,
		})
	}
	for index := range 8 {
		taskPlan.mutations = append(taskPlan.mutations, testkeyvalue.Mutation{
			Type: testkeyvalue.MutationDelete, Key: "/test/terminal-task-mutation/" + strconv.Itoa(index),
		})
	}
	prunePlan := backupPruneTransactionPlan{}
	for index := range 64 {
		key := "/test/terminal-prune-condition/" + strconv.Itoa(index)
		if index == 0 {
			key = testhierarchy.EnvironmentMutationEpochKey(receiptPlan.record.Task.Owner.EnvironmentID)
		}
		prunePlan.conditions = append(prunePlan.conditions, testkeyvalue.Condition{Key: key, ModRevision: 18})
	}
	for index := range 14 {
		prunePlan.mutations = append(prunePlan.mutations, testkeyvalue.Mutation{
			Type: testkeyvalue.MutationDelete, Key: "/test/terminal-prune-mutation/" + strconv.Itoa(index),
		})
	}
	conditions, mutations, err := composeBackupPruneTerminalTransaction(
		taskPlan,
		prunePlan,
		receiptPlan,
	)
	defer testkeyvalue.ClearMutationValues(mutations)
	if err != nil {
		t.Fatalf("composeBackupPruneTerminalTransaction() error = %v", err)
	}
	if operations := len(conditions) + len(mutations); operations != testkeyvalue.MaximumOperations {
		t.Fatalf("worst terminal prune operations = %d, want %d", operations, testkeyvalue.MaximumOperations)
	}
}
