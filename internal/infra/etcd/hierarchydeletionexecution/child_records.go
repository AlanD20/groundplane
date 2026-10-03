package hierarchydeletionexecution

import (
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func HierarchyDeletionChildMatches(
	entry hierarchydeletion.HierarchyDeletionChildEntry,
	operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
) bool {
	return entry.Schema == 1 && entry.ParentOperationID == operation.Tombstone.OperationID &&
		entry.ChildOperationID == action.AgentProcedure.ChildOperationID &&
		entry.RetryInputDigest == action.AgentProcedure.InputDigest && entry.CheckpointDigest != "" &&
		entry.CurrentAttemptID != "" && entry.CurrentTaskID != ""
}

func DecodeHierarchyDeletionChildEntry(value []byte) (hierarchydeletion.HierarchyDeletionChildEntry, error) {
	var entry hierarchydeletion.HierarchyDeletionChildEntry
	if hierarchydeletion.DecodeHierarchyDeletionRecord(
		value,
		hierarchydeletion.HierarchyDeletionLargeRecordBytes,
		&entry,
	) != nil ||
		entry.Schema != 1 {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return entry, nil
}

func HierarchyDeletionChildStableID(kind ids.Kind, values ...string) string {
	return string(
		kind,
	) + "_" + hierarchyDeletionStableULID(
		"gp-deletion-stable-id-v1",
		append([]string{string(kind)}, values...)...)
}

func HierarchyDeletionStableRawID(prefix string, values ...string) string {
	return prefix + "_" + hierarchyDeletionStableULID(
		"gp-deletion-stable-raw-id-v1",
		append([]string{prefix}, values...)...)
}

func hierarchyDeletionStableULID(domain string, values ...string) string {
	operation := hierarchydeletion.HierarchyDeletionStableOperationID(append([]string{domain}, values...)...)
	return operation[len(string(ids.KindOperation))+1:]
}

func HierarchyDeletionTaskResultDigest(
	result *taskjournal.TaskResultRecord,
	terminal taskjournal.TaskStatus,
) (string, string, error) {
	if result == nil {
		return "", "", errs.New(errs.KindInternal, "hierarchy deletion child Task lost its result")
	}
	value, err := json.Marshal(result)
	if err != nil {
		return "", "", errs.Wrap(errs.KindInternal, err)
	}
	defer clear(value)
	digest := hierarchydeletion.HierarchyDeletionBytesDigest(value)
	if terminal == taskjournal.TaskStatusCompleted {
		return digest, "", nil
	}
	return "", hierarchydeletion.HierarchyDeletionFoldDigest(
		"gp-deletion-child-error-v1",
		string(terminal),
		digest,
	), nil
}

func HierarchyDeletionAgentTerminalFromTask(
	status taskjournal.TaskStatus,
) (hierarchydeletion.HierarchyDeletionAgentTerminal, error) {
	switch status {
	case taskjournal.TaskStatusCompleted:
		return hierarchydeletion.HierarchyDeletionAgentCompleted, nil
	case taskjournal.TaskStatusFailed:
		return hierarchydeletion.HierarchyDeletionAgentFailed, nil
	case taskjournal.TaskStatusAborted:
		return hierarchydeletion.HierarchyDeletionAgentAborted, nil
	case taskjournal.TaskStatusTimedOut:
		return hierarchydeletion.HierarchyDeletionAgentTimedOut, nil
	default:
		return "", errs.Newf(errs.KindStateConflict, "hierarchy deletion child Task is %s", status)
	}
}
