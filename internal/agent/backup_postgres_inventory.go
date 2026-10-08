package agent

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/agentpostgresjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func databaseExecutionIDs(taskID string, step *agentpb.BackupStepAuthority) agentpostgresjournal.IDs {
	pointID := step.GetCapture().GetPointId()
	if pointID == "" {
		pointID = step.GetRestore().GetPointId()
	}
	return agentpostgresjournal.IDs{Task: taskID, Step: step.GetStepId(), Point: pointID}
}

func inventoryDatabaseExecutions(ctx context.Context, inventory *agentpb.BackupStagingInventory) error {
	markers, err := agentpostgresjournal.Inventory(ctx)
	if err != nil {
		return err
	}
	entries := make(map[string]*agentpb.BackupRecoveredStage, len(inventory.Entries))
	for _, entry := range inventory.Entries {
		entries[hex.EncodeToString(entry.RecoveryKeySha256)] = entry
	}
	for _, marker := range markers {
		key, err := executionplan.BackupStagingRecoveryKey(marker.Task, marker.Step, marker.Point)
		if err != nil {
			return err
		}
		entry := entries[hex.EncodeToString(key)]
		if entry == nil {
			entry = &agentpb.BackupRecoveredStage{RecoveryKeySha256: key}
			inventory.Entries = append(inventory.Entries, entry)
		}
		entry.DatabaseExecution = true
	}
	return nil
}

func retireDatabaseExecutionMarker(ctx context.Context, disposition *agentpb.BackupStagingDisposition) error {
	guard := disposition.DatabaseGuard
	if guard == nil || !guard.TerminalCleanup {
		return nil
	}
	return agentpostgresjournal.Remove(ctx, databaseExecutionIDs(guard.TaskId, guard.Step))
}
