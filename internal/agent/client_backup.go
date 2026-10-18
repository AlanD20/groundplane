package agent

import (
	"github.com/AlanD20/groundplane/internal/agent/backupruntime"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (client *Client) SetBackupExecutor(executor *backupruntime.Executor) error {
	if client == nil || executor == nil {
		return errs.New(errs.KindValidationFailed, "Agent Backup executor is required")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.started || client.backupExecutor != nil {
		return errs.New(errs.KindStateConflict, "Agent Backup executor cannot change after configuration")
	}
	client.backupExecutor = executor
	return nil
}
