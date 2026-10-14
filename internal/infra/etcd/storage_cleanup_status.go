package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/storagemaintenance"
)

// RecordCleanup retains one safe summary, never provider errors or Task output.
// It shares the maintenance record owner rather than writing another journal.
func (maintenance *StorageMaintenance) RecordCleanup(
	ctx context.Context,
	result storagemaintenance.CleanupStatus,
) error {
	maintenance.mu.Lock()
	defer maintenance.mu.Unlock()
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	read, err := maintenance.store.Get(bounded, storageMaintenanceKey)
	if err != nil {
		return err
	}
	if read.Entry != nil {
		record, err := decodeMaintenanceRecord(read.Entry.Value)
		if err != nil {
			return err
		}
		maintenance.record = record
	}
	maintenance.record.Status.Cleanup = &result
	if maintenance.record.Status.State == "" {
		maintenance.record.Status.State = "waiting_for_first_pass"
	}
	return maintenance.persist(bounded)
}
