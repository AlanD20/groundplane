package backupruntime

import ()

func ChangedBackupSourceOrdinal(current BackupRunRecord, next BackupRunRecord) (uint32, bool) {
	if len(current.Sources) != len(next.Sources) {
		return 0, false
	}
	changed := -1
	for index := range current.Sources {
		if backupRunSourceMutableEqual(current.Sources[index], next.Sources[index]) {
			continue
		}
		if changed >= 0 {
			if TerminalBackupRunState(next.State) && index > changed &&
				current.Sources[index].State == BackupSourceAttemptPending &&
				next.Sources[index].State == BackupSourceAttemptUnstarted &&
				current.Sources[index].Evidence == next.Sources[index].Evidence &&
				current.Sources[index].ConfigArchive == next.Sources[index].ConfigArchive &&
				current.Sources[index].VolumeArchive == next.Sources[index].VolumeArchive &&
				current.Sources[index].Upload == next.Sources[index].Upload &&
				current.Sources[index].Object == next.Sources[index].Object &&
				current.Sources[index].FailureCode == "" && next.Sources[index].FailureCode == "" {
				continue
			}
			return 0, false
		}
		changed = index
	}
	if changed < 0 {
		return 0, false
	}
	return uint32(changed), true
}
