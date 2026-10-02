package agent

import (
	"bytes"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// After a durably recorded cleanup plan, each private namespace may be gone
// or in its tagged retiring state. Any still-active component must retain its
// original exact evidence; a retiring component must retain sealed identity.
func volumeInventoryCleanupSubset(actual, retained *agentpb.BackupRecoveredVolumeRestore) bool {
	if actual == nil || retained == nil ||
		!bytes.Equal(actual.RecoveryKeySha256, retained.RecoveryKeySha256) ||
		actual.AssignmentId != retained.AssignmentId ||
		actual.RestoreGenerationId != retained.RestoreGenerationId ||
		!bytes.Equal(actual.StepSha256, retained.StepSha256) ||
		actual.JournalPresent && !retained.JournalPresent ||
		actual.ReceiverPresent && !retained.ReceiverPresent {
		return false
	}
	if actual.JournalPresent {
		if !bytes.Equal(actual.OldManifestSha256, retained.OldManifestSha256) ||
			!bytes.Equal(actual.OldFullTreeSha256, retained.OldFullTreeSha256) ||
			!bytes.Equal(actual.NewManifestSha256, retained.NewManifestSha256) ||
			!bytes.Equal(actual.NewFullTreeSha256, retained.NewFullTreeSha256) ||
			actual.OldEntryCount != retained.OldEntryCount || actual.NewEntryCount != retained.NewEntryCount {
			return false
		}
		if !actual.JournalRetiring &&
			(actual.JournalRecordCount != retained.JournalRecordCount ||
				!bytes.Equal(actual.JournalChainSha256, retained.JournalChainSha256) ||
				actual.JournalPending != retained.JournalPending ||
				actual.JournalRootDeleted != retained.JournalRootDeleted ||
				actual.JournalPartial != retained.JournalPartial) {
			return false
		}
	}
	if actual.ReceiverPresent {
		if !bytes.Equal(actual.ReceiverContentManifestSha256, retained.ReceiverContentManifestSha256) ||
			!bytes.Equal(actual.ReceiverFullTreeSha256, retained.ReceiverFullTreeSha256) ||
			!bytes.Equal(actual.ReceiverSourceSha256, retained.ReceiverSourceSha256) ||
			actual.ReceiverEntryCount != retained.ReceiverEntryCount {
			return false
		}
		if !actual.ReceiverRetiring &&
			(actual.ReceiverCommittedRecordSequence != retained.ReceiverCommittedRecordSequence ||
				actual.ReceiverComplete != retained.ReceiverComplete ||
				actual.ReceiverPartial != retained.ReceiverPartial) {
			return false
		}
	}
	return true
}
