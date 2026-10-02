package backupconfigtransfer

import (
	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// MaximumTransferRecords follows the canonical metadata-first chunk geometry.
// It is an upper bound, not a count reported by the sender.
func MaximumTransferRecords(expected *agentpb.BackupConfigContentAuthority) (uint64, error) {
	if !validContent(expected) {
		return 0, invalid("Config journal capacity requires sealed content")
	}
	entries, values := uint64(expected.EntryCount), expected.TotalSelectedValueBytes
	active := min(entries, values)
	chunks := active + (values-active)/backupconfig.TransferChunkBytes
	maximumEntryChunks := (uint64(backupconfig.MaxSelectedValueBytes) + backupconfig.TransferChunkBytes - 1) /
		backupconfig.TransferChunkBytes
	chunks = min(chunks, entries*maximumEntryChunks)
	return 2 + 2*entries + chunks, nil
}

// JournalAllocationUnit is the conservative allocation bound verified by the
// journal's filesystem owner. Each record and each possible credit gets its
// own unit; authority, a pending immutable write and directories get four more.
const JournalAllocationUnit uint64 = 64 * 1024

// The journal pins the complete validated Step, including predecessor files.
// Leave room for its typed binding outside the already bounded execution plan.
const MaximumJournalAuthorityBytes = executionplan.MaximumPlanBytes + 1024

func JournalGrowthUpperBound(expected *agentpb.BackupConfigContentAuthority) (uint64, error) {
	records, err := MaximumTransferRecords(expected)
	if err != nil {
		return 0, err
	}
	authorityUnits := (uint64(MaximumJournalAuthorityBytes) + JournalAllocationUnit - 1) / JournalAllocationUnit
	return (2*records + 4 + 2*authorityUnits) * JournalAllocationUnit, nil
}
