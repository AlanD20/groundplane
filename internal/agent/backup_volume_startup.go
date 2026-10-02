package agent

import (
	"bytes"
	"context"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/agentvolumejournal"
	"github.com/AlanD20/groundplane/internal/infra/agentvolumemanifest"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type recoveredVolumeRestore struct {
	journal  *agentvolumejournal.Recovered
	receiver *agentvolumemanifest.Recovered
	wire     *agentpb.BackupRecoveredVolumeRestore
}

func inventoryBackupVolumeRestores(ctx context.Context) ([]*agentpb.BackupRecoveredVolumeRestore,
	map[string]recoveredVolumeRestore, error,
) {
	if err := agentvolumejournal.ReapEmptyRetired(ctx, agentvolumejournal.AgentRoot); err != nil {
		return nil, nil, err
	}
	if err := agentvolumemanifest.ReapEmptyRetired(ctx, agentvolumemanifest.AgentRoot); err != nil {
		return nil, nil, err
	}
	journalRows, err := agentvolumejournal.Inventory(ctx, agentvolumejournal.AgentRoot)
	if err != nil {
		return nil, nil, err
	}
	receiverRows, err := agentvolumemanifest.Inventory(ctx, agentvolumemanifest.AgentRoot)
	if err != nil {
		return nil, nil, err
	}
	if len(journalRows)+len(receiverRows) > 2*executionplan.MaximumBackupRecoveredStages {
		return nil, nil, invalidAgentStaging()
	}
	local := make(map[string]recoveredVolumeRestore, len(journalRows)+len(receiverRows))
	for index := range journalRows {
		row := journalRows[index]
		if row.Unbound || row.Config.JournalRoot != agentvolumejournal.AgentRoot ||
			row.Config.RestoreGenerationID == "" {
			return nil, nil, invalidAgentStaging()
		}
		key, err := executionplan.BackupStagingRecoveryKey(row.Config.TaskID, row.Config.StepID, row.Config.PointID)
		if err != nil {
			return nil, nil, err
		}
		name := string(key)
		if _, exists := local[name]; exists {
			return nil, nil, invalidAgentStaging()
		}
		wire := &agentpb.BackupRecoveredVolumeRestore{RecoveryKeySha256: key,
			AssignmentId: row.Config.AssignmentID, RestoreGenerationId: row.Config.RestoreGenerationID,
			StepSha256: append([]byte(nil), row.Config.StepSHA256[:]...), JournalPresent: true,
			OldManifestSha256: append([]byte(nil), row.Config.OldManifestSHA256[:]...),
			OldFullTreeSha256: append([]byte(nil), row.Config.OldFullTreeSHA256[:]...),
			NewManifestSha256: append([]byte(nil), row.Config.NewManifestSHA256[:]...),
			NewFullTreeSha256: append([]byte(nil), row.Config.NewFullTreeSHA256[:]...),
			OldEntryCount:     row.Config.OldEntryCount, NewEntryCount: row.Config.NewEntryCount,
			JournalRecordCount: row.State.RecordCount,
			JournalChainSha256: append([]byte(nil), row.State.ChainSHA256[:]...),
			JournalPending:     row.State.Pending != nil, JournalRootDeleted: row.State.RootDeleted,
			JournalPartial: row.State.UncommittedPrefix, JournalRetiring: row.Retiring,
			JournalDiscarding: row.Discarding}
		local[name] = recoveredVolumeRestore{journal: &row, wire: wire}
	}
	for index := range receiverRows {
		row := receiverRows[index]
		if row.Unbound || row.Config.Root != agentvolumemanifest.AgentRoot ||
			row.Config.RestoreGenerationID == "" || row.CurrentCredit == nil && !row.Retiring {
			return nil, nil, invalidAgentStaging()
		}
		binding := row.Config.Binding
		transferID, err := backupvolumetransfer.TransferID(row.Config.RestoreGenerationID,
			agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW)
		if err != nil || binding.TransferID != transferID {
			return nil, nil, invalidAgentStaging()
		}
		key, err := executionplan.BackupStagingRecoveryKey(binding.TaskID, binding.StepID, row.Config.PointID)
		if err != nil {
			return nil, nil, err
		}
		name := string(key)
		entry := local[name]
		if entry.receiver != nil || entry.wire != nil &&
			(entry.wire.AssignmentId != binding.AssignmentID ||
				entry.wire.RestoreGenerationId != row.Config.RestoreGenerationID ||
				!bytes.Equal(entry.wire.StepSha256, binding.AuthorityDigest[:]) ||
				!bytes.Equal(entry.wire.NewManifestSha256, row.Config.ExpectedArchive.ContentManifestSHA256[:]) ||
				!bytes.Equal(entry.wire.NewFullTreeSha256, row.Config.ExpectedArchive.FullTreeSHA256[:]) ||
				entry.wire.NewEntryCount != row.Config.ExpectedArchive.EntryCount) {
			return nil, nil, invalidAgentStaging()
		}
		if entry.wire == nil {
			entry.wire = &agentpb.BackupRecoveredVolumeRestore{RecoveryKeySha256: key,
				AssignmentId: binding.AssignmentID, RestoreGenerationId: row.Config.RestoreGenerationID,
				StepSha256: append([]byte(nil), binding.AuthorityDigest[:]...)}
		}
		entry.wire.ReceiverPresent = true
		if row.CurrentCredit != nil {
			entry.wire.ReceiverCommittedRecordSequence = row.CurrentCredit.CommittedRecordSequence
		}
		entry.wire.ReceiverComplete = row.Complete
		entry.wire.ReceiverPartial = row.Partial
		entry.wire.ReceiverRetiring = row.Retiring
		entry.wire.ReceiverDiscarding = row.Discarding
		entry.wire.ReceiverContentManifestSha256 = append(
			[]byte(nil),
			row.Config.ExpectedArchive.ContentManifestSHA256[:]...)
		entry.wire.ReceiverFullTreeSha256 = append([]byte(nil), row.Config.ExpectedArchive.FullTreeSHA256[:]...)
		entry.wire.ReceiverSourceSha256 = append([]byte(nil), row.Config.ExpectedSource.SHA256[:]...)
		entry.wire.ReceiverEntryCount = row.Config.ExpectedArchive.EntryCount
		entry.receiver = &row
		local[name] = entry
	}
	if len(local) > executionplan.MaximumBackupRecoveredStages {
		return nil, nil, invalidAgentStaging()
	}
	wire := make([]*agentpb.BackupRecoveredVolumeRestore, 0, len(local))
	for _, entry := range local {
		wire = append(wire, entry.wire)
	}
	sort.Slice(wire, func(i, j int) bool {
		return bytes.Compare(wire[i].RecoveryKeySha256, wire[j].RecoveryKeySha256) < 0
	})
	return wire, local, nil
}
