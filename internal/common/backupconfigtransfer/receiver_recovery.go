package backupconfigtransfer

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// RecoveryRecordSource reads immutable acknowledged descriptors from the
// Agent's private journal. Value plaintext is read separately from staging.
type RecoveryRecordSource interface {
	ReadRecord(context.Context, uint64) (*RecordDescriptor, error)
}

// RecoverReceiver reconstructs validation/hash state up to the native durable
// credit cursor. Exact credit history is required; gaps or changed source bytes
// do not authorize a fresh transfer. destination must append to the same owned
// partial inode, while prefix is a fixed-size reader over its retained bytes.
func RecoverReceiver(
	ctx context.Context,
	binding Binding,
	expected *agentpb.BackupConfigContentAuthority,
	credits []*agentpb.BackupConfigCredit,
	records RecoveryRecordSource,
	prefix io.ReaderAt,
	prefixEvidence backupconfig.ArtifactEvidence,
	destination io.Writer,
) (*Receiver, error) {
	if ctx == nil || !validContent(expected) || records == nil || len(credits) == 0 ||
		prefixEvidence.SizeBytes > expected.SourceSizeBytes {
		return nil, invalid("Config recovery authority or retained history is missing")
	}
	ownedCredits := make([]*agentpb.BackupConfigCredit, len(credits))
	for index, credit := range credits {
		owned, err := ValidateCredit(binding, credit)
		if err != nil {
			return nil, err
		}
		if owned.CreditSequence != uint64(index+1) ||
			(index > 0 && owned.CommittedRecordSequence <= ownedCredits[index-1].CommittedRecordSequence) {
			return nil, invalid("Config recovery credit history is not contiguous")
		}
		ownedCredits[index] = owned
	}
	replay, err := newPrefixReplayWriter(ctx, prefix, prefixEvidence, destination)
	if err != nil {
		return nil, err
	}
	receiver, err := NewReceiver(binding, expected, replay, ownedCredits[0])
	if err != nil {
		return nil, err
	}
	receiver.retained = replay
	latest := ownedCredits[len(ownedCredits)-1]
	creditIndex := 1
	for sequence := uint64(1); sequence <= latest.CommittedRecordSequence; sequence++ {
		descriptor, err := records.ReadRecord(ctx, sequence)
		if err != nil {
			receiver.Abort()
			return nil, err
		}
		frame, err := RestoreRecord(ctx, binding, descriptor, prefix, receiver.layout)
		if err != nil {
			receiver.Abort()
			return nil, err
		}
		if frame.RecordSequence != sequence {
			ClearFrame(frame)
			receiver.Abort()
			return nil, invalid("Config replay record sequence changed")
		}
		err = receiver.AcceptFrame(ctx, frame)
		ClearFrame(frame)
		if err != nil {
			receiver.Abort()
			return nil, err
		}
		for creditIndex < len(ownedCredits) && ownedCredits[creditIndex].CommittedRecordSequence == sequence {
			if err := receiver.AcceptPersistedCredit(ownedCredits[creditIndex]); err != nil {
				receiver.Abort()
				return nil, err
			}
			creditIndex++
		}
	}
	if creditIndex != len(ownedCredits) ||
		receiver.window.Status().Committed.RecordSequence != latest.CommittedRecordSequence ||
		receiver.progress.Cursor.NextOrdinal != latest.NextOrdinal ||
		!sameDigest(receiver.progress.Cursor.ChainSHA256[:], latest.CumulativeChainSha256) {
		receiver.Abort()
		return nil, invalid("Config replay does not reproduce the durable native cursor")
	}
	return receiver, nil
}
