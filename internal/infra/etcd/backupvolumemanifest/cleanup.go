package backupvolumemanifest

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ValidateCleanupEntry proves that an individual key belongs to the retained
// assignment before its deletion. A prefix match alone is not ownership proof.
func ValidateCleanupEntry(owner Owner, key string, value []byte) error {
	if owner.Validate() != nil || !strings.HasPrefix(key, Prefix(owner.Binding)) {
		return invalidLedger()
	}
	switch {
	case key == CursorKey(owner.Binding):
		cursor, err := decodeCursor(value)
		if err != nil || cursor.Owner != owner || !cursor.Complete {
			return invalidLedger()
		}
	case strings.HasPrefix(key, Prefix(owner.Binding)+"frame/"):
		stored, frame, err := decodeFrame(value)
		if err != nil || stored != owner || key != FrameKey(owner.Binding, frame.RecordSequence) {
			return invalidLedger()
		}
	case strings.HasPrefix(key, Prefix(owner.Binding)+"credit/"):
		var credit agentpb.BackupVolumeManifestAckCredit
		if proto.Unmarshal(value, &credit) != nil ||
			backupvolumetransfer.ValidateCredit(owner.Binding, &credit) != nil ||
			key != CreditKey(owner.Binding, credit.AckSequence) {
			return invalidLedger()
		}
	default:
		return invalidLedger()
	}
	return nil
}
