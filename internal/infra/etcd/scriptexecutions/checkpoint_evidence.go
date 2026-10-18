package scriptexecutions

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/ids"

	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Evidence has its own immutable lifetime, separate from the mutable latest
// checkpoint. Its identity includes the attempt and the shared cursor sequence.
func CheckpointEvidenceKey(executionID, attemptID string, sequence uint64) string {
	return ScriptExecutionKey(executionID) + "/checkpoints/" + attemptID + fmt.Sprintf("/%020d", sequence)
}

func CheckpointEvidenceID(taskID, executionID, attemptID string, sequence uint64) (string, error) {
	at, err := ids.Timestamp(ids.KindTask, taskID)
	if err != nil {
		return "", err
	}
	return ids.DeriveAt(ids.KindPlan, at, taskID, CheckpointEvidenceKey(executionID, attemptID, sequence)), nil
}

func EncodeCheckpointEvidence(input ScriptCheckpointInput) ([]byte, string, error) {
	input.PayloadSHA256 = ""
	value, err := recordcodec.Encode("script-checkpoint-evidence", input)
	if err != nil {
		return nil, "", err
	}
	if len(value) > 16<<10 {
		clear(value)
		return nil, "", errs.New(errs.KindValidationFailed, "Script checkpoint evidence exceeds its size limit")
	}
	digest := sha256.Sum256(value)
	return value, hex.EncodeToString(digest[:]), nil
}
