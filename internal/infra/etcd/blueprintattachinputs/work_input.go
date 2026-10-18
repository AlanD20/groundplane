package blueprintattachinputs

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// WorkInput identifies the complete immutable generation, including encrypted
// inputs and retained Secret pins. Retries refer to the same bytes and identity.
func WorkInput(record Generation) (workexecution.Input, error) {
	value, err := Encode(record)
	if err != nil {
		return workexecution.Input{}, err
	}
	defer clear(value)
	at, err := ids.Timestamp(ids.KindOperation, record.OperationID)
	if err != nil {
		return workexecution.Input{}, err
	}
	digest := sha256.Sum256(value)
	return workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, at, record.WorkItemID, "attach-input/"+record.OperationID),
		SHA256: hex.EncodeToString(digest[:]),
	}, nil
}
