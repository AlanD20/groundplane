package agentworkjournal

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// EffectRecord retains one action's exact evidence until its Controller receipt
// and subsequent cursor confirmation. It is not an accumulating event log.
type EffectRecord struct {
	Boundary     releaseexecution.CheckpointReport `json:"boundary"`
	Reference    workexecution.Input               `json:"reference"`
	Raw          []byte                            `json:"raw"`
	Acknowledged bool                              `json:"acknowledged"`
}

func (record EffectRecord) validate(attempt workexecution.Attempt) error {
	if record.Boundary.ValidateEnvelope(attempt) != nil || record.Boundary.Ack == nil || !record.Boundary.Progress.InFlight ||
		record.Reference.Validate() != nil || len(record.Raw) == 0 || len(record.Raw) > releaseexecution.MaximumCheckpointBytes {
		return invalid()
	}
	digest := sha256.Sum256(record.Raw)
	if hex.EncodeToString(digest[:]) != record.Reference.SHA256 {
		return invalid()
	}
	return nil
}
