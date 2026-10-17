package attachremoval

import (
	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// WorkResult retains only the final successful command boundary and authority,
// independently of collected attempt history. No credential or hook bytes.
type WorkResult struct {
	Attempt   workexecution.Attempt            `json:"attempt"`
	Authority attachexecution.RemovalAuthority `json:"authority"`
	Report    attachexecution.RemovalReport    `json:"report"`
}

func EncodeWorkResult(result WorkResult) ([]byte, error) {
	if result.Attempt.StartedAt == nil || result.Attempt.FinishedAt != nil || result.Report.Progress.InFlight ||
		result.Report.Progress.Completed != result.Authority.Commands {
		return nil, recordcodec.CorruptRecord()
	}
	if _, err := result.Report.CheckpointAuthority(result.Attempt, result.Authority); err != nil {
		return nil, err
	}
	return recordcodec.Encode("attach-removal-work-result", result)
}

func DecodeWorkResult(raw []byte) (WorkResult, error) {
	if len(raw) > 8192 {
		return WorkResult{}, recordcodec.CorruptRecord()
	}
	result, err := recordcodec.Decode[WorkResult](raw, "attach-removal-work-result")
	if err != nil {
		return WorkResult{}, err
	}
	if _, err := EncodeWorkResult(result); err != nil {
		return WorkResult{}, err
	}
	return result, nil
}
