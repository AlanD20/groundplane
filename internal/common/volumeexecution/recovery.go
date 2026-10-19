package volumeexecution

import (
	"bytes"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

const MaximumRecoveryBytes = 8192

// Recovery captures the failed forward execution being accounted for.
// A successful cleanup does not replace this failure with a successful action.
type Recovery struct {
	Forward  workexecution.Attempt `json:"forward"`
	Failure  Failure               `json:"failure"`
	Evidence workexecution.Input   `json:"evidence"`
}

func (source Recovery) Validate() error {
	forward := source.Forward
	if forward.Validate() != nil || forward.Direction != workexecution.Forward || forward.FinishedAt == nil ||
		forward.Result == nil || forward.Outcome == workexecution.Succeeded || *forward.Result != source.Evidence ||
		!source.Failure.RecordedAt.Equal(*forward.FinishedAt) {
		return invalidProgress()
	}
	_, evidence, err := EncodeFailure(forward, source.Failure)
	if err != nil || evidence != source.Evidence {
		return invalidProgress()
	}
	return nil
}

func (source Recovery) ValidateFor(attempt workexecution.Attempt) error {
	if err := source.Validate(); err != nil {
		return err
	}
	if attempt.Validate() != nil || attempt.Direction != workexecution.Recovery ||
		attempt.TaskID != source.Forward.TaskID ||
		attempt.WorkItemID != source.Forward.WorkItemID ||
		attempt.Input != source.Forward.Input ||
		attempt.Number <= source.Forward.Number ||
		attempt.Executor.HostID != source.Forward.Executor.HostID {
		return invalidProgress()
	}
	return nil
}

func EncodeRecovery(source Recovery) ([]byte, error) {
	if err := source.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(source)
	if err != nil {
		return nil, invalidProgress()
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return nil, err
	}
	if len(canonical) > MaximumRecoveryBytes {
		return nil, invalidProgress()
	}
	return canonical, nil
}

func DecodeRecovery(raw []byte) (Recovery, error) {
	if len(raw) == 0 || len(raw) > MaximumRecoveryBytes {
		return Recovery{}, invalidProgress()
	}
	source, err := jcs.Decode[Recovery](raw)
	if err != nil {
		return Recovery{}, invalidProgress()
	}
	expected, err := EncodeRecovery(source)
	if err != nil || !bytes.Equal(expected, raw) {
		return Recovery{}, invalidProgress()
	}
	return source, nil
}
