package hierarchyexecution

import (
	"bytes"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

const MaximumEnvironmentRecoveryBytes = 8192

// EnvironmentRecovery captures the failed forward execution being accounted for.
// A successful cleanup does not replace this failure with a successful action.
type EnvironmentRecovery struct {
	Forward  workexecution.Attempt `json:"forward"`
	Failure  EnvironmentFailure    `json:"failure"`
	Evidence workexecution.Input   `json:"evidence"`
}

func (source EnvironmentRecovery) Validate() error {
	forward := source.Forward
	if forward.Validate() != nil || forward.Direction != workexecution.Forward || forward.FinishedAt == nil ||
		forward.Result == nil || forward.Outcome == workexecution.Succeeded || *forward.Result != source.Evidence ||
		!source.Failure.RecordedAt.Equal(*forward.FinishedAt) {
		return invalidEnvironmentProgress()
	}
	_, evidence, err := EncodeEnvironmentFailure(forward, source.Failure)
	if err != nil || evidence != source.Evidence {
		return invalidEnvironmentProgress()
	}
	return nil
}

func (source EnvironmentRecovery) ValidateFor(attempt workexecution.Attempt) error {
	if err := source.Validate(); err != nil {
		return err
	}
	if attempt.Validate() != nil || attempt.Direction != workexecution.Recovery ||
		attempt.TaskID != source.Forward.TaskID ||
		attempt.WorkItemID != source.Forward.WorkItemID ||
		attempt.Input != source.Forward.Input ||
		attempt.Number <= source.Forward.Number ||
		attempt.Executor.HostID != source.Forward.Executor.HostID {
		return invalidEnvironmentProgress()
	}
	return nil
}

func EncodeEnvironmentRecovery(source EnvironmentRecovery) ([]byte, error) {
	if err := source.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(source)
	if err != nil {
		return nil, invalidEnvironmentProgress()
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return nil, err
	}
	if len(canonical) > MaximumEnvironmentRecoveryBytes {
		return nil, invalidEnvironmentProgress()
	}
	return canonical, nil
}

func DecodeEnvironmentRecovery(raw []byte) (EnvironmentRecovery, error) {
	if len(raw) == 0 || len(raw) > MaximumEnvironmentRecoveryBytes {
		return EnvironmentRecovery{}, invalidEnvironmentProgress()
	}
	source, err := jcs.Decode[EnvironmentRecovery](raw)
	if err != nil {
		return EnvironmentRecovery{}, invalidEnvironmentProgress()
	}
	expected, err := EncodeEnvironmentRecovery(source)
	if err != nil || !bytes.Equal(expected, raw) {
		return EnvironmentRecovery{}, invalidEnvironmentProgress()
	}
	return source, nil
}
