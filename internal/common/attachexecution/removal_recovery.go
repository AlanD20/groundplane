package attachexecution

import (
	"bytes"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

const MaximumRemovalRecoveryBytes = 8192

// RemovalRecovery authorizes inspection of an interrupted command, not another
// invocation. It retains the original failure independently of cleanup results.
type RemovalRecovery struct {
	Forward  workexecution.Attempt `json:"forward"`
	Failure  RemovalFailure        `json:"failure"`
	Evidence workexecution.Input   `json:"evidence"`
}

func (source RemovalRecovery) Validate() error {
	forward := source.Forward
	if forward.Validate() != nil || forward.Direction != workexecution.Forward || forward.FinishedAt == nil ||
		forward.StartedAt == nil || forward.Result == nil || forward.Outcome == workexecution.Succeeded ||
		*forward.Result != source.Evidence || !source.Failure.RecordedAt.Equal(*forward.FinishedAt) ||
		source.Failure.Checkpoint == nil || source.Failure.Checkpoint.Sequence%2 != 1 {
		return invalid("Attach recovery requires an interrupted captured command")
	}
	_, evidence, err := EncodeRemovalFailure(forward, source.Failure)
	if err != nil || evidence != source.Evidence {
		return invalid("Attach recovery differs from its original failure")
	}
	return nil
}

func (source RemovalRecovery) ValidateFor(attempt workexecution.Attempt) error {
	if err := source.Validate(); err != nil {
		return err
	}
	if attempt.Validate() != nil || attempt.Direction != workexecution.Recovery ||
		attempt.Executor.Executor != workexecution.Agent || attempt.TaskID != source.Forward.TaskID ||
		attempt.WorkItemID != source.Forward.WorkItemID || attempt.Input != source.Forward.Input ||
		attempt.Number <= source.Forward.Number || attempt.Executor.HostID != source.Forward.Executor.HostID {
		return invalid("Attach recovery does not belong to the captured host and work item")
	}
	return nil
}

func EncodeRemovalRecovery(source RemovalRecovery) ([]byte, error) {
	if err := source.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(source)
	if err != nil {
		return nil, invalid("Attach recovery cannot be encoded")
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return nil, err
	}
	if len(canonical) > MaximumRemovalRecoveryBytes {
		return nil, invalid("Attach recovery source is oversized")
	}
	return canonical, nil
}

func DecodeRemovalRecovery(raw []byte) (RemovalRecovery, error) {
	if len(raw) == 0 || len(raw) > MaximumRemovalRecoveryBytes {
		return RemovalRecovery{}, invalid("Attach recovery source is missing or oversized")
	}
	source, err := jcs.Decode[RemovalRecovery](raw)
	if err != nil {
		return RemovalRecovery{}, invalid("Attach recovery source is malformed")
	}
	expected, err := EncodeRemovalRecovery(source)
	if err != nil || !bytes.Equal(raw, expected) {
		return RemovalRecovery{}, invalid("Attach recovery source differs from its canonical record")
	}
	return source, nil
}
