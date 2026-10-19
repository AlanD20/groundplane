package backingprovision

import (
	"bytes"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// Recovery accounts for the failed first creation and its captured absent
// runtime predecessor. It stops helpers and removes only the owned container,
// never database/Volume data, and never replays an after-start hook.
type Recovery struct {
	Forward  workexecution.Attempt  `json:"forward"`
	Failure  Failure                `json:"failure"`
	Evidence workexecution.Input    `json:"evidence"`
	Boundary *Progress              `json:"boundary,omitempty"`
	Previous *workexecution.Attempt `json:"previous_cleanup,omitempty"`
}

func (recovery Recovery) Validate(layout Layout) error {
	forward := recovery.Forward
	if forward.Validate() != nil || forward.Direction != workexecution.Forward || forward.FinishedAt == nil ||
		forward.Result == nil || forward.Outcome == workexecution.Succeeded || *forward.Result != recovery.Evidence ||
		!recovery.Failure.RecordedAt.Equal(
			*forward.FinishedAt,
		) || (recovery.Failure.Checkpoint == nil) != (recovery.Boundary == nil) {
		return invalid("backing creation recovery does not identify its actual failed forward execution")
	}
	_, evidence, err := EncodeFailure(forward, layout, recovery.Failure)
	if err != nil || evidence != recovery.Evidence {
		return invalid("backing creation recovery failure differs from its retained result")
	}
	if recovery.Boundary != nil {
		cursor, err := recovery.Boundary.Checkpoint(forward, layout)
		if err != nil || cursor != *recovery.Failure.Checkpoint {
			return invalid("backing creation recovery boundary differs from its saved checkpoint")
		}
	}
	if previous := recovery.Previous; previous != nil {
		if previous.Validate() != nil || previous.Direction != workexecution.Recovery || previous.FinishedAt == nil ||
			previous.Result == nil || previous.Outcome != workexecution.Failed || previous.TaskID != forward.TaskID ||
			previous.WorkItemID != forward.WorkItemID || previous.Input != forward.Input || previous.Executor.HostID != forward.Executor.HostID ||
			previous.Number <= forward.Number || previous.AssignedAt.Before(*forward.FinishedAt) {
			return invalid("backing creation cleanup retry does not identify its actual preceding interrupted cleanup")
		}
	}
	return nil
}

func (recovery Recovery) ValidateFor(attempt workexecution.Attempt, layout Layout) error {
	if recovery.Validate(layout) != nil || attempt.Validate() != nil || attempt.Direction != workexecution.Recovery ||
		attempt.FinishedAt != nil || attempt.TaskID != recovery.Forward.TaskID || attempt.WorkItemID != recovery.Forward.WorkItemID ||
		attempt.Input != recovery.Forward.Input || attempt.Number <= recovery.Forward.Number ||
		attempt.Executor.Executor != workexecution.Agent || attempt.Executor.HostID != recovery.Forward.Executor.HostID ||
		attempt.AssignedAt.Before(*recovery.Forward.FinishedAt) || layout.Validate(attempt) != nil {
		return invalid("backing creation cleanup assignment does not own its captured failure")
	}
	previous := recovery.Forward
	if recovery.Previous != nil {
		previous = *recovery.Previous
	}
	if previous.Number == ^uint64(0) || attempt.Number != previous.Number+1 ||
		attempt.AssignedAt.Before(*previous.FinishedAt) {
		return invalid("backing creation cleanup must follow its exact preceding attempt")
	}
	return nil
}

func EncodeRecovery(recovery Recovery, layout Layout) ([]byte, error) {
	if err := recovery.Validate(layout); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(recovery)
	if err != nil || len(raw) > 2*MaximumProgressBytes {
		return nil, invalid("backing creation recovery source exceeds its bounded format")
	}
	return jcs.Canonicalize(raw)
}

func DecodeRecovery(raw []byte, layout Layout) (Recovery, error) {
	if len(raw) == 0 || len(raw) > 2*MaximumProgressBytes {
		return Recovery{}, invalid("backing creation recovery source is missing or oversized")
	}
	recovery, err := jcs.Decode[Recovery](raw)
	if err != nil {
		return Recovery{}, invalid("backing creation recovery source cannot be decoded")
	}
	canonical, err := EncodeRecovery(recovery, layout)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Recovery{}, invalid("backing creation recovery source differs from its canonical receipt")
	}
	return recovery, nil
}
