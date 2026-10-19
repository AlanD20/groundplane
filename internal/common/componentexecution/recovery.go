package componentexecution

import (
	"bytes"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const MaximumRecoveryBytes = 8192

// Recovery captures the failed forward execution being accounted for.
// A successful cleanup does not replace this failure with a successful action.
type Recovery struct {
	Forward  workexecution.Attempt `json:"forward"`
	Failure  Failure               `json:"failure"`
	Evidence workexecution.Input   `json:"evidence"`
}

func (source Recovery) Validate(input *agentpb.ComponentWorkInput) error {
	layout, err := CaptureLayout(source.Forward, input)
	if err != nil {
		return err
	}
	return source.ValidateLayout(layout)
}

func (source Recovery) ValidateLayout(layout Layout) error {
	forward := source.Forward
	if forward.Validate() != nil || forward.Direction != workexecution.Forward || forward.FinishedAt == nil ||
		forward.Result == nil || forward.Outcome == workexecution.Succeeded || *forward.Result != source.Evidence ||
		!source.Failure.RecordedAt.Equal(*forward.FinishedAt) {
		return invalidProgress()
	}
	_, evidence, err := EncodeLayoutFailure(forward, layout, source.Failure)
	if err != nil || evidence != source.Evidence {
		return invalidProgress()
	}
	return nil
}

func (source Recovery) ValidateFor(attempt workexecution.Attempt, input *agentpb.ComponentWorkInput) error {
	layout, err := CaptureLayout(source.Forward, input)
	if err != nil {
		return err
	}
	return source.ValidateLayoutFor(attempt, layout)
}

func (source Recovery) ValidateLayoutFor(attempt workexecution.Attempt, layout Layout) error {
	if err := source.ValidateLayout(layout); err != nil {
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

func EncodeRecovery(source Recovery, input *agentpb.ComponentWorkInput) ([]byte, error) {
	if err := source.Validate(input); err != nil {
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

func DecodeRecovery(raw []byte, input *agentpb.ComponentWorkInput) (Recovery, error) {
	if len(raw) == 0 || len(raw) > MaximumRecoveryBytes {
		return Recovery{}, invalidProgress()
	}
	source, err := jcs.Decode[Recovery](raw)
	if err != nil {
		return Recovery{}, invalidProgress()
	}
	expected, err := EncodeRecovery(source, input)
	if err != nil || !bytes.Equal(expected, raw) {
		return Recovery{}, invalidProgress()
	}
	return source, nil
}
