package attachremoval

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

// WorkCapture adds operation-owned encrypted hook inputs to the original frozen
// removal snapshot. All attempts reuse this record, including after reconnect.
// It contains no resolved credential, generated fact or Secret plaintext.
type WorkCapture struct {
	Schema           uint32                                        `json:"schema"`
	TaskID           string                                        `json:"task_id"`
	WorkItemID       string                                        `json:"work_item_id"`
	OperationID      string                                        `json:"operation_id"`
	Action           workexecution.Input                           `json:"action"`
	FrozenSHA256     string                                        `json:"frozen_sha256"`
	BackingProjectID string                                        `json:"backing_project_id"`
	Hook             *taskconfiguration.BackingHookEncryptedInputs `json:"hook,omitempty"`
	Sources          []tasksecretpinrecord.Record                  `json:"sources,omitempty"`
	Pins             *tasksecretpins.Binding                       `json:"pins,omitempty"`
}

func WorkCaptureKey(workID string) string {
	return "/v1/private/attach-removal-work-inputs/" + workID
}

// EncodePreparedCapture seals encrypted inputs before pins activate. Prepared
// captures cannot be decoded as active work or authorize Agent execution.
func EncodePreparedCapture(capture WorkCapture) ([]byte, error) {
	if capture.Pins != nil || capture.validate(false) != nil {
		return nil, recordcodec.CorruptRecord()
	}
	raw, err := recordcodec.Encode("attach-removal-prepared-input", capture)
	if err != nil {
		return nil, err
	}
	if len(raw) > MaximumInputBytes {
		clear(raw)
		return nil, recordcodec.CorruptRecord()
	}
	return raw, nil
}

func DecodePreparedCapture(raw []byte) (WorkCapture, error) {
	if len(raw) > MaximumInputBytes {
		return WorkCapture{}, recordcodec.CorruptRecord()
	}
	capture, err := recordcodec.Decode[WorkCapture](raw, "attach-removal-prepared-input")
	if err != nil || capture.Pins != nil || capture.validate(false) != nil {
		capture.Clear()
		return WorkCapture{}, recordcodec.CorruptRecord()
	}
	if capture.Hook != nil {
		capture.Hook.SecretSources = append([]tasksecretpinrecord.Record(nil), capture.Sources...)
	}
	return capture, nil
}

func (capture WorkCapture) Validate() error {
	return capture.validate(true)
}

func (capture WorkCapture) validate(active bool) error {
	if capture.Schema != 1 || ids.Validate(ids.KindTask, capture.TaskID) != nil ||
		ids.Validate(ids.KindWorkItem, capture.WorkItemID) != nil || ids.Validate(ids.KindOperation, capture.OperationID) != nil ||
		ids.Validate(ids.KindPlan, capture.Action.ID) != nil || !recordcodec.ValidSHA256(capture.Action.SHA256) ||
		!recordcodec.ValidSHA256(capture.FrozenSHA256) || ids.Validate(ids.KindProject, capture.BackingProjectID) != nil {
		return recordcodec.CorruptRecord()
	}
	if capture.Hook == nil {
		if len(capture.Sources) != 0 || capture.Pins != nil {
			return recordcodec.CorruptRecord()
		}
		return nil
	}
	if taskconfiguration.ValidateBackingHookEncryptedInputs(*capture.Hook) != nil || capture.Hook.OperationID != capture.OperationID {
		return recordcodec.CorruptRecord()
	}
	previous := ""
	for _, source := range capture.Sources {
		if tasksecretpinrecord.Validate(source) != nil || source.OperationID != capture.OperationID || source.SecretID <= previous {
			return recordcodec.CorruptRecord()
		}
		previous = source.SecretID
	}
	if len(capture.Sources) == 0 {
		if capture.Pins != nil {
			return recordcodec.CorruptRecord()
		}
		return nil
	}
	if len(capture.Sources) > tasksecretpins.MaximumPins {
		return recordcodec.CorruptRecord()
	}
	if !active {
		if capture.Pins != nil {
			return recordcodec.CorruptRecord()
		}
		return nil
	}
	if capture.Pins == nil || capture.Pins.Validate() != nil ||
		capture.Pins.OperationID != capture.OperationID || capture.Pins.TaskID != capture.TaskID ||
		capture.Pins.WorkItemID != capture.WorkItemID || !capture.Pins.MatchesSources(capture.Sources) {
		return recordcodec.CorruptRecord()
	}
	return nil
}

func EncodeWorkCapture(capture WorkCapture) ([]byte, error) {
	if err := capture.Validate(); err != nil {
		return nil, err
	}
	raw, err := recordcodec.Encode("attach-removal-work-input", capture)
	if err != nil {
		return nil, err
	}
	if len(raw) > MaximumInputBytes {
		clear(raw)
		return nil, recordcodec.CorruptRecord()
	}
	return raw, nil
}

func DecodeWorkCapture(raw []byte) (WorkCapture, error) {
	if len(raw) > MaximumInputBytes {
		return WorkCapture{}, recordcodec.CorruptRecord()
	}
	capture, err := recordcodec.Decode[WorkCapture](raw, "attach-removal-work-input")
	if err != nil || capture.Validate() != nil {
		capture.Clear()
		return WorkCapture{}, recordcodec.CorruptRecord()
	}
	if capture.Hook != nil {
		capture.Hook.SecretSources = append([]tasksecretpinrecord.Record(nil), capture.Sources...)
	}
	return capture, nil
}

func (capture *WorkCapture) Clear() {
	if capture != nil && capture.Hook != nil {
		clear(capture.Hook.Ciphertext)
	}
}

// ApplyTo supplies only a matching frozen source with its separately sealed
// hook envelope. Callers must first compare Task/work/action/source identities.
func (capture WorkCapture) ApplyTo(input *Input) {
	if capture.Hook == nil {
		return
	}
	input.HookOperationID = capture.OperationID
	input.HookInputs = capture.Hook
	input.HookInputSet = &taskconfiguration.TaskBackingHookInputSet{ProjectID: capture.BackingProjectID,
		CiphertextSHA256: capture.Hook.CiphertextSHA256, SecretSources: capture.Sources}
}
