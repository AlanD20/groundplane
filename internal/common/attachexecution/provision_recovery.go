package attachexecution

import (
	"bytes"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

const MaximumProvisionRecoveryBytes = 8192

// ProvisionRecovery captures original failure authority for inspection. Unlike
// removal, even a completed provisioning command can leave durable credentials
// or grants requiring settlement. It never grants another command invocation.
type ProvisionRecovery struct {
	Forward  workexecution.Attempt `json:"forward"`
	Failure  ProvisionFailure      `json:"failure"`
	Evidence workexecution.Input   `json:"evidence"`
}

func (source ProvisionRecovery) Validate() error {
	forward := source.Forward
	if forward.Validate() != nil || forward.Direction != workexecution.Forward || forward.FinishedAt == nil ||
		forward.StartedAt == nil || forward.Result == nil || forward.Outcome == workexecution.Succeeded ||
		*forward.Result != source.Evidence || !source.Failure.RecordedAt.Equal(*forward.FinishedAt) || source.Failure.Checkpoint == nil {
		return invalid("Attach provisioning recovery requires retained failure and command authority")
	}
	_, evidence, err := EncodeProvisionFailure(forward, source.Failure)
	if err != nil || evidence != source.Evidence {
		return invalid("Attach provisioning recovery differs from its original failure")
	}
	return nil
}

func (source ProvisionRecovery) ValidateFor(attempt workexecution.Attempt) error {
	if err := source.Validate(); err != nil {
		return err
	}
	if attempt.Validate() != nil || attempt.Direction != workexecution.Recovery ||
		attempt.Executor.Executor != workexecution.Agent || attempt.TaskID != source.Forward.TaskID ||
		attempt.WorkItemID != source.Forward.WorkItemID || attempt.Input != source.Forward.Input ||
		attempt.Number <= source.Forward.Number || attempt.Executor.HostID != source.Forward.Executor.HostID ||
		attempt.AssignedAt.Before(*source.Forward.FinishedAt) {
		return invalid("Attach provisioning recovery does not belong to the captured host and work item")
	}
	return nil
}

func EncodeProvisionRecovery(source ProvisionRecovery) ([]byte, error) {
	if err := source.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(source)
	if err != nil {
		return nil, invalid("Attach provisioning recovery cannot be encoded")
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return nil, err
	}
	if len(canonical) > MaximumProvisionRecoveryBytes {
		return nil, invalid("Attach provisioning recovery source is oversized")
	}
	return canonical, nil
}

func DecodeProvisionRecovery(raw []byte) (ProvisionRecovery, error) {
	if len(raw) == 0 || len(raw) > MaximumProvisionRecoveryBytes {
		return ProvisionRecovery{}, invalid("Attach provisioning recovery source is missing or oversized")
	}
	source, err := jcs.Decode[ProvisionRecovery](raw)
	if err != nil {
		return ProvisionRecovery{}, invalid("Attach provisioning recovery source is malformed")
	}
	expected, err := EncodeProvisionRecovery(source)
	if err != nil || !bytes.Equal(raw, expected) {
		return ProvisionRecovery{}, invalid("Attach provisioning recovery source differs from its canonical record")
	}
	return source, nil
}
