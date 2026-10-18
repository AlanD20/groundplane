package executionplan

import (
	"bytes"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BackupWorkRecovery identifies the failed execution whose ownership must be
// accounted for. It does not authorize repeating capture, restore or deletion.
type BackupWorkRecovery struct {
	Forward  workexecution.Attempt `json:"forward"`
	Failure  BackupWorkFailure     `json:"failure"`
	Evidence workexecution.Input   `json:"evidence"`
}

func (source BackupWorkRecovery) Validate() error {
	forward := source.Forward
	if forward.Validate() != nil || forward.Direction != workexecution.Forward || forward.StartedAt == nil ||
		forward.FinishedAt == nil || forward.Result == nil || forward.Outcome == workexecution.Succeeded ||
		*forward.Result != source.Evidence || !source.Failure.RecordedAt.Equal(*forward.FinishedAt) {
		return errs.New(errs.KindValidationFailed, "Backup recovery requires its original failed execution")
	}
	_, evidence, err := EncodeBackupWorkFailure(forward, source.Failure)
	if err != nil {
		return err
	}
	if evidence != source.Evidence {
		return errs.New(errs.KindStateConflict, "Backup recovery failure evidence changed")
	}
	return nil
}

func (source BackupWorkRecovery) ValidateFor(attempt workexecution.Attempt) error {
	if err := source.Validate(); err != nil {
		return err
	}
	if attempt.Validate() != nil || attempt.Direction != workexecution.Recovery ||
		attempt.TaskID != source.Forward.TaskID || attempt.WorkItemID != source.Forward.WorkItemID ||
		attempt.Input != source.Forward.Input || attempt.Number <= source.Forward.Number ||
		attempt.Executor.Executor != workexecution.Agent || source.Forward.Executor.Executor != workexecution.Agent ||
		attempt.Executor.HostID != source.Forward.Executor.HostID {
		return errs.New(errs.KindStateConflict, "Backup recovery belongs to a different work item or host")
	}
	return nil
}

func EncodeBackupWorkRecovery(source BackupWorkRecovery) ([]byte, error) {
	if err := source.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(source)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		return nil, err
	}
	if len(raw) > 32768 {
		return nil, errs.New(errs.KindValidationFailed, "Backup recovery evidence is oversized")
	}
	return raw, nil
}

func DecodeBackupWorkRecovery(raw []byte) (BackupWorkRecovery, error) {
	if len(raw) == 0 || len(raw) > 32768 {
		return BackupWorkRecovery{}, errs.New(errs.KindValidationFailed, "Backup recovery evidence is missing or oversized")
	}
	source, err := jcs.Decode[BackupWorkRecovery](raw)
	if err != nil {
		return BackupWorkRecovery{}, err
	}
	expected, err := EncodeBackupWorkRecovery(source)
	if err != nil {
		return BackupWorkRecovery{}, err
	}
	if !bytes.Equal(raw, expected) {
		return BackupWorkRecovery{}, errs.New(errs.KindValidationFailed, "Backup recovery evidence is not canonical")
	}
	return source, nil
}
