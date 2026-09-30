package etcdcontainer

import (
	"context"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// This receipt is independent of etcd: bootstrap can restore an interrupted
// activation before it opens the store containing the native Task.
type activationRecord struct {
	TaskID    string `json:"task_id,omitempty"`
	Candidate string `json:"candidate,omitempty"`
	Previous  string `json:"previous,omitempty"`
	Phase     string `json:"phase,omitempty"`
}

func validateActivation(ctx context.Context, content []byte) error {
	record, err := jcs.Decode[activationRecord](content)
	if err != nil {
		return err
	}
	if record == (activationRecord{}) {
		return nil
	}
	if ids.Validate(ids.KindTask, record.TaskID) != nil ||
		(record.Phase != "prepared" && record.Phase != "applied" && record.Phase != "rolled_back") {
		return errs.New(errs.KindValidationFailed, "etcd activation receipt is invalid")
	}
	for _, document := range []string{record.Candidate, record.Previous} {
		if err := config.ValidateEtcdDocument(ctx, []byte(document)); err != nil {
			return errs.Wrap(errs.KindValidationFailed, err)
		}
	}
	return nil
}
func (m *Manager) activationRecord(ctx context.Context) (activationRecord, error) {
	content, _, _, err := m.operation.Current(ctx)
	if err != nil {
		return activationRecord{}, err
	}
	if err := validateActivation(ctx, []byte(content)); err != nil {
		return activationRecord{}, err
	}
	return jcs.Decode[activationRecord]([]byte(content))
}
func (m *Manager) recordActivation(ctx context.Context, record activationRecord) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		return err
	}
	current, revision, _, err := m.operation.Current(ctx)
	if err != nil {
		return err
	}
	if current == string(raw) {
		return nil
	}
	_, _, _, err = m.operation.Replace(ctx, record.TaskID+":"+record.Phase, revision, string(raw))
	return err
}
func (m *Manager) recoverInterrupted(ctx context.Context) error {
	record, err := m.activationRecord(ctx)
	if err != nil {
		return err
	}
	if record.Phase != "prepared" {
		return nil
	}
	predecessor, err := config.ParseEtcdDocument(ctx, []byte(record.Previous))
	if err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	replaced, err := m.activation.Replaced(ctx, record.TaskID+":rollback")
	if err != nil {
		return err
	}
	if !replaced {
		_, revision, _, err := m.activation.Current(ctx)
		if err != nil {
			return err
		}
		if _, _, _, err := m.activation.Replace(ctx, record.TaskID+":rollback", revision, record.Previous); err != nil {
			return err
		}
	}
	m.active = predecessor
	if err := m.reconcile(ctx); err != nil {
		return err
	}
	m.document.MarkApplied([]byte(record.Previous))
	record.Phase = "rolled_back"
	return m.recordActivation(ctx, record)
}
