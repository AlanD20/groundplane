package etcdcontainer

import (
	"context"
	"fmt"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/infra/configdocument"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const configurationPath = "/etc/groundplane/etcd.yaml"
const activationPath = "/etc/groundplane/etcd.applied.yaml"

func (m *Manager) openConfiguration(ctx context.Context) error {
	return m.openDocuments(ctx, configurationPath, activationPath)
}

func (m *Manager) openDocuments(ctx context.Context, desiredPath, appliedPath string) error {
	initial, err := config.DefaultEtcdDocument()
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	for _, path := range []string{desiredPath, appliedPath} {
		if err := configdocument.Initialize(ctx, path, initial); err != nil {
			return err
		}
	}
	activation, err := configdocument.New(
		ctx,
		appliedPath,
		initial,
		"etcd.config.activate",
		config.ValidateEtcdDocument,
	)
	if err != nil {
		return err
	}
	content, _, _, err := activation.Current(ctx)
	if err != nil {
		return err
	}
	m.active, err = config.ParseEtcdDocument(ctx, []byte(content))
	if err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	m.activation = activation
	m.document, err = configdocument.New(
		ctx,
		desiredPath,
		[]byte(content),
		"etcd.config.set",
		config.ValidateEtcdDocument,
	)
	if err != nil {
		return err
	}
	operationPath := appliedPath + ".activation"
	if err := configdocument.Initialize(ctx, operationPath, []byte("{}")); err != nil {
		return err
	}
	m.operation, err = configdocument.New(
		ctx,
		operationPath,
		[]byte("{}"),
		"etcd.config.activation-record",
		validateActivation,
	)
	return err
}

func (m *Manager) Configuration() *configdocument.Store { return m.document }

func (m *Manager) AppliedConfiguration(ctx context.Context) (string, error) {
	content, _, _, err := m.activation.Current(ctx)
	return content, err
}

// Apply pins both documents in the native Task. The separate applied file owns
// bootstrap selection, so a Controller restart never activates a mere Save.
// Replacement receipts allow replay after etcd went down or the process died.
func (m *Manager) Apply(ctx context.Context, taskID, candidate, previous string) error {
	cfg, err := config.ParseEtcdDocument(ctx, []byte(candidate))
	if err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	predecessor, err := config.ParseEtcdDocument(ctx, []byte(previous))
	if err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rolledBack, err := m.activation.Replaced(ctx, taskID+":rollback")
	if err != nil {
		return err
	}
	if rolledBack {
		m.active = predecessor
		if err := m.reconcile(ctx); err != nil {
			return err
		}
		m.document.MarkApplied([]byte(previous))
		return errs.New(errs.KindStateConflict, "etcd Apply failed; the previous configuration is restored")
	}
	content, revision, _, err := m.activation.Current(ctx)
	if err != nil {
		return err
	}
	if content != previous && content != candidate {
		return errs.New(errs.KindStateConflict, "etcd applied configuration changed after this Task was accepted")
	}
	record, err := m.activationRecord(ctx)
	if err != nil {
		return err
	}
	if record.TaskID != taskID {
		if record.Phase == "prepared" {
			return errs.New(errs.KindResourceInUse, "etcd activation recovery must finish first")
		}
		if err := m.recordActivation(ctx, activationRecord{TaskID: taskID, Candidate: candidate, Previous: previous, Phase: "prepared"}); err != nil {
			return err
		}
	}
	if content != candidate {
		if _, _, _, err := m.activation.Replace(ctx, taskID+":apply", revision, candidate); err != nil {
			return err
		}
	}
	m.active = cfg
	if err := m.reconcile(ctx); err != nil {
		// Bounded independent cleanup is necessary because the store itself was
		// interrupted; cancellation must not strand the control plane offline.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*startupTimeout)
		defer cancel()
		_, revision, _, readErr := m.activation.Current(cleanup)
		if readErr != nil {
			return errs.Wrap(
				errs.KindStorageUnavailable,
				fmt.Errorf("etcd Apply failed (%v); rollback selection failed: %w", err, readErr),
			)
		}
		if _, _, _, restoreErr := m.activation.Replace(cleanup, taskID+":rollback", revision, previous); restoreErr != nil {
			return errs.Wrap(
				errs.KindStorageUnavailable,
				fmt.Errorf("etcd Apply failed (%v); rollback selection failed: %w", err, restoreErr),
			)
		}
		m.active = predecessor
		if restoreErr := m.reconcile(cleanup); restoreErr != nil {
			return errs.Wrap(
				errs.KindStorageUnavailable,
				fmt.Errorf("etcd Apply failed (%v); predecessor is not ready: %w", err, restoreErr),
			)
		}
		m.document.MarkApplied([]byte(previous))
		if recordErr := m.recordActivation(cleanup, activationRecord{TaskID: taskID, Candidate: candidate, Previous: previous, Phase: "rolled_back"}); recordErr != nil {
			return recordErr
		}
		return errs.Wrap(
			errs.KindStateConflict,
			fmt.Errorf("etcd Apply failed; previous configuration restored: %w", err),
		)
	}
	m.document.MarkApplied([]byte(candidate))
	if err := m.recordActivation(ctx, activationRecord{TaskID: taskID, Candidate: candidate, Previous: previous, Phase: "applied"}); err != nil {
		return err
	}
	return nil
}
