package etcd

import (
	context "context"
	errors "errors"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	core "github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	testing "testing"
	time "time"
)

// Rationale: an Attach selected by an active Backup run must remain stable
// across detach initiation and final durable deprovisioning.
func TestAttachRepositoryRejectsDestructiveBackupSourceMutations(t *testing.T) {
	t.Run("detach lifecycle replacement", func(t *testing.T) {
		ctx := context.Background()
		store := newAttachTestStore()
		scope := seedAttachScope(t, ctx, store)
		repository, err := NewAttachRepository(store)
		if err != nil {
			t.Fatalf("NewAttachRepository() error = %v", err)
		}
		record, facts := testPendingAttach(t, scope, 62, "backup-source-detach", nil)
		ready := createTestAttach(t, ctx, repository, scope, record, &facts)
		ready, err = advanceAttachReady(ctx, repository, ready)
		if err != nil {
			t.Fatalf("advanceAttachReady() error = %v", err)
		}
		exclusionKey, err := testbackupruntime.BackupSourceTargetExclusionKey(
			testbackupruntime.BackupSourceTargetAttach,
			record.ID,
		)
		if err != nil {
			t.Fatalf("backupSourceTargetExclusionKey() error = %v", err)
		}
		exclusionValue := testAttachBackupExclusionValue(
			t, scope.Environment.Record.ID, record.ID, 621,
		)
		if _, err := store.Put(ctx, exclusionKey, exclusionValue); err != nil {
			t.Fatalf("Put(Backup source exclusion) error = %v", err)
		}
		ready, err = repository.GetAttach(ctx, record.ID)
		if err != nil {
			t.Fatalf("GetAttach() error = %v", err)
		}
		detaching, err := testattachments.BeginAttachDetaching(
			ready.Record,
			ids.NewAt(ids.KindTask, testAttachTime.Add(2*time.Minute), 620),
		)
		if err != nil {
			t.Fatalf("BeginAttachDetaching() error = %v", err)
		}
		_, err = repository.ReplaceLifecycle(ctx, ready, detaching)
		if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
			t.Fatalf("ReplaceLifecycle(detaching) error = %v, want resource in use", err)
		}
	})

	t.Run("detached record removal", func(t *testing.T) {
		ctx := context.Background()
		store := newAttachTestStore()
		scope := seedAttachScope(t, ctx, store)
		repository, err := NewAttachRepository(store)
		if err != nil {
			t.Fatalf("NewAttachRepository() error = %v", err)
		}
		record, facts := testPendingAttach(t, scope, 63, "backup-source-removal", nil)
		current := createTestAttach(t, ctx, repository, scope, record, &facts)
		current, err = advanceAttachReady(ctx, repository, current)
		if err != nil {
			t.Fatalf("advanceAttachReady() error = %v", err)
		}
		taskID := ids.NewAt(ids.KindTask, testAttachTime.Add(3*time.Minute), 630)
		detaching, err := testattachments.BeginAttachDetaching(current.Record, taskID)
		if err != nil {
			t.Fatalf("BeginAttachDetaching() error = %v", err)
		}
		current, err = repository.ReplaceLifecycle(ctx, current, detaching)
		if err != nil {
			t.Fatalf("ReplaceLifecycle(detaching) error = %v", err)
		}
		detached, err := testattachments.CompleteAttachDetaching(current.Record, taskID, true)
		if err != nil {
			t.Fatalf("CompleteAttachDetaching() error = %v", err)
		}
		current, err = repository.ReplaceLifecycle(ctx, current, detached)
		if err != nil {
			t.Fatalf("ReplaceLifecycle(detached) error = %v", err)
		}
		exclusionKey, err := testbackupruntime.BackupSourceTargetExclusionKey(
			testbackupruntime.BackupSourceTargetAttach,
			record.ID,
		)
		if err != nil {
			t.Fatalf("backupSourceTargetExclusionKey() error = %v", err)
		}
		exclusionValue := testAttachBackupExclusionValue(
			t, scope.Environment.Record.ID, record.ID, 631,
		)
		if _, err := store.Put(ctx, exclusionKey, exclusionValue); err != nil {
			t.Fatalf("Put(Backup source exclusion) error = %v", err)
		}
		current, err = repository.GetAttach(ctx, record.ID)
		if err != nil {
			t.Fatalf("GetAttach() error = %v", err)
		}
		_, err = repository.DeleteDetachedAttach(ctx, current)
		if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
			t.Fatalf("DeleteDetachedAttach() error = %v, want resource in use", err)
		}
	})
}

// Rationale: an Attach detach failure preserves truthful non-destructive state
// even when a Backup source exclusion becomes active after detach initiation.
func TestAttachRepositoryAllowsDetachFailureWithActiveBackupSourceExclusion(t *testing.T) {
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 68, "backup-source-detach-failure", nil)
	current := createTestAttach(t, ctx, repository, scope, record, &facts)
	current, err = advanceAttachReady(ctx, repository, current)
	if err != nil {
		t.Fatalf("advanceAttachReady() error = %v", err)
	}
	taskID := ids.NewAt(ids.KindTask, testAttachTime.Add(8*time.Minute), 680)
	detaching, err := testattachments.BeginAttachDetaching(current.Record, taskID)
	if err != nil {
		t.Fatalf("BeginAttachDetaching() error = %v", err)
	}
	current, err = repository.ReplaceLifecycle(ctx, current, detaching)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(detaching) error = %v", err)
	}
	exclusionKey, err := testbackupruntime.BackupSourceTargetExclusionKey(
		testbackupruntime.BackupSourceTargetAttach,
		record.ID,
	)
	if err != nil {
		t.Fatalf("backupSourceTargetExclusionKey() error = %v", err)
	}
	exclusionValue := testAttachBackupExclusionValue(
		t, scope.Environment.Record.ID, record.ID, 681,
	)
	if _, err := store.Put(ctx, exclusionKey, exclusionValue); err != nil {
		t.Fatalf("Put(Backup source exclusion) error = %v", err)
	}
	current, err = repository.GetAttach(ctx, record.ID)
	if err != nil {
		t.Fatalf("GetAttach(detaching) error = %v", err)
	}
	failed, err := testattachments.CompleteAttachDetaching(current.Record, taskID, false)
	if err != nil {
		t.Fatalf("CompleteAttachDetaching(failed) error = %v", err)
	}
	current, err = repository.ReplaceLifecycle(ctx, current, failed)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(failed) error = %v", err)
	}
	if current.Record.Status != core.AttachFailed || current.Record.Operation != testattachments.AttachOperationDetach {
		t.Fatalf("failed Attach = %#v", current.Record)
	}
}

// Rationale: malformed or misbucketed Backup exclusion bytes are corrupt
// authority, not a valid resource-in-use fence that may be trusted blindly.
func TestAttachRepositoryRejectsInvalidBackupSourceExclusionEvidence(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		value func(*testing.T, AttachCreateScope, testattachments.Record) []byte
	}{
		{
			name: "malformed",
			value: func(*testing.T, AttachCreateScope, testattachments.Record) []byte {
				return []byte("not-an-exclusion-record")
			},
		},
		{
			name: "target mismatch",
			value: func(t *testing.T, scope AttachCreateScope, _ testattachments.Record) []byte {
				return testAttachBackupExclusionValue(
					t,
					scope.Environment.Record.ID,
					ids.NewAt(ids.KindAttach, testAttachTime.Add(7*time.Minute), 671),
					672,
				)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := newAttachTestStore()
			scope := seedAttachScope(t, ctx, store)
			repository, err := NewAttachRepository(store)
			if err != nil {
				t.Fatalf("NewAttachRepository() error = %v", err)
			}
			record, facts := testPendingAttach(t, scope, 67, "invalid-exclusion", nil)
			current := createTestAttach(t, ctx, repository, scope, record, &facts)
			current, err = advanceAttachReady(ctx, repository, current)
			if err != nil {
				t.Fatalf("advanceAttachReady() error = %v", err)
			}
			exclusionKey, err := testbackupruntime.BackupSourceTargetExclusionKey(
				testbackupruntime.BackupSourceTargetAttach,
				record.ID,
			)
			if err != nil {
				t.Fatalf("backupSourceTargetExclusionKey() error = %v", err)
			}
			if _, err := store.Put(ctx, exclusionKey, test.value(t, scope, record)); err != nil {
				t.Fatalf("Put(invalid Backup exclusion) error = %v", err)
			}
			current, err = repository.GetAttach(ctx, record.ID)
			if err != nil {
				t.Fatalf("GetAttach() error = %v", err)
			}
			detaching, err := testattachments.BeginAttachDetaching(
				current.Record,
				ids.NewAt(ids.KindTask, testAttachTime.Add(7*time.Minute), 673),
			)
			if err != nil {
				t.Fatalf("BeginAttachDetaching() error = %v", err)
			}
			if _, err := repository.ReplaceLifecycle(ctx, current, detaching); !errors.Is(
				err,
				errs.New(errs.KindInternal, ""),
			) {
				t.Fatalf("ReplaceLifecycle(invalid exclusion) error = %v, want internal", err)
			}
			stored, err := repository.GetAttach(ctx, record.ID)
			if err != nil || stored.Record.Status != core.AttachReady {
				t.Fatalf("GetAttach(after invalid exclusion) = %#v/%v", stored, err)
			}
		})
	}
}
