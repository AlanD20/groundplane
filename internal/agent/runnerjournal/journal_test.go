package runnerjournal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestJournalRecoversLongestSyncedChainWhenHeadIsStale(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "runner-ownership")
	journal, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	attempt := testAttempt(1)
	progress, err := journal.Begin(context.Background(), attempt, runnerallocation.RuntimeOperationCreate)
	if err != nil {
		t.Fatal(err)
	}
	progress, err = journal.Issue(context.Background(), progress, runnerallocation.StepEnsureIdentity)
	if err != nil {
		t.Fatal(err)
	}
	directory, err := journal.operationDirectory(attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "HEAD"), []byte(`{"revision":"0","digest":"sha256:`+strings.Repeat("0", 64)+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	recovered, err := journal.Begin(context.Background(), attempt, runnerallocation.RuntimeOperationCreate)
	if err != nil || recovered.Revision != progress.Revision || recovered.ActiveStep == nil ||
		*recovered.ActiveStep != runnerallocation.StepEnsureIdentity {
		t.Fatalf("recovered = %#v, %v", recovered, err)
	}
}

func TestJournalRejectsUnknownCommittedInputWithoutFollowingIt(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "runner-ownership")
	journal, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	attempt := testAttempt(2)
	if _, err := journal.Begin(context.Background(), attempt, runnerallocation.RuntimeOperationCreate); err != nil {
		t.Fatal(err)
	}
	directory, err := journal.operationDirectory(attempt)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "must-not-read")
	if err := os.WriteFile(target, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(directory, "00000000000000000001.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Begin(context.Background(), attempt, runnerallocation.RuntimeOperationCreate); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("Begin(corrupt) error = %v", err)
	}
}

func TestJournalRejectsPathIdentityBeforeFilesystemAccess(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "runner-ownership")
	journal, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	attempt := testAttempt(3)
	attempt.OwnershipNonce = "../redirect"
	if _, err := journal.Begin(context.Background(), attempt, runnerallocation.RuntimeOperationCreate); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("Begin(redirect) error = %v", err)
	}
}

func testAttempt(entropy int64) runnerallocation.RunnerRuntimeAttempt {
	at := time.Date(2026, time.August, 26, 12, 0, 0, 0, time.UTC)
	return runnerallocation.RunnerRuntimeAttempt{
		TaskID: ids.NewAt(ids.KindTask, at, entropy+100), Executor: controllerExecutor,
		OwnershipNonce: strings.Repeat("a", 64),
		Plan: runnerallocation.RuntimePlan{
			RunnerID: ids.NewAt(ids.KindRunner, at, entropy), RuntimeEpoch: 1,
		},
	}
}
