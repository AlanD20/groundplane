package runnerjournal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
)

// RUN-10: caller mutation, changed nonces and a new boot cannot mint a second
// authority for an existing Task or change its pinned recovery inputs.
func TestJournalPinsPlanAndRejectsChangedAuthority(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "ownership")
	journal, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	attempt := testAttempt(20)
	attempt.Plan.Container.Labels = []string{"release-builder"}
	progress, err := journal.Begin(ctx, attempt, runnerallocation.RuntimeOperationCreate)
	if err != nil {
		t.Fatal(err)
	}
	attempt.Plan.Container.Labels[0] = "changed"
	if progress.Plan.Container.Labels[0] != "release-builder" {
		t.Fatal("caller changed the journal's captured plan")
	}
	attempt.Plan.Container.Labels[0] = "release-builder"
	reopened, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := reopened.Begin(ctx, attempt, runnerallocation.RuntimeOperationCreate)
	if err != nil || recovered.Plan.Container.Labels[0] != "release-builder" {
		t.Fatalf("pinned plan was not recovered: %v", err)
	}
	changed := attempt
	changed.Plan = attempt.Plan.Clone()
	changed.Plan.Container.Labels[0] = "changed"
	changed.OwnershipNonce = strings.Repeat("b", 64)
	if _, err := reopened.Begin(ctx, changed, runnerallocation.RuntimeOperationCreate); err == nil {
		t.Fatal("changed nonce selected a fresh journal for the same Task")
	}
	progress.Plan.Container.Labels[0] = "changed"
	progress.PlanDigest = progress.Plan.Digest()
	progress.IdentityDigest = progress.Plan.IdentityDigest()
	if _, err := reopened.Issue(ctx, progress, runnerallocation.StepEnsureIdentity); err == nil {
		t.Fatal("self-consistent but altered caller snapshot granted host authority")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := reopened.Issue(canceled, recovered, runnerallocation.StepEnsureIdentity); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf("canceled issue: %v", err)
	}
	unchanged, err := reopened.Begin(ctx, attempt, runnerallocation.RuntimeOperationCreate)
	if err != nil || unchanged.Revision != recovered.Revision {
		t.Fatalf("rejected request changed committed history: %v", err)
	}
	reopened.bootID = func(context.Context) (string, error) { return "11111111-1111-4111-8111-111111111111", nil }
	if _, err := reopened.Begin(ctx, attempt, runnerallocation.RuntimeOperationCreate); err == nil {
		t.Fatal("new boot resumed old process authority")
	}
	if _, err := reopened.Issue(ctx, recovered, runnerallocation.StepEnsureIdentity); err == nil {
		t.Fatal("new boot issued from retained caller state")
	}
}

// RUN-10: unpublished partial writes do not corrupt the committed prefix, but
// a hash-consistent committed revision may not replace the plan mid-operation.
func TestJournalRejectsAuthorityRewriteAndIgnoresUnpublishedStage(t *testing.T) {
	ctx := context.Background()
	journal, err := New(filepath.Join(t.TempDir(), "ownership"))
	if err != nil {
		t.Fatal(err)
	}
	attempt := testAttempt(21)
	progress, err := journal.Begin(ctx, attempt, runnerallocation.RuntimeOperationCreate)
	if err != nil {
		t.Fatal(err)
	}
	directory, err := journal.operationDirectory(attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, ".revision.interrupted"), []byte("{\"progress\":"), 0o600); err != nil {
		t.Fatal(err)
	}
	issued, err := journal.Issue(ctx, progress, runnerallocation.StepEnsureIdentity)
	if err != nil {
		t.Fatalf("unpublished stage blocked valid committed prefix: %v", err)
	}
	if _, err := journal.Issue(ctx, progress, runnerallocation.StepEnsureIdentity); err == nil {
		t.Fatal("stale snapshot issued a second host effect")
	}
	previous, err := os.ReadFile(filepath.Join(directory, revisionName(issued.Revision)))
	if err != nil {
		t.Fatal(err)
	}
	forged := issued
	forged.Plan = issued.Plan.Clone()
	forged.Plan.Paths.RawSocket = "/different/daemon.sock"
	forged.PlanDigest = forged.Plan.Digest()
	forged.IdentityDigest = forged.Plan.IdentityDigest()
	forged.NextStep++
	forged.ActiveStep = nil
	forged.Revision++
	forged.PredecessorSHA256 = digestBytes(previous)
	encoded, err := encodeRevision(forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, revisionName(forged.Revision)), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Begin(ctx, attempt, runnerallocation.RuntimeOperationCreate); err == nil {
		t.Fatal("hash-consistent authority rewrite was adopted")
	}
}

// RUN-09, RUN-10: real file-backed create/removal histories must retain each
// completed effect, seal only after all steps, and replay without further writes.
func TestJournalSealsCompleteLifecycleAndReplaysPinnedResult(t *testing.T) {
	for _, operation := range []runnerallocation.RuntimeOperation{
		runnerallocation.RuntimeOperationCreate, runnerallocation.RuntimeOperationRemove,
	} {
		t.Run(string(operation), func(t *testing.T) {
			ctx := context.Background()
			journal, err := New(filepath.Join(t.TempDir(), "ownership"))
			if err != nil {
				t.Fatal(err)
			}
			attempt := testAttempt(22)
			progress, err := journal.Begin(ctx, attempt, operation)
			if err != nil {
				t.Fatal(err)
			}
			owner := runnerallocation.RunnerRuntimeEvidence{
				ContainerID: strings.Repeat(
					"c",
					64,
				), DaemonNonce: strings.Repeat("d", 64), SocketDevice: 1, SocketInode: 2,
			}
			for _, step := range operationSteps(operation) {
				progress, err = journal.Issue(ctx, progress, step)
				if err != nil {
					t.Fatal(err)
				}
				evidence := runnerallocation.RunnerRuntimeStepEvidence{
					Step:  step,
					State: runnerallocation.RuntimeEffectApplied,
				}
				if operation == runnerallocation.RuntimeOperationRemove {
					evidence.State = runnerallocation.RuntimeEffectAbsent
					evidence.ReceiptSHA256 = "sha256:" + strings.Repeat("e", 64)
				} else if step == runnerallocation.StepStartRunner {
					evidence.Ownership = &owner
				}
				progress, err = journal.Checkpoint(ctx, progress, evidence)
				if err != nil {
					t.Fatal(err)
				}
			}
			want := runnerallocation.RuntimeStatusReady
			if operation == runnerallocation.RuntimeOperationRemove {
				want = runnerallocation.RuntimeStatusRemoved
				err = journal.Removed(ctx, progress)
			} else {
				err = journal.Ready(ctx, progress, owner)
			}
			if err != nil {
				t.Fatal(err)
			}
			result, err := journal.Begin(ctx, attempt, operation)
			if err != nil || result.Status != want || result.Revision != progress.Revision+1 ||
				result.Plan.Digest() != attempt.Plan.Digest() {
				t.Fatalf("sealed result not recovered: %v, %v", result.Status, err)
			}
			if err := journal.Fail(ctx, result); err == nil {
				t.Fatal("terminal history was rewritten")
			}
		})
	}
}
