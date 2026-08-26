package swarmgit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/swarmcheck"
)

// Rationale: full integrate validation must preserve conflict windows and resume only after current approvals.
func TestValidateIntegrationHandlesConflictRemediationLifecycle(t *testing.T) {
	root := t.TempDir()
	diskInspector := NewInspector(root, runner.NewFake())
	manifest := completeEvidenceManifest(t, diskInspector)
	manifest.Phase = swarmcheck.PhaseIntegrate
	manifest.Applied = nil
	manifest.Gates = nil
	manifest.Delivery = nil
	initial := manifest.Snapshots[0]
	manifest.Reserves = []swarmcheck.RemediationReserve{{
		Name: "reserve-a", Identity: "remediator-a",
		Worktree: swarmcheck.ReserveWorktreePath(manifest.Wave, "reserve-a"),
	}}
	manifest.Conflicts = []swarmcheck.IntegrationConflict{{
		Lane: initial.Lane, Attempt: initial.Attempt, Snapshot: initial.Commit,
		PrefixCommit: manifest.Base.Commit,
	}}
	writeReport := func(path swarmcheck.RepoPath, data string) swarmcheck.Digest {
		t.Helper()
		if err := diskInspector.writeArtifactAtomically(
			context.Background(),
			manifest.Wave,
			path,
			[]byte(data),
		); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256([]byte(data))
		return swarmcheck.Digest(hex.EncodeToString(digest[:]))
	}
	first := swarmcheck.Snapshot{
		Lane: initial.Lane, Attempt: "attempt-2", Author: "remediator-a",
		Ref:    swarmcheck.SnapshotRef(manifest.Wave, initial.Lane, "attempt-2"),
		Commit: swarmcheck.CommitID(strings.Repeat("8", 40)),
		Parent: initial.Commit, Replaces: initial.Commit,
		Tree: swarmcheck.TreeID(strings.Repeat("9", 40)), Signature: "G",
		SignerFingerprint: manifest.PrimarySigner,
		RemediationLease:  "internal/feature/a/remediation",
	}
	second := swarmcheck.Snapshot{
		Lane: initial.Lane, Attempt: "attempt-3", Author: "remediator-a",
		Ref:    swarmcheck.SnapshotRef(manifest.Wave, initial.Lane, "attempt-3"),
		Commit: swarmcheck.CommitID(strings.Repeat("5", 40)),
		Parent: first.Commit, Replaces: first.Commit,
		Tree: swarmcheck.TreeID(strings.Repeat("4", 40)), Signature: "G",
		SignerFingerprint: manifest.PrimarySigner,
		RemediationLease:  "internal/feature/a/remediation/second",
	}
	fake := runner.NewFake()
	mergeCalls := 0
	commitCalls := 0
	fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		switch {
		case containsArg(opts.Args, "show"):
			ref := opts.Args[len(opts.Args)-1]
			if ref == string(manifest.Base.Commit) {
				return commitResult(
					manifest.Base.Commit, "", manifest.Base.Tree, "G", manifest.PrimarySigner,
				), nil
			}
			for _, snapshot := range manifest.Snapshots {
				if ref == string(snapshot.Commit) {
					return commitResult(
						snapshot.Commit,
						snapshot.Parent,
						snapshot.Tree,
						"G",
						manifest.PrimarySigner,
					), nil
				}
			}
		case containsArg(opts.Args, "diff"):
			return runner.Result{}, nil
		case containsArg(opts.Args, "for-each-ref"):
			ref := opts.Args[len(opts.Args)-1]
			for _, snapshot := range manifest.Snapshots {
				if ref == string(snapshot.Ref) {
					return runner.Result{Stdout: []byte(string(snapshot.Commit) + "\n")}, nil
				}
			}
		case containsArg(opts.Args, "merge-tree"):
			mergeCalls++
			if opts.Args[len(opts.Args)-1] == string(initial.Commit) {
				return runner.Result{ExitCode: 1, Stderr: []byte("conflict")}, nil
			}
			return runner.Result{Stdout: []byte(strings.Repeat("3", 40) + "\n")}, nil
		case containsArg(opts.Args, "commit-tree"):
			commitCalls++
			return runner.Result{Stdout: []byte(strings.Repeat("2", 40) + "\n")}, nil
		case containsArg(opts.Args, "status"), containsArg(opts.Args, "ls-files"):
			return runner.Result{}, nil
		case containsArg(opts.Args, "rev-parse") && containsArg(opts.Args, "--abbrev-ref"):
			return runner.Result{Stdout: []byte("HEAD\n")}, nil
		case containsArg(opts.Args, "rev-parse"):
			assignment, exists := swarmcheck.ResolveWritingAssignment(manifest, initial.Lane)
			if !exists {
				t.Fatal("resolve active writing assignment")
			}
			return runner.Result{Stdout: []byte(string(assignment.Head) + "\n")}, nil
		case containsArg(opts.Args, "symbolic-ref"):
			return runner.Result{Stdout: []byte(string(manifest.TargetRef) + "\n")}, nil
		default:
			t.Fatalf("unexpected Git command: %v", opts.Args)
		}
		return runner.Result{}, nil
	}
	inspector := NewInspector(root, fake)
	validate := func(state string, expectedMergeCalls, expectedCommitCalls int) {
		t.Helper()
		mergeCalls = 0
		commitCalls = 0
		if err := inspector.ValidateIntegration(context.Background(), manifest); err != nil {
			t.Fatalf("%s: %v", state, err)
		}
		if mergeCalls != expectedMergeCalls || commitCalls != expectedCommitCalls {
			t.Fatalf(
				"%s ran merge-tree %d times and commit-tree %d times, want %d and %d",
				state,
				mergeCalls,
				commitCalls,
				expectedMergeCalls,
				expectedCommitCalls,
			)
		}
	}
	validate("conflict only", 1, 0)

	manifest.Snapshots = append(manifest.Snapshots, first)
	manifest.Active[0] = swarmcheck.ActiveSnapshot{
		Lane: first.Lane, Attempt: first.Attempt, Snapshot: first.Commit,
	}
	firstWriterPath := swarmcheck.RepoPath(
		string(swarmcheck.ArtifactRoot(manifest.Wave)) + "/first-remediation-writer.json",
	)
	manifest.WriterReports = append(manifest.WriterReports, swarmcheck.ArtifactReport{
		Wave: manifest.Wave, Lane: first.Lane, Attempt: first.Attempt,
		Identity: first.Author, Base: manifest.Base.Commit, Snapshot: first.Commit,
		Path: firstWriterPath, Digest: writeReport(firstWriterPath, "first writer\n"),
	})
	validate("unapproved first replacement", 1, 0)

	firstReviewPath := swarmcheck.RepoPath(
		string(swarmcheck.ArtifactRoot(manifest.Wave)) + "/first-remediation-review.json",
	)
	firstReviewDigest := writeReport(firstReviewPath, "first rejected review\n")
	manifest.ReviewReports = append(manifest.ReviewReports, swarmcheck.ArtifactReport{
		Wave: manifest.Wave, Lane: first.Lane, Attempt: first.Attempt,
		Identity: "reviewer-a", Base: manifest.Base.Commit, Snapshot: first.Commit,
		Path: firstReviewPath, Digest: firstReviewDigest,
	})
	manifest.Reviews = append(manifest.Reviews, swarmcheck.ReviewReceipt{
		Lane: first.Lane, Attempt: first.Attempt, Reviewer: "reviewer-a",
		Snapshot: first.Commit, Report: firstReviewPath, ReportDigest: firstReviewDigest,
	})
	manifest.Snapshots = append(manifest.Snapshots, second)
	manifest.Active[0] = swarmcheck.ActiveSnapshot{
		Lane: second.Lane, Attempt: second.Attempt, Snapshot: second.Commit,
	}
	secondWriterPath := swarmcheck.RepoPath(
		string(swarmcheck.ArtifactRoot(manifest.Wave)) + "/second-remediation-writer.json",
	)
	manifest.WriterReports = append(manifest.WriterReports, swarmcheck.ArtifactReport{
		Wave: manifest.Wave, Lane: second.Lane, Attempt: second.Attempt,
		Identity: second.Author, Base: manifest.Base.Commit, Snapshot: second.Commit,
		Path: secondWriterPath, Digest: writeReport(secondWriterPath, "second writer\n"),
	})
	validate("rejected first replacement with second pending", 1, 0)

	secondReviewPath := swarmcheck.RepoPath(
		string(swarmcheck.ArtifactRoot(manifest.Wave)) + "/second-remediation-review.json",
	)
	secondReviewDigest := writeReport(secondReviewPath, "second approved review\n")
	manifest.ReviewReports = append(manifest.ReviewReports, swarmcheck.ArtifactReport{
		Wave: manifest.Wave, Lane: second.Lane, Attempt: second.Attempt,
		Identity: "reviewer-a", Base: manifest.Base.Commit, Snapshot: second.Commit,
		Path: secondReviewPath, Digest: secondReviewDigest,
	})
	manifest.Reviews = append(manifest.Reviews, swarmcheck.ReviewReceipt{
		Lane: second.Lane, Attempt: second.Attempt, Reviewer: "reviewer-a",
		Snapshot: second.Commit, Approved: true,
		Report: secondReviewPath, ReportDigest: secondReviewDigest,
	})
	currentSnapshots := []swarmcheck.CommitID{second.Commit}
	currentSnapshotDigest := sha256.Sum256([]byte(string(second.Commit)))
	setDigest := swarmcheck.Digest(hex.EncodeToString(currentSnapshotDigest[:]))
	for _, assignment := range manifest.CrossAudits {
		path := swarmcheck.RepoPath(fmt.Sprintf(
			"%s/current-%s.json",
			swarmcheck.ArtifactRoot(manifest.Wave),
			assignment.Role,
		))
		reportDigest := writeReport(path, "current "+string(assignment.Role)+"\n")
		manifest.AuditReports = append(manifest.AuditReports, swarmcheck.AuditReport{
			Wave: manifest.Wave, Identity: assignment.Identity, Role: assignment.Role,
			Base: manifest.Base.Commit, Snapshots: currentSnapshots,
			SnapshotDigest: setDigest, ReportDigest: reportDigest, Path: path,
		})
		manifest.Audits = append(manifest.Audits, swarmcheck.AuditReceipt{
			Wave: manifest.Wave, Identity: assignment.Identity, Role: assignment.Role,
			Base: manifest.Base.Commit, Snapshots: currentSnapshots,
			SnapshotDigest: setDigest, Report: path, ReportDigest: reportDigest,
			Approved: true,
		})
	}
	validate("approved replacement resume", 2, 1)
}

// Rationale: pending remediation may rebuild only retained prefixes, never the conflicting active tail.
func TestPendingConflictPhaseStateNeverComputesTheFullPrefix(t *testing.T) {
	base := swarmcheck.CommitID(strings.Repeat("1", 40))
	baseTree := swarmcheck.TreeID(strings.Repeat("2", 40))
	retained := swarmcheck.CommitID(strings.Repeat("3", 40))
	conflicting := swarmcheck.CommitID(strings.Repeat("4", 40))
	replacement := swarmcheck.CommitID(strings.Repeat("5", 40))
	retainedTree := swarmcheck.TreeID(strings.Repeat("6", 40))
	prefixCommit := swarmcheck.CommitID(strings.Repeat("7", 40))
	manifest := swarmcheck.Manifest{
		Wave:  "wave-ordering",
		Phase: swarmcheck.PhaseIntegrate,
		Base:  swarmcheck.Base{Commit: base, Tree: baseTree},
		Snapshots: []swarmcheck.Snapshot{
			{Lane: "lane-a", Attempt: "attempt-1", Commit: retained, Parent: base},
			{Lane: "lane-b", Attempt: "attempt-1", Commit: conflicting, Parent: base},
		},
		Active: []swarmcheck.ActiveSnapshot{
			{Lane: "lane-a", Attempt: "attempt-1", Snapshot: retained},
			{Lane: "lane-b", Attempt: "attempt-1", Snapshot: conflicting},
		},
		Reviews: []swarmcheck.ReviewReceipt{{
			Lane: "lane-a", Attempt: "attempt-1", Snapshot: retained, Approved: true,
		}},
		Integration: []swarmcheck.Lane{"lane-a", "lane-b"},
		Applied: []swarmcheck.AppliedReceipt{{
			Lane: "lane-a", Snapshot: retained, PrimaryIndexTree: retainedTree,
		}},
		Conflicts: []swarmcheck.IntegrationConflict{{
			Lane: "lane-b", Attempt: "attempt-1", Snapshot: conflicting,
			PrefixSnapshots: []swarmcheck.CommitID{retained}, PrefixCommit: prefixCommit,
		}},
	}
	mergeCalls := 0
	commitCalls := 0
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		switch {
		case containsArg(opts.Args, "status"), containsArg(opts.Args, "ls-files"):
			return runner.Result{}, nil
		case containsArg(opts.Args, "merge-tree"):
			mergeCalls++
			if opts.Args[len(opts.Args)-1] != string(retained) {
				t.Fatalf("pending remediation merged active tail: %v", opts.Args)
			}
			return runner.Result{Stdout: []byte(string(retainedTree) + "\n")}, nil
		case containsArg(opts.Args, "commit-tree"):
			commitCalls++
			return runner.Result{Stdout: []byte(string(prefixCommit) + "\n")}, nil
		default:
			t.Fatalf("unexpected Git command: %v", opts.Args)
		}
		return runner.Result{}, nil
	}
	inspector := NewInspector(t.TempDir(), fake)
	validate := func(state string) {
		t.Helper()
		mergeCalls = 0
		commitCalls = 0
		err := inspector.validatePhaseState(context.Background(), manifest, Inspection{
			Head: base, IndexTree: retainedTree,
		})
		if err != nil {
			t.Fatalf("%s: %v", state, err)
		}
		if mergeCalls != 1 || commitCalls != 1 {
			t.Fatalf(
				"%s rebuilt %d merges and %d commits, want retained prefix only",
				state,
				mergeCalls,
				commitCalls,
			)
		}
	}
	validate("conflict only")

	manifest.Snapshots = append(manifest.Snapshots, swarmcheck.Snapshot{
		Lane: "lane-b", Attempt: "attempt-2", Commit: replacement,
		Parent: conflicting, Replaces: conflicting,
	})
	manifest.Active[1] = swarmcheck.ActiveSnapshot{
		Lane: "lane-b", Attempt: "attempt-2", Snapshot: replacement,
	}
	validate("pending replacement")
}
