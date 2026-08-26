package swarmgit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/swarmcheck"
)

// Rationale: closed gates must use trusted executables, sanitized toolchains, timeouts, and capture caps.
func TestGateCommandsUseTrustedExecutablesControlledEnvironmentAndBounds(t *testing.T) {
	root := t.TempDir()
	verifier := filepath.Join(root, filepath.FromSlash(operatorVerifierPath))
	if err := os.MkdirAll(filepath.Dir(verifier), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(verifier, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	gopath := t.TempDir()
	gopathBin := filepath.Join(gopath, "bin")
	if err := os.MkdirAll(gopathBin, 0o700); err != nil {
		t.Fatal(err)
	}
	repositoryBin := filepath.Join(root, "fake-toolchain", "bin")
	if err := os.MkdirAll(repositoryBin, 0o700); err != nil {
		t.Fatal(err)
	}
	miseRoot := t.TempDir()
	pinnedNodeBin := filepath.Join(miseRoot, "installs", "node", "24.19.0", "bin")
	if err := os.MkdirAll(pinnedNodeBin, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".node-version"), []byte("24.19.0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOPATH", gopath)
	t.Setenv("MISE_DATA_DIR", miseRoot)
	t.Setenv("PATH", strings.Join([]string{repositoryBin, gopathBin, "/attacker"}, string(os.PathListSeparator)))
	t.Setenv("GIT_DIR", "/attacker/repository")
	inspector := NewInspector(root, runner.NewFake())
	for _, gate := range []swarmcheck.GateID{
		swarmcheck.GateRepositoryCI,
		swarmcheck.GateOperatorVerifier,
	} {
		command, err := inspector.gateCommand(context.Background(), gate)
		if err != nil {
			t.Fatalf("gateCommand(%s): %v", gate, err)
		}
		if !filepath.IsAbs(command.executable) || command.timeout <= 0 ||
			gateCaptureLimitBytes <= 0 {
			t.Fatalf(
				"gate %s lacks compiled executable/timeout/capture controls: %#v",
				gate,
				command,
			)
		}
		if containsExactEnv(command.environment, "PATH=/attacker") ||
			containsEnv(command.environment, "GIT_DIR=") {
			t.Fatalf("gate %s inherited redirecting environment: %v", gate, command.environment)
		}
		path := environmentValue(command.environment, "PATH")
		if !containsPath(filepath.SplitList(path), gopathBin) ||
			!containsPath(filepath.SplitList(path), pinnedNodeBin) ||
			containsPath(filepath.SplitList(path), repositoryBin) {
			t.Fatalf("gate %s trusted PATH = %q", gate, path)
		}
	}
}

// Rationale: failed operator gates need actionable diagnostics without emitting unbounded output.
func TestGateFailureDiagnosticIsBoundedAndPrefersStderr(t *testing.T) {
	stderr := strings.Repeat("e", gateDiagnosticPrefixBytes+100)
	message := gateFailureMessage(
		swarmcheck.GateOperatorVerifier,
		runner.Result{ExitCode: 7, Stdout: []byte("less useful"), Stderr: []byte(stderr)},
		errors.New("gate failed"),
	)
	if !strings.Contains(
		message,
		"stderr prefix: "+strings.Repeat("e", gateDiagnosticPrefixBytes),
	) ||
		strings.Contains(message, "less useful") ||
		len(message) > gateDiagnosticPrefixBytes+256 {
		t.Fatalf("unexpected bounded gate diagnostic: %q", message)
	}
}

// Rationale: overflow, timeout, and cancellation must fail a gate before any passing proof exists.
func TestGateRunFailsOnOutputOverflowTimeoutAndCancellation(t *testing.T) {
	inspector := NewInspector(t.TempDir(), runner.New(nil))
	_, err := inspector.runGateCommand(context.Background(), gateCommand{
		executable: "/usr/bin/head",
		args:       []string{"-c", "200000", "/dev/zero"},
		timeout:    5 * time.Second,
	})
	if err == nil {
		t.Fatal("gate output overflow succeeded")
	}
	_, err = inspector.runGateCommand(context.Background(), gateCommand{
		executable: "/usr/bin/sleep",
		args:       []string{"1"},
		timeout:    10 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("gate timeout succeeded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = inspector.runGateCommand(ctx, gateCommand{
		executable: "/usr/bin/sleep",
		args:       []string{"1"},
		timeout:    5 * time.Second,
	})
	if err == nil {
		t.Fatal("canceled gate succeeded")
	}
}

// Rationale: integration permits its exact staged prefix but no other primary repository drift.
func TestIntegrationDriftAllowsOnlyExpectedStagingAndWaveIgnoredState(t *testing.T) {
	tests := []struct {
		name       string
		status     string
		ignored    string
		wantReject bool
	}{
		{name: "staged tracked", status: "M  internal/a.go\x00"},
		{
			name: "wave ignored", status: "!! .tmp/\x00",
			ignored: ".tmp/swarm/wave-1/artifacts/gate.json\x00",
		},
		{name: "unstaged tracked", status: " M internal/a.go\x00", wantReject: true},
		{name: "untracked", status: "?? stray.txt\x00", wantReject: true},
		{
			name: "collapsed tmp with unrelated ignored content", status: "!! .tmp/\x00",
			ignored: "build/generated/output.bin\x00", wantReject: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := runner.NewFake()
			fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
				if containsArg(opts.Args, "status") {
					return runner.Result{Stdout: []byte(test.status)}, nil
				}
				if containsArg(opts.Args, "ls-files") && containsArg(opts.Args, "--others") {
					return runner.Result{Stdout: []byte(test.ignored)}, nil
				}
				if containsArg(opts.Args, "ls-files") {
					return runner.Result{}, nil
				}
				t.Fatalf("unexpected Git command: %v", opts.Args)
				return runner.Result{}, nil
			}
			err := NewInspector(t.TempDir(), fake).validateIntegrationDrift(
				context.Background(), swarmcheck.Manifest{Wave: "wave-1"},
			)
			if (err != nil) != test.wantReject {
				t.Fatalf(
					"validateIntegrationDrift error = %v, want reject %t",
					err,
					test.wantReject,
				)
			}
		})
	}
}

// Rationale: complete must enumerate the whole wave namespace rather than only declared resources.
func TestCompleteRejectsAnyRetainedWaveRefOrWorktree(t *testing.T) {
	root := t.TempDir()
	tests := []struct {
		name         string
		refs         string
		worktreePath string
	}{
		{name: "extra ref", refs: "refs/heads/groundplane/swarm/wave-1/unlisted/attempt-9\n"},
		{
			name:         "extra worktree",
			worktreePath: filepath.Join(root, ".tmp", "swarm", "wave-1", "worktrees", "unlisted"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := runner.NewFake()
			fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
				switch {
				case containsArg(opts.Args, "for-each-ref"):
					return runner.Result{Stdout: []byte(test.refs)}, nil
				case containsArg(opts.Args, "worktree"):
					return runner.Result{
						Stdout: []byte(
							"worktree " + test.worktreePath + "\nHEAD " + strings.Repeat(
								"a",
								40,
							) + "\ndetached\n",
						),
					}, nil
				default:
					t.Fatalf("unexpected Git command: %v", opts.Args)
					return runner.Result{}, nil
				}
			}
			if err := NewInspector(root, fake).requireWaveResourcesAbsent(
				context.Background(), swarmcheck.Manifest{Wave: "wave-1"},
			); err == nil {
				t.Fatal("complete accepted a retained wave resource")
			}
		})
	}
}

// Rationale: an unregistered but present declared worktree directory remains mutable temporary state.
func TestCompleteRejectsPrunedButPresentDeclaredWorktreeDirectory(t *testing.T) {
	root := t.TempDir()
	worktree := swarmcheck.WorktreePath("wave-1", "lane-a")
	if err := os.MkdirAll(
		filepath.Join(root, filepath.FromSlash(string(worktree))),
		0o700,
	); err != nil {
		t.Fatal(err)
	}
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		if containsArg(opts.Args, "for-each-ref") || containsArg(opts.Args, "worktree") {
			return runner.Result{}, nil
		}
		t.Fatalf("unexpected Git command: %v", opts.Args)
		return runner.Result{}, nil
	}
	manifest := swarmcheck.Manifest{
		Wave:    "wave-1",
		Writers: []swarmcheck.Writer{{Lane: "lane-a", Worktree: worktree}},
	}
	if err := NewInspector(root, fake).requireWaveResourcesAbsent(
		context.Background(),
		manifest,
	); err == nil {
		t.Fatal("complete accepted a pruned but still-present worktree directory")
	}
}

// Rationale: gate before-and-after state must apply the same recursive ignored-path invariant.
func TestGateStateRejectsIgnoredDriftOutsideWaveRoot(t *testing.T) {
	commit := swarmcheck.CommitID(strings.Repeat("a", 40))
	tree := swarmcheck.TreeID(strings.Repeat("b", 40))
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		switch {
		case containsArg(opts.Args, "symbolic-ref"):
			return runner.Result{Stdout: []byte("refs/heads/main\n")}, nil
		case containsArg(opts.Args, "rev-parse"):
			return runner.Result{Stdout: []byte(string(commit) + "\n")}, nil
		case containsArg(opts.Args, "write-tree"):
			return runner.Result{Stdout: []byte(string(tree) + "\n")}, nil
		case containsArg(opts.Args, "status"):
			return runner.Result{Stdout: []byte("!! .tmp/\x00")}, nil
		case containsArg(opts.Args, "ls-files") && containsArg(opts.Args, "--others"):
			return runner.Result{Stdout: []byte("unrelated/generated.bin\x00")}, nil
		case containsArg(opts.Args, "ls-files"):
			return runner.Result{}, nil
		default:
			t.Fatalf("unexpected Git command: %v", opts.Args)
			return runner.Result{}, nil
		}
	}
	if _, err := NewInspector(t.TempDir(), fake).gateState(
		context.Background(),
		swarmcheck.Manifest{Wave: "wave-1", TargetRef: "refs/heads/main"},
	); err == nil {
		t.Fatal("gate state accepted ignored drift outside the exact wave root")
	}
}

// Rationale: writing CLI validation must prove every partial snapshot and writer-report pair with Git.
func TestValidateWritingEvidenceRejectsForgedSnapshotAndReport(t *testing.T) {
	root := t.TempDir()
	manifest, reportData := writingEvidenceManifest()
	inspector := NewInspector(root, runner.NewFake())
	if err := inspector.writeArtifactAtomically(
		context.Background(), manifest.Wave, manifest.WriterReports[0].Path, reportData,
	); err != nil {
		t.Fatal(err)
	}
	base := manifest.Base.Commit
	snapshot := manifest.Snapshots[0]
	setRunner := func(snapshotTree swarmcheck.TreeID) {
		fake := runner.NewFake()
		fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
			switch {
			case containsArg(opts.Args, "show") && containsArg(opts.Args, base):
				return commitResult(base, "", manifest.Base.Tree, "G", manifest.PrimarySigner), nil
			case containsArg(opts.Args, "show") && containsArg(opts.Args, snapshot.Commit):
				return commitResult(
					snapshot.Commit,
					base,
					snapshotTree,
					"G",
					manifest.PrimarySigner,
				), nil
			case containsArg(opts.Args, "diff"):
				return runner.Result{}, nil
			case containsArg(opts.Args, "for-each-ref"):
				return runner.Result{Stdout: []byte(snapshot.Commit + "\n")}, nil
			default:
				t.Fatalf("unexpected Git command: %v", opts.Args)
				return runner.Result{}, nil
			}
		}
		inspector.Runner = fake
	}
	setRunner(snapshot.Tree)
	if err := inspector.ValidateWritingEvidence(context.Background(), manifest); err != nil {
		t.Fatalf("valid writing evidence: %v", err)
	}

	setRunner(swarmcheck.TreeID(strings.Repeat("f", 40)))
	if err := inspector.ValidateWritingEvidence(context.Background(), manifest); err == nil {
		t.Fatal("writing accepted forged live snapshot evidence")
	}

	setRunner(snapshot.Tree)
	forgedReport := manifest
	forgedReport.WriterReports = append([]swarmcheck.ArtifactReport(nil), manifest.WriterReports...)
	forgedReport.WriterReports[0].Digest = swarmcheck.Digest(strings.Repeat("0", 64))
	if err := inspector.ValidateWritingEvidence(context.Background(), forgedReport); err == nil {
		t.Fatal("writing accepted forged writer report bytes")
	}
}

// Rationale: a replacement snapshot must bind its immutable predecessor as parent and replacement target.
func TestManifestSnapshotAcceptsDeclaredRemediationParent(t *testing.T) {
	const (
		wave        = "wave-1"
		lane        = "lane-a"
		base        = "1111111111111111111111111111111111111111"
		predecessor = "2222222222222222222222222222222222222222"
		replacement = "3333333333333333333333333333333333333333"
		signer      = "4444444444444444444444444444444444444444"
	)
	snapshot := swarmcheck.Snapshot{
		Lane: lane, Attempt: "attempt-2", Author: "remediator-a",
		Ref: swarmcheck.SnapshotRef(wave, lane, "attempt-2"), Commit: replacement,
		Parent: predecessor, Replaces: predecessor,
		Tree:      swarmcheck.TreeID(strings.Repeat("5", 40)),
		Signature: "G", SignerFingerprint: signer,
		RemediationLease: "internal/feature/a/remediation",
	}
	manifest := swarmcheck.Manifest{
		Wave: wave, PrimarySigner: signer, Base: swarmcheck.Base{Commit: base},
		Snapshots: []swarmcheck.Snapshot{snapshot},
	}
	if err := validateManifestSnapshot(manifest, snapshot); err != nil {
		t.Fatalf("declared remediation parent: %v", err)
	}
	if !swarmcheck.ChangeAllowedForSnapshot(
		manifest,
		snapshot,
		"internal/feature/a/remediation/fix.go",
	) || swarmcheck.ChangeAllowedForSnapshot(manifest, snapshot, "internal/feature/a/other.go") {
		t.Fatal("reserve attempt did not enforce its narrow remediation lease")
	}
}

// Rationale: reserve-authored remediation must inspect the declared reserve worktree, never the broad writer worktree.
func TestValidateWritingRejectsWriterWorktreeForReserveAttempt(t *testing.T) {
	root := t.TempDir()
	manifest, _ := writingEvidenceManifest()
	manifest.Phase = swarmcheck.PhaseReview
	manifest.Reserves = []swarmcheck.RemediationReserve{{
		Name: "reserve-a", Identity: "remediator-a",
		Worktree: swarmcheck.ReserveWorktreePath(manifest.Wave, "reserve-a"),
	}}
	initial := manifest.Snapshots[0]
	reviewPath := swarmcheck.RepoPath(
		string(swarmcheck.ArtifactRoot(manifest.Wave)) + "/initial-review.json",
	)
	reviewDigest := swarmcheck.Digest(strings.Repeat("c", 64))
	manifest.ReviewReports = []swarmcheck.ArtifactReport{{
		Wave: manifest.Wave, Lane: initial.Lane, Attempt: initial.Attempt,
		Identity: "reviewer-a", Base: manifest.Base.Commit, Snapshot: initial.Commit,
		Path: reviewPath, Digest: reviewDigest,
	}}
	manifest.Reviews = []swarmcheck.ReviewReceipt{{
		Lane: initial.Lane, Attempt: initial.Attempt, Reviewer: "reviewer-a",
		Snapshot: initial.Commit, Report: reviewPath, ReportDigest: reviewDigest,
	}}
	replacement := swarmcheck.Snapshot{
		Lane: initial.Lane, Attempt: "attempt-2", Author: "remediator-a",
		Ref:    swarmcheck.SnapshotRef(manifest.Wave, initial.Lane, "attempt-2"),
		Commit: swarmcheck.CommitID(strings.Repeat("e", 40)),
		Parent: initial.Commit, Replaces: initial.Commit,
		Tree: swarmcheck.TreeID(strings.Repeat("f", 40)), Signature: "G",
		SignerFingerprint: manifest.PrimarySigner,
		RemediationLease:  "internal/feature/a/remediation",
	}
	manifest.Snapshots = append(manifest.Snapshots, replacement)
	manifest.Active[0] = swarmcheck.ActiveSnapshot{
		Lane: replacement.Lane, Attempt: replacement.Attempt, Snapshot: replacement.Commit,
	}
	manifest.WriterReports = append(manifest.WriterReports, swarmcheck.ArtifactReport{
		Wave: manifest.Wave, Lane: replacement.Lane, Attempt: replacement.Attempt,
		Identity: replacement.Author, Base: manifest.Base.Commit, Snapshot: replacement.Commit,
		Path: swarmcheck.RepoPath(
			string(swarmcheck.ArtifactRoot(manifest.Wave)) + "/replacement-writer.json",
		),
		Digest: swarmcheck.Digest(strings.Repeat("d", 64)),
	})
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		if containsArg(opts.Args, "show") {
			return commitResult(
				manifest.Base.Commit,
				"",
				manifest.Base.Tree,
				"G",
				manifest.PrimarySigner,
			), nil
		}
		t.Fatalf("unexpected Git command: %v", opts.Args)
		return runner.Result{}, nil
	}
	writerRoot := filepath.Join(
		root,
		filepath.FromSlash(string(manifest.Writers[0].Worktree)),
	)
	if err := NewInspector(writerRoot, fake).ValidateWriting(
		context.Background(),
		manifest,
		initial.Lane,
	); err == nil {
		t.Fatal("reserve remediation accepted inspection of the original writer worktree")
	}

	for _, phase := range []swarmcheck.Phase{swarmcheck.PhaseReview, swarmcheck.PhaseIntegrate} {
		phaseManifest := manifest
		phaseManifest.Phase = phase
		if phase == swarmcheck.PhaseIntegrate {
			phaseManifest.Conflicts = []swarmcheck.IntegrationConflict{{
				Lane: initial.Lane, Attempt: initial.Attempt, Snapshot: initial.Commit,
				PrefixCommit: phaseManifest.Base.Commit,
			}}
		}
		fullFake := runner.NewFake()
		fullFake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
			switch {
			case containsArg(opts.Args, "show"):
				ref := opts.Args[len(opts.Args)-1]
				switch ref {
				case string(phaseManifest.Base.Commit):
					return commitResult(
						phaseManifest.Base.Commit,
						"",
						phaseManifest.Base.Tree,
						"G",
						phaseManifest.PrimarySigner,
					), nil
				case string(initial.Commit):
					return commitResult(
						initial.Commit,
						initial.Parent,
						initial.Tree,
						"G",
						phaseManifest.PrimarySigner,
					), nil
				case string(replacement.Commit):
					return commitResult(
						replacement.Commit,
						replacement.Parent,
						replacement.Tree,
						"G",
						phaseManifest.PrimarySigner,
					), nil
				}
			case containsArg(opts.Args, "diff"):
				return runner.Result{}, nil
			case containsArg(opts.Args, "for-each-ref"):
				ref := opts.Args[len(opts.Args)-1]
				if ref == string(initial.Ref) {
					return runner.Result{Stdout: []byte(string(initial.Commit) + "\n")}, nil
				}
				return runner.Result{Stdout: []byte(string(replacement.Commit) + "\n")}, nil
			case containsArg(opts.Args, "merge-tree"):
				return runner.Result{ExitCode: 1, Stderr: []byte("conflict")}, nil
			case containsArg(opts.Args, "status"):
				if !strings.HasSuffix(
					opts.Dir,
					filepath.FromSlash(string(phaseManifest.Reserves[0].Worktree)),
				) {
					t.Fatalf("active reserve inspected wrong worktree %s", opts.Dir)
				}
				return runner.Result{
					Stdout: []byte("?? internal/feature/a/outside-remediation.go\x00"),
				}, nil
			case containsArg(opts.Args, "ls-files"):
				return runner.Result{}, nil
			case containsArg(opts.Args, "rev-parse") && containsArg(opts.Args, "--abbrev-ref"):
				return runner.Result{Stdout: []byte("HEAD\n")}, nil
			case containsArg(opts.Args, "rev-parse"):
				return runner.Result{Stdout: []byte(string(replacement.Parent) + "\n")}, nil
			default:
				t.Fatalf("unexpected Git command: %v", opts.Args)
			}
			return runner.Result{}, nil
		}
		if err := NewInspector(root, fullFake).ValidateIntegration(
			context.Background(),
			phaseManifest,
		); err == nil {
			t.Fatalf("phase %s accepted reserve worktree lease violation", phase)
		}
	}
}

// Rationale: full complete validation must reach snapshots through ordered delivery parents alone.
func TestCompleteValidatesDurableDeliveryParentsAfterTemporaryRefsAreDeleted(t *testing.T) {
	root := t.TempDir()
	inspector := NewInspector(root, runner.NewFake())
	manifest := completeEvidenceManifest(t, inspector)
	refOutput := ""
	worktreeOutput := ""
	base := manifest.Base.Commit
	snapshot := manifest.Snapshots[0]
	delivery := manifest.Delivery
	finalTree := manifest.Applied[0].PrimaryIndexTree
	if expected := swarmcheck.ExpectedDeliveryParents(
		manifest,
	); !sameValues(
		delivery.Parents,
		expected,
	) {
		t.Fatalf("fixture delivery parents = %v, expected %v", delivery.Parents, expected)
	}
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		switch {
		case containsArg(opts.Args, "show") && containsArg(opts.Args, base):
			return commitResult(base, "", manifest.Base.Tree, "G", manifest.PrimarySigner), nil
		case containsArg(opts.Args, "show") && containsArg(opts.Args, snapshot.Commit):
			return commitResult(
				snapshot.Commit,
				base,
				snapshot.Tree,
				"G",
				manifest.PrimarySigner,
			), nil
		case containsArg(opts.Args, "show") && containsArg(opts.Args, delivery.Commit):
			return commitResult(
				delivery.Commit,
				joinValues(delivery.Parents, " "),
				delivery.Tree,
				"G",
				manifest.PrimarySigner,
			), nil
		case containsArg(opts.Args, "diff"):
			return runner.Result{}, nil
		case containsArg(opts.Args, "for-each-ref"):
			return runner.Result{Stdout: []byte(refOutput)}, nil
		case containsArg(opts.Args, "worktree"):
			return runner.Result{Stdout: []byte(worktreeOutput)}, nil
		case containsArg(opts.Args, "symbolic-ref"):
			return runner.Result{Stdout: []byte(manifest.TargetRef + "\n")}, nil
		case containsArg(opts.Args, "merge-tree"):
			return runner.Result{Stdout: []byte(finalTree + "\n")}, nil
		case containsArg(opts.Args, "commit-tree"):
			return runner.Result{Stdout: []byte(strings.Repeat("9", 40) + "\n")}, nil
		case containsArg(opts.Args, "rev-parse"):
			return runner.Result{Stdout: []byte(delivery.Commit + "\n")}, nil
		default:
			t.Fatalf("unexpected Git command: %v", opts.Args)
			return runner.Result{}, nil
		}
	}
	inspector.Runner = fake
	if err := inspector.ValidateIntegration(context.Background(), manifest); err != nil {
		t.Fatalf("complete validation with deleted temporary refs: %v", err)
	}

	reordered := manifest
	reordered.Delivery = &swarmcheck.DeliveryReceipt{
		Commit:            delivery.Commit,
		Parents:           []swarmcheck.CommitID{snapshot.Commit, base},
		Tree:              delivery.Tree,
		Signature:         delivery.Signature,
		SignerFingerprint: delivery.SignerFingerprint,
		TargetRef:         delivery.TargetRef,
	}
	if err := inspector.ValidateIntegration(context.Background(), reordered); err == nil {
		t.Fatal("complete accepted reordered delivery parents")
	}

	refOutput = string(swarmcheck.SnapshotNamespace(manifest.Wave)) + "retained/attempt-9\n"
	if err := inspector.ValidateIntegration(context.Background(), manifest); err == nil {
		t.Fatal("complete accepted an unlisted retained wave ref")
	}
	refOutput = ""
	worktreeOutput = "worktree " + filepath.Join(
		root,
		".tmp",
		"swarm",
		string(manifest.Wave),
		"worktrees",
		"retained",
	) +
		"\nHEAD " + string(base) + "\ndetached\n"
	if err := inspector.ValidateIntegration(context.Background(), manifest); err == nil {
		t.Fatal("complete accepted an unlisted retained wave worktree")
	}
}

// Rationale: cancellation must reach every Git subprocess through the Runner
// so signal-triggered shutdown cannot strand checker operations.
func writingEvidenceManifest() (swarmcheck.Manifest, []byte) {
	const (
		wave     = "wave-1"
		lane     = "lane-a"
		attempt  = "attempt-1"
		base     = "1111111111111111111111111111111111111111"
		baseTree = "2222222222222222222222222222222222222222"
		commit   = "3333333333333333333333333333333333333333"
		tree     = "4444444444444444444444444444444444444444"
		signer   = "5555555555555555555555555555555555555555"
	)
	reportData := []byte("writer evidence\n")
	digest := sha256.Sum256(reportData)
	reportPath := swarmcheck.ArtifactRoot(wave) + "/lane-a-attempt-1-writer.json"
	manifest := swarmcheck.Manifest{
		Version:       1,
		Wave:          wave,
		Phase:         swarmcheck.PhaseWriting,
		PrimarySigner: signer,
		TargetRef:     "refs/heads/main",
		Base: swarmcheck.Base{
			Commit: base, Tree: baseTree, Signature: "G", SignerFingerprint: signer,
		},
		Writers: []swarmcheck.Writer{{
			Lane: lane, Identity: "writer-a", Worktree: swarmcheck.WorktreePath(wave, lane),
			Base: base, Leases: []swarmcheck.RepoPath{"internal/feature/a"},
		}},
		Reviewers: []swarmcheck.Reviewer{{Lane: lane, Identity: "reviewer-a"}},
		CrossAudits: []swarmcheck.CrossAudit{
			{
				Role:     swarmcheck.AuditProduct,
				Identity: "audit-a",
			},
			{
				Role:     swarmcheck.AuditArchitecture,
				Identity: "audit-b",
			},
			{
				Role:     swarmcheck.AuditOperations,
				Identity: "audit-c",
			},
		},
		Snapshots: []swarmcheck.Snapshot{{
			Lane: lane, Attempt: attempt, Author: "writer-a",
			Ref: swarmcheck.SnapshotRef(wave, lane, attempt), Commit: commit, Parent: base,
			Tree: tree, Signature: "G", SignerFingerprint: signer,
		}},
		Active: []swarmcheck.ActiveSnapshot{{Lane: lane, Attempt: attempt, Snapshot: commit}},
		WriterReports: []swarmcheck.ArtifactReport{{
			Wave: wave, Lane: lane, Attempt: attempt, Identity: "writer-a", Base: base,
			Snapshot: commit, Path: reportPath,
			Digest: swarmcheck.Digest(hex.EncodeToString(digest[:])),
		}},
		Integration: []swarmcheck.Lane{lane},
	}
	return manifest, reportData
}

func completeEvidenceManifest(t *testing.T, inspector *Inspector) swarmcheck.Manifest {
	t.Helper()
	manifest, writerData := writingEvidenceManifest()
	manifest.Phase = swarmcheck.PhaseComplete
	snapshot := manifest.Snapshots[0]
	writeReport := func(path swarmcheck.RepoPath, data []byte) swarmcheck.Digest {
		t.Helper()
		digest := sha256.Sum256(data)
		if err := inspector.writeArtifactAtomically(
			context.Background(), manifest.Wave, path, data,
		); err != nil {
			t.Fatal(err)
		}
		return swarmcheck.Digest(hex.EncodeToString(digest[:]))
	}
	manifest.WriterReports[0].Digest = writeReport(manifest.WriterReports[0].Path, writerData)
	reviewPath := swarmcheck.ArtifactRoot(manifest.Wave) + "/lane-a-attempt-1-review.json"
	reviewDigest := writeReport(reviewPath, []byte("review evidence\n"))
	manifest.ReviewReports = []swarmcheck.ArtifactReport{
		{
			Wave:     manifest.Wave,
			Lane:     snapshot.Lane,
			Attempt:  snapshot.Attempt,
			Identity: "reviewer-a",
			Base:     manifest.Base.Commit,
			Snapshot: snapshot.Commit,
			Path:     reviewPath,
			Digest:   reviewDigest,
		},
	}
	manifest.Reviews = []swarmcheck.ReviewReceipt{
		{
			Lane:         snapshot.Lane,
			Attempt:      snapshot.Attempt,
			Reviewer:     "reviewer-a",
			Snapshot:     snapshot.Commit,
			Approved:     true,
			Report:       reviewPath,
			ReportDigest: reviewDigest,
		},
	}
	snapshotSet := []swarmcheck.CommitID{snapshot.Commit}
	snapshotHash := sha256.Sum256([]byte(string(snapshot.Commit)))
	snapshotDigest := swarmcheck.Digest(hex.EncodeToString(snapshotHash[:]))
	for _, assignment := range manifest.CrossAudits {
		reportPath := swarmcheck.RepoPath(fmt.Sprintf(
			"%s/%s-%s.json",
			swarmcheck.ArtifactRoot(manifest.Wave),
			assignment.Role,
			assignment.Identity,
		))
		reportDigest := writeReport(
			reportPath,
			[]byte("audit "+string(assignment.Role)+"\n"),
		)
		manifest.AuditReports = append(manifest.AuditReports, swarmcheck.AuditReport{
			Wave: manifest.Wave, Identity: assignment.Identity, Role: assignment.Role,
			Base: manifest.Base.Commit, Snapshots: snapshotSet, SnapshotDigest: snapshotDigest,
			ReportDigest: reportDigest, Path: reportPath,
		})
		manifest.Audits = append(manifest.Audits, swarmcheck.AuditReceipt{
			Wave: manifest.Wave, Identity: assignment.Identity, Role: assignment.Role,
			Base: manifest.Base.Commit, Snapshots: snapshotSet, SnapshotDigest: snapshotDigest,
			Report: reportPath, ReportDigest: reportDigest, Approved: true,
		})
	}
	finalTree := swarmcheck.TreeID(strings.Repeat("6", 40))
	manifest.Applied = []swarmcheck.AppliedReceipt{{
		Lane: snapshot.Lane, Snapshot: snapshot.Commit, PrimaryIndexTree: finalTree,
	}}
	deliveryCommit := swarmcheck.CommitID(strings.Repeat("7", 40))
	manifest.Delivery = &swarmcheck.DeliveryReceipt{
		Commit:            deliveryCommit,
		Parents:           []swarmcheck.CommitID{manifest.Base.Commit, snapshot.Commit},
		Tree:              finalTree,
		Signature:         "G",
		SignerFingerprint: manifest.PrimarySigner,
		TargetRef:         manifest.TargetRef,
	}
	verifier := filepath.Join(inspector.Root, filepath.FromSlash(operatorVerifierPath))
	if err := os.MkdirAll(filepath.Dir(verifier), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(verifier, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	state := swarmcheck.GateState{Head: deliveryCommit, Tree: finalTree, Clean: true}
	for _, gate := range []swarmcheck.GateID{
		swarmcheck.GateRepositoryCI,
		swarmcheck.GateOperatorVerifier,
	} {
		command, err := inspector.gateCommand(context.Background(), gate)
		if err != nil {
			t.Fatal(err)
		}
		artifact := swarmcheck.NewGateArtifact(
			manifest.Wave, gate, command.executable, command.args, command.environment,
			deliveryCommit, finalTree,
			state, state, 0, nil, nil,
		)
		data, digest, err := swarmcheck.EncodeGateArtifact(context.Background(), artifact)
		if err != nil {
			t.Fatal(err)
		}
		path := swarmcheck.GateArtifactPath(manifest.Wave, gate)
		if err := inspector.writeArtifactAtomically(
			context.Background(),
			manifest.Wave,
			path,
			data,
		); err != nil {
			t.Fatal(err)
		}
		manifest.Gates = append(manifest.Gates, swarmcheck.GateReceipt{
			Wave: manifest.Wave, Gate: gate, Candidate: deliveryCommit, Tree: finalTree,
			Artifact: path, Digest: digest,
		})
	}
	return manifest
}

func commitResult[C ~string, P ~string, T ~string, F ~string](
	commit C,
	parents P,
	tree T,
	signature string,
	signer F,
) runner.Result {
	return runner.Result{Stdout: bytes.Join(
		[][]byte{
			[]byte(string(commit)),
			[]byte(string(parents)),
			[]byte(string(tree)),
			[]byte(signature),
			[]byte(string(signer)),
		},
		[]byte{0},
	)}
}

func joinValues[T ~string](values []T, separator string) string {
	parts := make([]string, len(values))
	for index, value := range values {
		parts[index] = string(value)
	}
	return strings.Join(parts, separator)
}

func environmentValue(environment []string, key string) string {
	prefix := key + "="
	for _, value := range environment {
		if strings.HasPrefix(value, prefix) {
			return strings.TrimPrefix(value, prefix)
		}
	}
	return ""
}

func containsPath(paths []string, wanted string) bool {
	for _, path := range paths {
		if path == wanted {
			return true
		}
	}
	return false
}
