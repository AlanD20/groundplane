package swarmgit

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/swarmcheck"
)

// Rationale: Git diff evidence must preserve unusual valid filenames through NUL framing.
func TestDiffParsesNULDelimitedPaths(t *testing.T) {
	// Rationale: filenames may contain whitespace, so newline parsing would let
	// lease validation inspect different bytes than Git reported.
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		if !containsArg(opts.Args, "diff") {
			t.Fatalf("expected git diff call, got %v", opts.Args)
		}
		if !containsArg(opts.Args, "--literal-pathspecs") {
			t.Fatal("git calls must force literal pathspec handling")
		}
		return runner.Result{Stdout: []byte("M\x00dir/file name.go\x00A\x00new.go\x00")}, nil
	}
	changed, err := NewInspector(t.TempDir(), fake).diff(
		context.Background(),
		"1111111111111111111111111111111111111111",
		"2222222222222222222222222222222222222222",
	)
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if len(changed) != 2 || changed[0].Path != "dir/file name.go" || changed[1].Path != "new.go" {
		t.Fatalf("unexpected changed paths: %#v", changed)
	}
}

// Rationale: unknown Git status codes must fail closed instead of weakening lease validation.
func TestDiffRejectsUnknownStatus(t *testing.T) {
	// Rationale: a newly introduced Git status must fail closed until its path
	// record grammar and lease semantics are explicitly understood.
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, _ runner.RunCmdOpts) (runner.Result, error) {
		return runner.Result{Stdout: []byte("R100\x00old.go\x00new.go\x00")}, nil
	}
	_, err := NewInspector(t.TempDir(), fake).diff(
		context.Background(),
		"1111111111111111111111111111111111111111",
		"2222222222222222222222222222222222222222",
	)
	if err == nil {
		t.Fatal("expected an unknown-status error")
	}
}

// Rationale: signed commit evidence must parse the exact object, parents, tree, and fingerprint.
func TestCommitParsesExactSignedEvidence(t *testing.T) {
	// Rationale: delivery and snapshot acceptance depend on each NUL-delimited
	// field retaining its exact position, including the observed parent.
	const (
		commit = "1111111111111111111111111111111111111111"
		parent = "2222222222222222222222222222222222222222"
		tree   = "3333333333333333333333333333333333333333"
		signer = "4444444444444444444444444444444444444444"
	)
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, _ runner.RunCmdOpts) (runner.Result, error) {
		return runner.Result{Stdout: bytes.Join(
			[][]byte{[]byte(commit), []byte(parent), []byte(tree), []byte("G"), []byte(signer)},
			[]byte{0},
		)}, nil
	}
	evidence, err := NewInspector(t.TempDir(), fake).commit(context.Background(), commit)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if evidence.Commit != swarmcheck.CommitID(commit) ||
		!sameValues(evidence.Parents, []swarmcheck.CommitID{swarmcheck.CommitID(parent)}) ||
		evidence.Tree != tree ||
		evidence.Signature != "G" ||
		evidence.SignerFingerprint != signer {
		t.Fatalf("unexpected evidence: %#v", evidence)
	}
}

// Rationale: a real detached worktree must be represented as state rather than parsed as a branch ref.
func TestHeadAttachmentRecognizesRealDetachedRepository(t *testing.T) {
	root := t.TempDir()
	inspector := NewInspector(root, runner.New(nil))
	ctx := context.Background()
	if _, err := inspector.git(ctx, "init", "--initial-branch=main"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "tracked.txt"), []byte("tracked\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := inspector.git(ctx, "add", "tracked.txt"); err != nil {
		t.Fatal(err)
	}
	tree, err := inspector.writeTree(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result, err := inspector.gitWithEnv(
		ctx,
		syntheticCommitEnvironment(),
		"-c", "user.name=Groundplane Swarm Checker",
		"-c", "user.email=swarm-check@localhost",
		"commit-tree", string(tree), "-m", "detached test",
	)
	if err != nil {
		t.Fatal(err)
	}
	commit, ok := exactCommitID(result.Stdout)
	if !ok {
		t.Fatal("commit-tree returned malformed commit")
	}
	if _, err := inspector.git(ctx, "update-ref", "refs/heads/main", string(commit)); err != nil {
		t.Fatal(err)
	}
	if _, err := inspector.git(ctx, "checkout", "--detach", string(commit)); err != nil {
		t.Fatal(err)
	}
	attachment, err := inspector.headAttachment(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !attachment.Detached || attachment.Ref != "" {
		t.Fatalf("detached attachment = %#v", attachment)
	}
}

// Rationale: replacement Git evidence must diff from its predecessor and enforce the narrow attempt lease.
func TestReserveSnapshotDiffUsesPredecessorAndNarrowLease(t *testing.T) {
	base := swarmcheck.CommitID(strings.Repeat("1", 40))
	predecessor := swarmcheck.CommitID(strings.Repeat("2", 40))
	commit := swarmcheck.CommitID(strings.Repeat("3", 40))
	tree := swarmcheck.TreeID(strings.Repeat("4", 40))
	signer := swarmcheck.Fingerprint(strings.Repeat("5", 40))
	path := swarmcheck.RepoPath("internal/feature/a/remediation/fix.go")
	snapshot := swarmcheck.Snapshot{
		Lane: "lane-a", Attempt: "attempt-2", Author: "remediator-a",
		Commit: commit, Parent: predecessor, Replaces: predecessor, Tree: tree,
		Signature: "G", SignerFingerprint: signer,
		RemediationLease: "internal/feature/a/remediation",
	}
	manifest := swarmcheck.Manifest{
		Base: swarmcheck.Base{Commit: base}, PrimarySigner: signer,
		Writers: []swarmcheck.Writer{{
			Lane: "lane-a", Leases: []swarmcheck.RepoPath{"internal/feature/a"},
		}},
	}
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		switch {
		case containsArg(opts.Args, "show"):
			return runner.Result{Stdout: bytes.Join(
				[][]byte{
					[]byte(commit), []byte(predecessor), []byte(tree), []byte("G"), []byte(signer),
				},
				[]byte{0},
			)}, nil
		case containsArg(opts.Args, "diff"):
			if !containsConsecutiveArgs(opts.Args, string(predecessor), string(commit)) ||
				containsArg(opts.Args, base) {
				t.Fatalf("replacement diff did not use predecessor: %v", opts.Args)
			}
			return runner.Result{Stdout: []byte("M\x00" + string(path) + "\x00")}, nil
		case containsArg(opts.Args, "ls-tree"):
			return runner.Result{Stdout: []byte(
				"100644 blob 6666666666666666666666666666666666666666\t" + string(path) + "\x00",
			)}, nil
		default:
			t.Fatalf("unexpected Git command: %v", opts.Args)
			return runner.Result{}, nil
		}
	}
	if err := NewInspector(t.TempDir(), fake).validateSnapshotEvidence(
		context.Background(),
		manifest,
		snapshot,
	); err != nil {
		t.Fatalf("reserve snapshot evidence: %v", err)
	}
}

// Rationale: integration receipts must bind Git-computed cumulative merges in manifest order.
func TestExpectedPrefixTreesUsesGitComputedCumulativeTrees(t *testing.T) {
	const (
		base      = "1111111111111111111111111111111111111111"
		snapshotA = "2222222222222222222222222222222222222222"
		snapshotB = "3333333333333333333333333333333333333333"
		treeA     = "4444444444444444444444444444444444444444"
		treeB     = "5555555555555555555555555555555555555555"
		commitA   = "6666666666666666666666666666666666666666"
		commitB   = "7777777777777777777777777777777777777777"
	)
	fake := runner.NewFake()
	mergeCalls := 0
	commitCalls := 0
	fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		switch {
		case containsArg(opts.Args, "merge-tree"):
			mergeCalls++
			if mergeCalls == 1 {
				if !containsConsecutiveArgs(opts.Args, base, snapshotA) {
					t.Fatalf("first merge inputs are not base and lane A: %v", opts.Args)
				}
				return runner.Result{Stdout: []byte(treeA + "\n")}, nil
			}
			if !containsConsecutiveArgs(opts.Args, commitA, snapshotB) {
				t.Fatalf("second merge is not cumulative: %v", opts.Args)
			}
			return runner.Result{Stdout: []byte(treeB + "\n")}, nil
		case containsArg(opts.Args, "commit-tree"):
			if containsEnv(opts.Env, "GIT_OBJECT_DIRECTORY=") ||
				!containsEnv(opts.Env, "GIT_AUTHOR_DATE="+syntheticCommitDate) ||
				!containsEnv(opts.Env, "GIT_COMMITTER_DATE="+syntheticCommitDate) {
				t.Fatalf("commit-tree environment is not deterministic: %v", opts.Env)
			}
			commitCalls++
			if commitCalls == 1 {
				if !containsConsecutiveArgs(opts.Args, "-p", base) {
					t.Fatalf("first synthetic prefix parent is not base: %v", opts.Args)
				}
				return runner.Result{Stdout: []byte(commitA + "\n")}, nil
			}
			if !containsConsecutiveArgs(opts.Args, "-p", commitA) {
				t.Fatalf("second synthetic prefix parent is not the first prefix: %v", opts.Args)
			}
			return runner.Result{Stdout: []byte(commitB + "\n")}, nil
		default:
			t.Fatalf("unexpected git command: %v", opts.Args)
			return runner.Result{}, nil
		}
	}
	manifest := swarmcheck.Manifest{
		Wave:        "wave-1",
		Base:        swarmcheck.Base{Commit: base},
		Integration: []swarmcheck.Lane{"lane-a", "lane-b"},
		Snapshots: []swarmcheck.Snapshot{
			{Lane: "lane-a", Commit: snapshotA},
			{Lane: "lane-b", Commit: snapshotB},
		},
		Active: []swarmcheck.ActiveSnapshot{
			{Lane: "lane-a", Snapshot: snapshotA},
			{Lane: "lane-b", Snapshot: snapshotB},
		},
		Reviews: []swarmcheck.ReviewReceipt{
			{Lane: "lane-a", Snapshot: snapshotA, Approved: true},
			{Lane: "lane-b", Snapshot: snapshotB, Approved: true},
		},
	}
	trees, err := NewInspector(
		t.TempDir(),
		fake,
	).expectedPrefixTrees(context.Background(), manifest)
	if err != nil {
		t.Fatalf("expected prefix trees: %v", err)
	}
	if len(trees) != 2 || trees[0] != treeA || trees[1] != treeB {
		t.Fatalf("unexpected prefix trees: %v", trees)
	}
}

// Rationale: conflict diagnostics must never be mistaken for one machine-computed tree object.
func TestExpectedPrefixTreesRejectsMalformedMergeOutput(t *testing.T) {
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		return runner.Result{
			Stdout: []byte("4444444444444444444444444444444444444444\nconflict\n"),
		}, nil
	}
	manifest := swarmcheck.Manifest{
		Wave:        "wave-1",
		Base:        swarmcheck.Base{Commit: "1111111111111111111111111111111111111111"},
		Integration: []swarmcheck.Lane{"lane-a"},
		Snapshots: []swarmcheck.Snapshot{{
			Lane: "lane-a", Commit: "2222222222222222222222222222222222222222",
		}},
		Active: []swarmcheck.ActiveSnapshot{{
			Lane: "lane-a", Snapshot: "2222222222222222222222222222222222222222",
		}},
		Reviews: []swarmcheck.ReviewReceipt{{
			Lane: "lane-a", Snapshot: "2222222222222222222222222222222222222222", Approved: true,
		}},
	}
	if _, err := NewInspector(
		t.TempDir(),
		fake,
	).expectedPrefixTrees(context.Background(), manifest); err == nil {
		t.Fatal("expected malformed merge output error")
	}
}

// Rationale: stored integration conflicts must be reproduced from the bound prefix and snapshot.
func TestStoredIntegrationConflictIsRecomputedFromExactPrefix(t *testing.T) {
	base := swarmcheck.CommitID(strings.Repeat("1", 40))
	snapshotA := swarmcheck.CommitID(strings.Repeat("2", 40))
	snapshotB := swarmcheck.CommitID(strings.Repeat("3", 40))
	prefixTree := swarmcheck.TreeID(strings.Repeat("4", 40))
	prefixCommit := swarmcheck.CommitID(strings.Repeat("5", 40))
	mergeCalls := 0
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		switch {
		case containsArg(opts.Args, "merge-tree"):
			mergeCalls++
			if mergeCalls == 1 {
				return runner.Result{Stdout: []byte(string(prefixTree) + "\n")}, nil
			}
			return runner.Result{ExitCode: 1, Stderr: []byte("conflict")}, nil
		case containsArg(opts.Args, "commit-tree"):
			return runner.Result{Stdout: []byte(string(prefixCommit) + "\n")}, nil
		default:
			t.Fatalf("unexpected Git command: %v", opts.Args)
			return runner.Result{}, nil
		}
	}
	manifest := swarmcheck.Manifest{
		Wave: "wave-1",
		Base: swarmcheck.Base{Commit: base},
		Snapshots: []swarmcheck.Snapshot{
			{Lane: "lane-a", Commit: snapshotA},
			{Lane: "lane-b", Commit: snapshotB},
		},
		Conflicts: []swarmcheck.IntegrationConflict{{
			Lane: "lane-b", Attempt: "attempt-1", Snapshot: snapshotB,
			PrefixSnapshots: []swarmcheck.CommitID{snapshotA}, PrefixCommit: prefixCommit,
		}},
	}
	inspector := NewInspector(t.TempDir(), fake)
	if err := inspector.validateStoredIntegrationConflicts(
		context.Background(),
		manifest,
	); err != nil {
		t.Fatalf("recompute stored conflict: %v", err)
	}
	mergeCalls = 0
	manifest.Conflicts[0].PrefixCommit = swarmcheck.CommitID(strings.Repeat("6", 40))
	if err := inspector.validateStoredIntegrationConflicts(
		context.Background(),
		manifest,
	); err == nil {
		t.Fatal("stored conflict accepted a substituted prefix commit")
	}
}

// Rationale: recursive ignored enumeration must not trust Git's collapsed directory marker.
func TestStatusEnumeratesIgnoredPathsBeyondCollapsedDirectoryMarker(t *testing.T) {
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		switch {
		case containsArg(opts.Args, "status"):
			if !containsArg(opts.Args, "--ignored=matching") {
				t.Fatal("writer status must request ignored entries")
			}
			return runner.Result{Stdout: []byte("!! .tmp/\x00")}, nil
		case containsArg(opts.Args, "ls-files") && containsArg(opts.Args, "--others"):
			return runner.Result{
				Stdout: []byte(".tmp/swarm/wave-1/artifacts/gate.json\x00"),
			}, nil
		case containsArg(opts.Args, "ls-files"):
			return runner.Result{}, nil
		default:
			t.Fatalf("unexpected git command: %v", opts.Args)
			return runner.Result{}, nil
		}
	}
	status, err := NewInspector(t.TempDir(), fake).status(context.Background(), true)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if len(status.Paths) != 1 || status.Paths[0].Status != statusIgnored ||
		status.Paths[0].Path != ".tmp/swarm/wave-1/artifacts/gate.json" {
		t.Fatalf("unexpected ignored paths: %#v", status.Paths)
	}
}

// Rationale: callers requesting ignored state must observe every ignored path as repository drift.
func TestCleanTreatsRequestedIgnoredPathsAsDirty(t *testing.T) {
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		switch {
		case containsArg(opts.Args, "status") && containsArg(opts.Args, "--ignored=matching"):
			return runner.Result{Stdout: []byte("!! .tmp/\x00")}, nil
		case containsArg(opts.Args, "ls-files") && containsArg(opts.Args, "--others"):
			return runner.Result{Stdout: []byte(".tmp/swarm/wave-1/artifact.json\x00")}, nil
		case containsArg(opts.Args, "status"), containsArg(opts.Args, "ls-files"):
			return runner.Result{}, nil
		default:
			t.Fatalf("unexpected git command: %v", opts.Args)
			return runner.Result{}, nil
		}
	}
	inspector := NewInspector(t.TempDir(), fake)
	dirty, err := inspector.clean(context.Background(), true)
	if err != nil {
		t.Fatalf("clean with ignored paths: %v", err)
	}
	if dirty {
		t.Fatal("requested ignored path must make the repository dirty")
	}
	clean, err := inspector.clean(context.Background(), false)
	if err != nil {
		t.Fatalf("clean without ignored paths: %v", err)
	}
	if !clean {
		t.Fatal("primary cleanliness must exclude ignored paths")
	}
}

// Rationale: replace refs can make signed-object and diff queries observe
// attacker-selected objects, so both independent Git controls must be fixed
// centrally even when a caller supplies a conflicting environment value.
func TestGitCommandsDisableReplacementObjectsCentrally(t *testing.T) {
	t.Setenv("GIT_NO_REPLACE_OBJECTS", "0")
	t.Setenv("GIT_DIR", "/attacker")
	t.Setenv("GIT_CONFIG_GLOBAL", "/attacker/config")
	t.Setenv("PATH", "/attacker")
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		if opts.Name != gitExecutable || opts.Timeout != gitCommandTimeout ||
			opts.CaptureLimitBytes != maxGitOutputBytes {
			t.Fatalf("Git execution controls are not compiled: %#v", opts)
		}
		if !containsArg(opts.Args, "--no-replace-objects") {
			t.Fatalf("Git argv does not disable replacements: %v", opts.Args)
		}
		if !opts.ReplaceEnv || countExactEnv(opts.Env, "GIT_NO_REPLACE_OBJECTS=1") != 1 ||
			containsExactEnv(opts.Env, "GIT_NO_REPLACE_OBJECTS=0") ||
			containsEnv(opts.Env, "GIT_DIR=") || containsEnv(opts.Env, "GIT_CONFIG_GLOBAL=") ||
			containsExactEnv(opts.Env, "PATH=/attacker") {
			t.Fatalf("Git environment does not force replacement resistance: %v", opts.Env)
		}
		return runner.Result{}, nil
	}
	_, err := NewInspector(t.TempDir(), fake).gitWithEnv(
		context.Background(),
		[]string{"GIT_AUTHOR_NAME=Allowed Author"},
		"status",
	)
	if err != nil {
		t.Fatalf("gitWithEnv: %v", err)
	}
}

// Rationale: delivery parents must keep accepted snapshots durable after temporary refs are deleted.
func TestDeliveryEvidenceUsesOrderedParentsWithDeletedTemporaryRefs(t *testing.T) {
	const (
		base      = "1111111111111111111111111111111111111111"
		snapshotA = "2222222222222222222222222222222222222222"
		snapshotB = "3333333333333333333333333333333333333333"
		delivery  = "4444444444444444444444444444444444444444"
		tree      = "5555555555555555555555555555555555555555"
		signer    = "6666666666666666666666666666666666666666"
	)
	manifest := swarmcheck.Manifest{
		Wave:          "wave-1",
		PrimarySigner: signer,
		TargetRef:     "refs/heads/main",
		Base:          swarmcheck.Base{Commit: base},
		Integration:   []swarmcheck.Lane{"lane-a", "lane-b"},
		Snapshots: []swarmcheck.Snapshot{
			{
				Lane:    "lane-a",
				Attempt: "attempt-1",
				Ref:     swarmcheck.SnapshotRef("wave-1", "lane-a", "attempt-1"),
				Commit:  snapshotA,
			},
			{
				Lane:    "lane-b",
				Attempt: "attempt-1",
				Ref:     swarmcheck.SnapshotRef("wave-1", "lane-b", "attempt-1"),
				Commit:  snapshotB,
			},
		},
		Active: []swarmcheck.ActiveSnapshot{
			{Lane: "lane-a", Attempt: "attempt-1", Snapshot: snapshotA},
			{Lane: "lane-b", Attempt: "attempt-1", Snapshot: snapshotB},
		},
		Delivery: &swarmcheck.DeliveryReceipt{
			Commit:            delivery,
			Parents:           []swarmcheck.CommitID{base, snapshotA, snapshotB},
			Tree:              tree,
			Signature:         "G",
			SignerFingerprint: signer,
		},
	}
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		switch {
		case containsArg(opts.Args, "show"):
			return runner.Result{Stdout: []byte(strings.Join(
				[]string{delivery, base + " " + snapshotA + " " + snapshotB, tree, "G", signer},
				"\x00",
			))}, nil
		case containsArg(opts.Args, "rev-parse"):
			return runner.Result{Stdout: []byte(delivery + "\n")}, nil
		case containsArg(opts.Args, "for-each-ref"):
			return runner.Result{}, nil
		default:
			t.Fatalf("unexpected Git command: %v", opts.Args)
			return runner.Result{}, nil
		}
	}
	inspector := NewInspector(t.TempDir(), fake)
	if err := inspector.validateDeliveryEvidence(
		context.Background(),
		manifest,
		swarmcheck.TreeID(tree),
	); err != nil {
		t.Fatalf("validateDeliveryEvidence: %v", err)
	}
	for _, snapshot := range manifest.Snapshots {
		if err := inspector.requireSnapshotRef(context.Background(), snapshot, false); err != nil {
			t.Fatalf("require deleted snapshot ref: %v", err)
		}
	}
}

// Rationale: gate evidence must bind its exact closed command, environment, candidate, and tree.
func TestGateArtifactRejectsCommandAndTreeMismatch(t *testing.T) {
	const (
		candidate = "1111111111111111111111111111111111111111"
		tree      = "2222222222222222222222222222222222222222"
		otherTree = "3333333333333333333333333333333333333333"
	)
	root := t.TempDir()
	state := swarmcheck.GateState{Head: candidate, Tree: tree, Clean: true}
	artifact := swarmcheck.NewGateArtifact(
		"wave-1",
		swarmcheck.GateRepositoryCI,
		repositoryCIExecutable,
		[]string{"ci"},
		[]string{"PATH=/usr/bin"},
		candidate,
		tree,
		state,
		state,
		0,
		nil,
		nil,
	)
	relativePath := swarmcheck.GateArtifactPath("wave-1", swarmcheck.GateRepositoryCI)
	data, digest, err := swarmcheck.EncodeGateArtifact(
		context.Background(),
		artifact,
	)
	if err != nil {
		t.Fatalf("EncodeGateArtifact: %v", err)
	}
	inspector := NewInspector(root, runner.NewFake())
	if err := inspector.writeArtifactAtomically(
		context.Background(), "wave-1", relativePath, data,
	); err != nil {
		t.Fatalf("writeArtifactAtomically: %v", err)
	}
	manifest := swarmcheck.Manifest{
		Wave: "wave-1",
		Delivery: &swarmcheck.DeliveryReceipt{
			Commit: candidate,
			Tree:   tree,
		},
	}
	receipt := swarmcheck.GateReceipt{
		Wave:      "wave-1",
		Gate:      swarmcheck.GateOperatorVerifier,
		Candidate: candidate,
		Tree:      tree,
		Artifact:  relativePath,
		Digest:    digest,
	}
	if err := inspector.requireGateArtifact(
		context.Background(), manifest, receipt,
	); err == nil {
		t.Fatal("gate artifact satisfied a different closed command")
	}

	receipt.Gate = swarmcheck.GateRepositoryCI
	receipt.Tree = otherTree
	if err := inspector.requireGateArtifact(
		context.Background(), manifest, receipt,
	); err == nil {
		t.Fatal("gate artifact satisfied a different candidate tree")
	}

	receipt.Tree = tree
	if err := inspector.requireGateArtifact(
		context.Background(), manifest, receipt,
	); err == nil {
		t.Fatal("gate artifact satisfied a different controlled environment")
	}
}

// Rationale: canceled validation must terminate Git subprocess work through the shared Runner.
func TestGitCommandPropagatesCancellation(t *testing.T) {
	fake := runner.NewFake()
	fake.RunFunc = func(ctx context.Context, _ runner.RunCmdOpts) (runner.Result, error) {
		<-ctx.Done()
		return runner.Result{}, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := NewInspector(t.TempDir(), fake).git(ctx, "status")
		result <- err
	}()
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("git cancellation error = %v", err)
	}
}

func containsArg[T ~string](args []string, wanted T) bool {
	for _, arg := range args {
		if arg == string(wanted) {
			return true
		}
	}
	return false
}

func containsConsecutiveArgs(args []string, first, second string) bool {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == first && args[index+1] == second {
			return true
		}
	}
	return false
}

func containsEnv(environment []string, prefix string) bool {
	for _, value := range environment {
		if bytes.HasPrefix([]byte(value), []byte(prefix)) {
			return true
		}
	}
	return false
}

func containsExactEnv(environment []string, wanted string) bool {
	for _, value := range environment {
		if value == wanted {
			return true
		}
	}
	return false
}

func countExactEnv(environment []string, wanted string) int {
	count := 0
	for _, value := range environment {
		if value == wanted {
			count++
		}
	}
	return count
}
