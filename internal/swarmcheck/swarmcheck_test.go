package swarmcheck

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validWritingManifest() Manifest {
	base := CommitID(strings.Repeat("a", 40))
	return Manifest{
		Version:       1,
		Wave:          "wave-1",
		Phase:         PhaseWriting,
		PrimarySigner: Fingerprint(strings.Repeat("c", 40)),
		TargetRef:     "refs/heads/main",
		Base: Base{
			Commit:            base,
			Tree:              TreeID(strings.Repeat("b", 40)),
			Signature:         "G",
			SignerFingerprint: Fingerprint(strings.Repeat("c", 40)),
		},
		Writers: []Writer{
			{
				Lane:     "lane-a",
				Identity: "writer-a",
				Worktree: WorktreePath("wave-1", "lane-a"),
				Base:     base,
				Leases:   []RepoPath{"internal/feature/a"},
			},
		},
		Reviewers: []Reviewer{
			{Lane: "lane-a", Identity: "reviewer-a"},
		},
		CrossAudits: []CrossAudit{
			{Role: AuditProduct, Identity: "audit-a"},
			{Role: AuditArchitecture, Identity: "audit-b"},
			{Role: AuditOperations, Identity: "audit-c"},
		},
		Integration: []Lane{"lane-a"},
	}
}

// Rationale: signer identity must have one canonical representation before byte-for-byte evidence comparison.
func TestManifestRejectsNoncanonicalFingerprintCase(t *testing.T) {
	manifest := validWritingManifest()
	uppercase := Fingerprint(strings.Repeat("A", 40))
	manifest.PrimarySigner = uppercase
	manifest.Base.SignerFingerprint = uppercase
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("manifest accepted a noncanonical uppercase signing fingerprint")
	}
}

// Rationale: leases must remain segment-aware and disjoint from the compiled primary-only policy.
func TestPrimaryOnlyAndSegmentAwareLeases(t *testing.T) {
	manifest := validWritingManifest()
	if err := ValidateManifest(manifest); err != nil {
		t.Fatal(err)
	}
	if !IsPrimaryOnlyPath("internal/swarmcheck/extra.go") || !IsPrimaryOnlyPath("docs/mvp.md") {
		t.Fatal("primary-only policy did not classify protected paths")
	}
	if !ChangeAllowed(manifest, "lane-a", "internal/feature/a/file.go") ||
		ChangeAllowed(manifest, "lane-a", "internal/feature/ab/file.go") {
		t.Fatal("lease matching is not segment-aware")
	}
	manifest.Writers[0].Leases = []RepoPath{"docs/mvp.md"}
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("primary-only lease was accepted")
	}
	manifest.Writers[0].Leases = []RepoPath{"docs"}
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("ancestor lease containing a primary-only path was accepted")
	}
}

// Rationale: derived writer and reserve paths must never identify the same mutable worktree.
func TestWriterAndReserveDerivedWorktreesAreGloballyUnique(t *testing.T) {
	manifest := validWritingManifest()
	manifest.Writers[0].Lane = "reserve-fix"
	manifest.Writers[0].Worktree = WorktreePath(manifest.Wave, "reserve-fix")
	manifest.Reviewers[0].Lane = "reserve-fix"
	manifest.Integration[0] = "reserve-fix"
	manifest.Reserves = []RemediationReserve{{
		Name: "fix", Identity: "remediator-a",
		Worktree: ReserveWorktreePath(manifest.Wave, "fix"),
	}}
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("writer and reserve derived worktree collision was accepted")
	}
}

// Rationale: strict decoding must reject ambiguous or superseded manifest shapes at the boundary.
func TestReadManifestRejectsDuplicateAndUnknownFields(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "manifest.json")
	for _, content := range []string{
		`{"version":1,"version":1}`,
		`{"version":1,"unknown":true}`,
	} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadManifest(context.Background(), path); err == nil {
			t.Fatalf("ReadManifest accepted %s", content)
		}
	}
}

// Rationale: artifacts must stay beneath the exact derived wave root with canonical bounded paths.
func TestArtifactPathIsDerivedAndBounded(t *testing.T) {
	if err := ValidateArtifactPath(
		"wave-1",
		".tmp/swarm/wave-1/artifacts/report.json",
	); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{
		".tmp/swarm/wave-2/artifacts/report.json",
		".tmp/swarm/wave-1/artifacts/../manifest.json",
		".tmp/swarm/wave-1/artifacts/é.json",
	} {
		if err := ValidateArtifactPath("wave-1", RepoPath(value)); err == nil {
			t.Fatalf("accepted invalid artifact path %q", value)
		}
	}
}

// Rationale: writing and review must permit incremental atomic pairs without permitting partial pairs.
func TestWritingAndReviewAcceptAtomicPartialEvidence(t *testing.T) {
	manifest := validWritingManifest()
	manifest.Writers = append(manifest.Writers, Writer{
		Lane:     "lane-b",
		Identity: "writer-b",
		Worktree: WorktreePath(manifest.Wave, "lane-b"),
		Base:     manifest.Base.Commit,
		Leases:   []RepoPath{"internal/feature/b"},
	})
	manifest.Reviewers = append(manifest.Reviewers, Reviewer{
		Lane:     "lane-b",
		Identity: "reviewer-b",
	})
	manifest.Integration = []Lane{"lane-a", "lane-b"}
	addWriterPair(&manifest, "lane-a", CommitID(strings.Repeat("d", 40)), Digest(strings.Repeat("1", 64)))
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("writing with one atomic pair: %v", err)
	}
	manifest.Phase = PhaseReview
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("review began before every writer supplied an initial pair")
	}
	manifest.Phase = PhaseWriting

	manifest.Snapshots = append(
		manifest.Snapshots,
		testSnapshot(manifest, "lane-b", CommitID(strings.Repeat("e", 40))),
	)
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("writing accepted a snapshot without its writer report")
	}
	manifest.Snapshots = manifest.Snapshots[:1]
	addWriterPair(&manifest, "lane-b", CommitID(strings.Repeat("e", 40)), Digest(strings.Repeat("2", 64)))
	manifest.Phase = PhaseReview
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("review entry with zero reviewer reports: %v", err)
	}

	reviewDigest := Digest(strings.Repeat("3", 64))
	manifest.ReviewReports = []ArtifactReport{{
		Wave:     manifest.Wave,
		Lane:     "lane-a",
		Attempt:  manifest.Snapshots[0].Attempt,
		Identity: manifest.Reviewers[0].Identity,
		Base:     manifest.Base.Commit,
		Snapshot: manifest.Snapshots[0].Commit,
		Path:     reviewReportPath(manifest.Wave, "lane-a", manifest.Snapshots[0].Attempt),
		Digest:   reviewDigest,
	}}
	manifest.Reviews = []ReviewReceipt{{
		Lane:         "lane-a",
		Attempt:      manifest.Snapshots[0].Attempt,
		Reviewer:     manifest.Reviewers[0].Identity,
		Snapshot:     manifest.Snapshots[0].Commit,
		Approved:     true,
		Report:       reviewReportPath(manifest.Wave, "lane-a", manifest.Snapshots[0].Attempt),
		ReportDigest: reviewDigest,
	}}
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("review with one accumulated approval: %v", err)
	}
	manifest.AuditReports = []AuditReport{{}}
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("audit evidence was accepted before every paired approval")
	}
}

// Rationale: audit evidence must bind the assigned actor and the exact complete immutable attempt set.
func TestAuditRejectsIdentityAndSnapshotSetMismatch(t *testing.T) {
	manifest := validWritingManifest()
	addWriterPair(&manifest, "lane-a", CommitID(strings.Repeat("d", 40)), Digest(strings.Repeat("1", 64)))
	manifest.Phase = PhaseReview
	reviewDigest := Digest(strings.Repeat("2", 64))
	manifest.ReviewReports = []ArtifactReport{{
		Wave:     manifest.Wave,
		Lane:     "lane-a",
		Attempt:  manifest.Snapshots[0].Attempt,
		Identity: manifest.Reviewers[0].Identity,
		Base:     manifest.Base.Commit,
		Snapshot: manifest.Snapshots[0].Commit,
		Path:     reviewReportPath(manifest.Wave, "lane-a", manifest.Snapshots[0].Attempt),
		Digest:   reviewDigest,
	}}
	manifest.Reviews = []ReviewReceipt{{
		Lane:         "lane-a",
		Attempt:      manifest.Snapshots[0].Attempt,
		Reviewer:     manifest.Reviewers[0].Identity,
		Snapshot:     manifest.Snapshots[0].Commit,
		Approved:     true,
		Report:       reviewReportPath(manifest.Wave, "lane-a", manifest.Snapshots[0].Attempt),
		ReportDigest: reviewDigest,
	}}
	reportDigest := Digest(strings.Repeat("3", 64))
	snapshots := []CommitID{manifest.Snapshots[0].Commit}
	auditPath := auditReportPath(manifest.Wave, AuditProduct, auditSnapshotDigest(snapshots))
	manifest.AuditReports = []AuditReport{{
		Wave:           manifest.Wave,
		Identity:       "audit-b",
		Role:           AuditProduct,
		Base:           manifest.Base.Commit,
		Snapshots:      snapshots,
		SnapshotDigest: auditSnapshotDigest(snapshots),
		ReportDigest:   reportDigest,
		Path:           auditPath,
	}}
	manifest.Audits = []AuditReceipt{{
		Wave:           manifest.Wave,
		Identity:       "audit-b",
		Role:           AuditProduct,
		Base:           manifest.Base.Commit,
		Snapshots:      snapshots,
		SnapshotDigest: auditSnapshotDigest(snapshots),
		Report:         auditPath,
		ReportDigest:   reportDigest,
		Approved:       true,
	}}
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("audit report accepted the wrong assigned identity")
	}

	manifest.AuditReports[0].Identity = manifest.CrossAudits[0].Identity
	manifest.Audits[0].Identity = manifest.CrossAudits[0].Identity
	wrongSnapshots := []CommitID{CommitID(strings.Repeat("f", 40))}
	manifest.AuditReports[0].Snapshots = wrongSnapshots
	manifest.AuditReports[0].SnapshotDigest = auditSnapshotDigest(wrongSnapshots)
	manifest.Audits[0].Snapshots = wrongSnapshots
	manifest.Audits[0].SnapshotDigest = auditSnapshotDigest(wrongSnapshots)
	if err := ValidateManifest(manifest); err == nil {
		t.Fatal("audit report accepted a substituted snapshot set")
	}
}

// Rationale: replacement attempts must preserve history, prove authorization, and select one chain tail.
func TestRemediationRequiresRejectedPredecessorAndSelectsOneActiveTail(t *testing.T) {
	manifest := validWritingManifest()
	manifest.Reserves = []RemediationReserve{{
		Name:     "reserve-a",
		Identity: "remediator-a",
		Worktree: ReserveWorktreePath(manifest.Wave, "reserve-a"),
	}}
	initialCommit := CommitID(strings.Repeat("d", 40))
	addWriterPair(&manifest, "lane-a", initialCommit, Digest(strings.Repeat("1", 64)))
	manifest.Phase = PhaseReview
	addReview(&manifest, manifest.Snapshots[0], false, Digest(strings.Repeat("2", 64)))

	replacement := Snapshot{
		Lane:              "lane-a",
		Attempt:           "attempt-2",
		Author:            manifest.Reserves[0].Identity,
		Ref:               SnapshotRef(manifest.Wave, "lane-a", "attempt-2"),
		Commit:            CommitID(strings.Repeat("e", 40)),
		Parent:            initialCommit,
		Replaces:          initialCommit,
		Tree:              TreeID(strings.Repeat("8", 40)),
		Signature:         "G",
		SignerFingerprint: manifest.PrimarySigner,
		RemediationLease:  "internal/feature/a/remediation",
	}
	manifest.Snapshots = append(manifest.Snapshots, replacement)
	manifest.WriterReports = append(manifest.WriterReports, ArtifactReport{
		Wave:     manifest.Wave,
		Lane:     replacement.Lane,
		Attempt:  replacement.Attempt,
		Identity: replacement.Author,
		Base:     manifest.Base.Commit,
		Snapshot: replacement.Commit,
		Path:     writerReportPath(manifest.Wave, replacement.Lane, replacement.Attempt),
		Digest:   Digest(strings.Repeat("3", 64)),
	})
	manifest.Active[0] = ActiveSnapshot{
		Lane: replacement.Lane, Attempt: replacement.Attempt, Snapshot: replacement.Commit,
	}
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("valid remediation chain: %v", err)
	}

	outOfScope := manifest
	outOfScope.Snapshots = append([]Snapshot(nil), manifest.Snapshots...)
	outOfScope.Snapshots[1].RemediationLease = "internal/unassigned/repair"
	if err := ValidateManifest(outOfScope); err == nil {
		t.Fatal("reserve remediation lease escaped the original lane lease union")
	}

	sharedCollision := manifest
	sharedCollision.Writers = append([]Writer(nil), manifest.Writers...)
	sharedCollision.Writers[0].Leases = []RepoPath{"internal/controller"}
	sharedCollision.SharedAreas = []SharedArea{{
		Name: "controller-server", Paths: []RepoPath{"internal/controller/server.go"},
		OwnerLane: "lane-a",
	}}
	sharedCollision.Snapshots = append([]Snapshot(nil), manifest.Snapshots...)
	sharedCollision.Snapshots[1].RemediationLease = "internal/controller/server.go"
	if err := ValidateManifest(sharedCollision); err == nil {
		t.Fatal("reserve remediation lease overlapped a shared area")
	}

	withoutRejection := manifest
	withoutRejection.ReviewReports = nil
	withoutRejection.Reviews = nil
	if err := ValidateManifest(withoutRejection); err == nil {
		t.Fatal("replacement accepted without a rejecting paired review")
	}

	wrongTail := manifest
	wrongTail.Active = []ActiveSnapshot{{
		Lane: "lane-a", Attempt: "attempt-1", Snapshot: initialCommit,
	}}
	if err := ValidateManifest(wrongTail); err == nil {
		t.Fatal("rejected predecessor remained selectable as the active tail")
	}

	wrongAuthor := manifest
	wrongAuthor.Snapshots = append([]Snapshot(nil), manifest.Snapshots...)
	wrongAuthor.Snapshots[1].Author = "undeclared-remediator"
	if err := ValidateManifest(wrongAuthor); err == nil {
		t.Fatal("replacement accepted an undeclared remediation author")
	}
}

// Rationale: rejected cross-audits must authorize remediation without overwriting historical evidence.
func TestRejectedAuditAuthorizesAppendOnlyReplacementHistory(t *testing.T) {
	manifest := validWritingManifest()
	manifest.Reserves = []RemediationReserve{{
		Name: "reserve-a", Identity: "remediator-a",
		Worktree: ReserveWorktreePath(manifest.Wave, "reserve-a"),
	}}
	initial := CommitID(strings.Repeat("d", 40))
	addWriterPair(&manifest, "lane-a", initial, Digest(strings.Repeat("1", 64)))
	manifest.Phase = PhaseReview
	addReview(&manifest, manifest.Snapshots[0], true, Digest(strings.Repeat("2", 64)))
	addAudit(
		&manifest,
		manifest.CrossAudits[0],
		[]CommitID{initial},
		false,
		Digest(strings.Repeat("3", 64)),
	)
	replacement := Snapshot{
		Lane: "lane-a", Attempt: "attempt-2", Author: manifest.Reserves[0].Identity,
		Ref:    SnapshotRef(manifest.Wave, "lane-a", "attempt-2"),
		Commit: CommitID(strings.Repeat("e", 40)), Parent: initial, Replaces: initial,
		Tree: TreeID(strings.Repeat("8", 40)), Signature: "G",
		SignerFingerprint: manifest.PrimarySigner, RemediationLease: "internal/feature/a/remediation",
	}
	manifest.Snapshots = append(manifest.Snapshots, replacement)
	manifest.Active[0] = ActiveSnapshot{
		Lane: replacement.Lane, Attempt: replacement.Attempt, Snapshot: replacement.Commit,
	}
	manifest.WriterReports = append(manifest.WriterReports, ArtifactReport{
		Wave: manifest.Wave, Lane: replacement.Lane, Attempt: replacement.Attempt,
		Identity: replacement.Author, Base: manifest.Base.Commit, Snapshot: replacement.Commit,
		Path:   writerReportPath(manifest.Wave, replacement.Lane, replacement.Attempt),
		Digest: Digest(strings.Repeat("4", 64)),
	})
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("audit-authorized append-only replacement: %v", err)
	}
	if len(manifest.Audits) != 1 || manifest.Audits[0].Snapshots[0] != initial {
		t.Fatal("historical rejected audit was overwritten")
	}
}

// Rationale: a typed exact-prefix integration conflict is an append-only replacement authorization.
func TestRecomputableIntegrationConflictAuthorizesReplacement(t *testing.T) {
	manifest := validWritingManifest()
	manifest.Phase = PhaseIntegrate
	manifest.Reserves = []RemediationReserve{{
		Name: "reserve-a", Identity: "remediator-a",
		Worktree: ReserveWorktreePath(manifest.Wave, "reserve-a"),
	}}
	initial := CommitID(strings.Repeat("d", 40))
	addWriterPair(&manifest, "lane-a", initial, Digest(strings.Repeat("1", 64)))
	manifest.Conflicts = []IntegrationConflict{{
		Lane: "lane-a", Attempt: "attempt-1", Snapshot: initial,
		PrefixCommit: manifest.Base.Commit,
	}}
	replacement := Snapshot{
		Lane: "lane-a", Attempt: "attempt-2", Author: manifest.Reserves[0].Identity,
		Ref:    SnapshotRef(manifest.Wave, "lane-a", "attempt-2"),
		Commit: CommitID(strings.Repeat("e", 40)), Parent: initial, Replaces: initial,
		Tree: TreeID(strings.Repeat("8", 40)), Signature: "G",
		SignerFingerprint: manifest.PrimarySigner, RemediationLease: "internal/feature/a/remediation",
	}
	manifest.Snapshots = append(manifest.Snapshots, replacement)
	manifest.Active[0] = ActiveSnapshot{
		Lane: replacement.Lane, Attempt: replacement.Attempt, Snapshot: replacement.Commit,
	}
	manifest.WriterReports = append(manifest.WriterReports, ArtifactReport{
		Wave: manifest.Wave, Lane: replacement.Lane, Attempt: replacement.Attempt,
		Identity: replacement.Author, Base: manifest.Base.Commit, Snapshot: replacement.Commit,
		Path:   writerReportPath(manifest.Wave, replacement.Lane, replacement.Attempt),
		Digest: Digest(strings.Repeat("2", 64)),
	})
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("integration-conflict-authorized replacement: %v", err)
	}
}

// Rationale: conflict remediation must retain a nonzero applied prefix and resume without rewriting history.
func TestMidIntegrationConflictRemediationRetainsAndResumesAppliedPrefix(t *testing.T) {
	manifest := validWritingManifest()
	manifest.Writers = append(manifest.Writers, Writer{
		Lane: "lane-b", Identity: "writer-b", Worktree: WorktreePath(manifest.Wave, "lane-b"),
		Base: manifest.Base.Commit, Leases: []RepoPath{"internal/feature/b"},
	})
	manifest.Reviewers = append(manifest.Reviewers, Reviewer{
		Lane: "lane-b", Identity: "reviewer-b",
	})
	manifest.Integration = []Lane{"lane-a", "lane-b"}
	manifest.Reserves = []RemediationReserve{{
		Name: "reserve-b", Identity: "remediator-b",
		Worktree: ReserveWorktreePath(manifest.Wave, "reserve-b"),
	}}
	aInitial := CommitID(strings.Repeat("d", 40))
	bInitial := CommitID(strings.Repeat("e", 40))
	addWriterPair(&manifest, "lane-a", aInitial, Digest(strings.Repeat("1", 64)))
	addWriterPair(&manifest, "lane-b", bInitial, Digest(strings.Repeat("2", 64)))
	manifest.Phase = PhaseReview
	addReview(&manifest, manifest.Snapshots[0], false, Digest(strings.Repeat("3", 64)))
	addReview(&manifest, manifest.Snapshots[1], true, Digest(strings.Repeat("4", 64)))
	aReplacement := Snapshot{
		Lane: "lane-a", Attempt: "attempt-2", Author: "writer-a",
		Ref:    SnapshotRef(manifest.Wave, "lane-a", "attempt-2"),
		Commit: CommitID(strings.Repeat("f", 40)), Parent: aInitial, Replaces: aInitial,
		Tree: TreeID(strings.Repeat("7", 40)), Signature: "G",
		SignerFingerprint: manifest.PrimarySigner,
	}
	manifest.Snapshots = append(manifest.Snapshots, aReplacement)
	manifest.Active[0] = ActiveSnapshot{
		Lane: aReplacement.Lane, Attempt: aReplacement.Attempt, Snapshot: aReplacement.Commit,
	}
	manifest.WriterReports = append(manifest.WriterReports, ArtifactReport{
		Wave: manifest.Wave, Lane: aReplacement.Lane, Attempt: aReplacement.Attempt,
		Identity: aReplacement.Author, Base: manifest.Base.Commit, Snapshot: aReplacement.Commit,
		Path:   writerReportPath(manifest.Wave, aReplacement.Lane, aReplacement.Attempt),
		Digest: Digest(strings.Repeat("5", 64)),
	})
	addReview(&manifest, aReplacement, true, Digest(strings.Repeat("6", 64)))
	oldActiveSet := []CommitID{bInitial, aReplacement.Commit}
	for index, auditor := range manifest.CrossAudits {
		addAudit(
			&manifest,
			auditor,
			oldActiveSet,
			true,
			Digest(strings.Repeat(string(rune('7'+index)), 64)),
		)
	}
	manifest.Phase = PhaseIntegrate
	manifest.Applied = []AppliedReceipt{{
		Lane: "lane-a", Snapshot: aReplacement.Commit,
		PrimaryIndexTree: TreeID(strings.Repeat("a", 40)),
	}}
	manifest.Conflicts = []IntegrationConflict{{
		Lane: "lane-b", Attempt: "attempt-1", Snapshot: bInitial,
		PrefixSnapshots: []CommitID{aReplacement.Commit},
		PrefixCommit:    CommitID(strings.Repeat("6", 40)),
	}}
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("conflict-only integrate state: %v", err)
	}
	if !HasPendingConflictRemediation(manifest) {
		t.Fatal("conflict-only state did not retain prefix-only validation")
	}
	bReplacement := Snapshot{
		Lane: "lane-b", Attempt: "attempt-2", Author: "remediator-b",
		Ref:    SnapshotRef(manifest.Wave, "lane-b", "attempt-2"),
		Commit: CommitID(strings.Repeat("7", 40)), Parent: bInitial, Replaces: bInitial,
		Tree: TreeID(strings.Repeat("8", 40)), Signature: "G",
		SignerFingerprint: manifest.PrimarySigner,
		RemediationLease:  "internal/feature/b/remediation",
	}
	manifest.Snapshots = append(manifest.Snapshots, bReplacement)
	manifest.Active[1] = ActiveSnapshot{
		Lane: bReplacement.Lane, Attempt: bReplacement.Attempt, Snapshot: bReplacement.Commit,
	}
	manifest.WriterReports = append(manifest.WriterReports, ArtifactReport{
		Wave: manifest.Wave, Lane: bReplacement.Lane, Attempt: bReplacement.Attempt,
		Identity: bReplacement.Author, Base: manifest.Base.Commit, Snapshot: bReplacement.Commit,
		Path:   writerReportPath(manifest.Wave, bReplacement.Lane, bReplacement.Attempt),
		Digest: Digest(strings.Repeat("b", 64)),
	})
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("pending mid-integration remediation: %v", err)
	}
	if !HasPendingConflictRemediation(manifest) {
		t.Fatal("unapproved first replacement did not retain prefix-only validation")
	}
	addReview(&manifest, bReplacement, false, Digest(strings.Repeat("c", 64)))
	bSecondReplacement := Snapshot{
		Lane: "lane-b", Attempt: "attempt-3", Author: "remediator-b",
		Ref:    SnapshotRef(manifest.Wave, "lane-b", "attempt-3"),
		Commit: CommitID(strings.Repeat("5", 40)),
		Parent: bReplacement.Commit, Replaces: bReplacement.Commit,
		Tree: TreeID(strings.Repeat("9", 40)), Signature: "G",
		SignerFingerprint: manifest.PrimarySigner,
		RemediationLease:  "internal/feature/b/remediation/second",
	}
	manifest.Snapshots = append(manifest.Snapshots, bSecondReplacement)
	manifest.Active[1] = ActiveSnapshot{
		Lane:     bSecondReplacement.Lane,
		Attempt:  bSecondReplacement.Attempt,
		Snapshot: bSecondReplacement.Commit,
	}
	manifest.WriterReports = append(manifest.WriterReports, ArtifactReport{
		Wave: manifest.Wave, Lane: bSecondReplacement.Lane,
		Attempt: bSecondReplacement.Attempt, Identity: bSecondReplacement.Author,
		Base: manifest.Base.Commit, Snapshot: bSecondReplacement.Commit,
		Path: writerReportPath(
			manifest.Wave,
			bSecondReplacement.Lane,
			bSecondReplacement.Attempt,
		),
		Digest: Digest(strings.Repeat("a", 64)),
	})
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("second pending replacement descended from conflict: %v", err)
	}
	if !HasPendingConflictRemediation(manifest) {
		t.Fatal("second pending replacement did not retain prefix-only validation")
	}
	mismatched := manifest
	mismatched.Conflicts = append([]IntegrationConflict(nil), manifest.Conflicts...)
	mismatched.Conflicts[0].PrefixSnapshots = []CommitID{aInitial}
	if err := ValidateManifest(mismatched); err == nil {
		t.Fatal("conflict accepted a historical attempt instead of the applied snapshot")
	}
	addReview(&manifest, bSecondReplacement, true, Digest(strings.Repeat("0", 64)))
	currentSet := []CommitID{bSecondReplacement.Commit, aReplacement.Commit}
	for index, auditor := range manifest.CrossAudits {
		addAudit(
			&manifest,
			auditor,
			currentSet,
			true,
			Digest(strings.Repeat(string(rune('d'+index)), 64)),
		)
	}
	if HasPendingConflictRemediation(manifest) {
		t.Fatal("fully approved replacement did not resume complete prefix validation")
	}
	manifest.Applied = append(manifest.Applied, AppliedReceipt{
		Lane: "lane-b", Snapshot: bSecondReplacement.Commit,
		PrimaryIndexTree: TreeID(strings.Repeat("b", 40)),
	})
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("resumed application after conflict remediation: %v", err)
	}
}

// Rationale: artifact decoding must accept the protocol output bound after JSON base64 expansion.
func TestGateArtifactAcceptsMaximumDeclaredOutput(t *testing.T) {
	commit := CommitID(strings.Repeat("a", 40))
	tree := TreeID(strings.Repeat("b", 40))
	state := GateState{Head: commit, Tree: tree, Clean: true}
	artifact := NewGateArtifact(
		"wave-1",
		GateRepositoryCI,
		"/usr/bin/make",
		[]string{"ci"},
		[]string{"PATH=/usr/bin"},
		commit,
		tree,
		state,
		state,
		0,
		bytes.Repeat([]byte{'x'}, maxGateOutputBytes),
		nil,
	)
	data, _, err := EncodeGateArtifact(context.Background(), artifact)
	if err != nil {
		t.Fatalf("encode maximum gate output: %v", err)
	}
	if _, err := ParseGateArtifact(context.Background(), data); err != nil {
		t.Fatalf("parse maximum gate output: %v", err)
	}
}

func addWriterPair(manifest *Manifest, lane Lane, commit CommitID, reportDigest Digest) {
	snapshot := testSnapshot(*manifest, lane, commit)
	manifest.Snapshots = append(manifest.Snapshots, snapshot)
	manifest.Active = append(manifest.Active, ActiveSnapshot{
		Lane:     lane,
		Attempt:  snapshot.Attempt,
		Snapshot: commit,
	})
	manifest.WriterReports = append(manifest.WriterReports, ArtifactReport{
		Wave:     manifest.Wave,
		Lane:     lane,
		Attempt:  snapshot.Attempt,
		Identity: snapshot.Author,
		Base:     manifest.Base.Commit,
		Snapshot: commit,
		Path:     writerReportPath(manifest.Wave, lane, snapshot.Attempt),
		Digest:   reportDigest,
	})
}

func testSnapshot(manifest Manifest, lane Lane, commit CommitID) Snapshot {
	var author Identity
	for _, writer := range manifest.Writers {
		if writer.Lane == lane {
			author = writer.Identity
			break
		}
	}
	attempt := AttemptID("attempt-1")
	return Snapshot{
		Lane:              lane,
		Attempt:           attempt,
		Author:            author,
		Ref:               SnapshotRef(manifest.Wave, lane, attempt),
		Commit:            commit,
		Parent:            manifest.Base.Commit,
		Tree:              TreeID(strings.Repeat("9", 40)),
		Signature:         "G",
		SignerFingerprint: manifest.PrimarySigner,
	}
}

func writerReportPath(wave WaveID, lane Lane, attempt AttemptID) RepoPath {
	return RepoPath(
		string(ArtifactRoot(wave)) + "/" + string(lane) + "-" + string(attempt) + "-writer.json",
	)
}

func reviewReportPath(wave WaveID, lane Lane, attempt AttemptID) RepoPath {
	return RepoPath(
		string(ArtifactRoot(wave)) + "/" + string(lane) + "-" + string(attempt) + "-review.json",
	)
}

func auditReportPath(wave WaveID, role AuditRole, digest Digest) RepoPath {
	return RepoPath(string(ArtifactRoot(wave)) + "/audit-" + string(role) + "-" + string(digest) + ".json")
}

func addReview(manifest *Manifest, snapshot Snapshot, approved bool, digest Digest) {
	var reviewer Identity
	for _, assignment := range manifest.Reviewers {
		if assignment.Lane == snapshot.Lane {
			reviewer = assignment.Identity
			break
		}
	}
	path := reviewReportPath(manifest.Wave, snapshot.Lane, snapshot.Attempt)
	manifest.ReviewReports = append(manifest.ReviewReports, ArtifactReport{
		Wave:     manifest.Wave,
		Lane:     snapshot.Lane,
		Attempt:  snapshot.Attempt,
		Identity: reviewer,
		Base:     manifest.Base.Commit,
		Snapshot: snapshot.Commit,
		Path:     path,
		Digest:   digest,
	})
	manifest.Reviews = append(manifest.Reviews, ReviewReceipt{
		Lane:         snapshot.Lane,
		Attempt:      snapshot.Attempt,
		Reviewer:     reviewer,
		Snapshot:     snapshot.Commit,
		Approved:     approved,
		Report:       path,
		ReportDigest: digest,
	})
}

func addAudit(
	manifest *Manifest,
	assignment CrossAudit,
	snapshots []CommitID,
	approved bool,
	reportDigest Digest,
) {
	snapshotDigest := auditSnapshotDigest(snapshots)
	path := auditReportPath(manifest.Wave, assignment.Role, snapshotDigest)
	manifest.AuditReports = append(manifest.AuditReports, AuditReport{
		Wave: manifest.Wave, Identity: assignment.Identity, Role: assignment.Role,
		Base: manifest.Base.Commit, Snapshots: append([]CommitID(nil), snapshots...),
		SnapshotDigest: snapshotDigest, ReportDigest: reportDigest, Path: path,
	})
	manifest.Audits = append(manifest.Audits, AuditReceipt{
		Wave: manifest.Wave, Identity: assignment.Identity, Role: assignment.Role,
		Base: manifest.Base.Commit, Snapshots: append([]CommitID(nil), snapshots...),
		SnapshotDigest: snapshotDigest, Report: path, ReportDigest: reportDigest,
		Approved: approved,
	})
}
