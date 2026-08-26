// Package swarmgit contains the concrete Git and filesystem evidence reader
// used by the swarm coordinator. It deliberately has no interface of its
// own: the existing runner.Runner is the only process seam.
package swarmgit

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/swarmcheck"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Inspector is the primary's concrete view of one repository and its
// detached writer worktrees.
type Inspector struct {
	Root   string
	Runner runner.Runner
}

// NewInspector constructs a Git inspector. A nil runner selects the real
// repository Runner; tests can provide runner.FakeRunner.
func NewInspector(root string, commandRunner runner.Runner) *Inspector {
	if absolute, err := filepath.Abs(root); err == nil {
		root = absolute
	}
	if commandRunner == nil {
		commandRunner = runner.New(nil)
	}
	return &Inspector{Root: root, Runner: commandRunner}
}

// Inspection is the live primary state used to compare a manifest phase with
// the repository's current HEAD and index tree.
type Inspection struct {
	Head      swarmcheck.CommitID
	IndexTree swarmcheck.TreeID
	Clean     bool
}

// CommitEvidence is the bounded metadata needed to prove signature and
// parent lineage without trusting a report supplied by an agent.
type CommitEvidence struct {
	Commit            swarmcheck.CommitID
	Parents           []swarmcheck.CommitID
	Tree              swarmcheck.TreeID
	Signature         string
	SignerFingerprint swarmcheck.Fingerprint
}

// Inspect compares the live primary repository to the manifest's current
// phase contract. It never treats a pstack report as repository evidence.
func (inspector *Inspector) Inspect(
	ctx context.Context,
	manifest swarmcheck.Manifest,
) (Inspection, error) {
	if err := manifest.Validate(ctx); err != nil {
		return Inspection{}, err
	}
	if err := inspector.validateSignedBase(ctx, manifest); err != nil {
		return Inspection{}, err
	}
	head, err := inspector.resolve(ctx, "HEAD")
	if err != nil {
		return Inspection{}, err
	}
	if err := inspector.requireTargetRef(ctx, manifest.TargetRef); err != nil {
		return Inspection{}, err
	}
	indexTree, err := inspector.writeTree(ctx)
	if err != nil {
		return Inspection{}, err
	}
	clean, err := inspector.clean(ctx, false)
	if err != nil {
		return Inspection{}, err
	}
	inspection := Inspection{Head: head, IndexTree: indexTree, Clean: clean}
	if err := inspector.validatePhaseState(ctx, manifest, inspection); err != nil {
		return inspection, err
	}
	return inspection, nil
}

// ValidateDispatch proves that the primary and all registered detached
// writer worktrees are clean and at the exact signed base.
func (inspector *Inspector) ValidateDispatch(
	ctx context.Context,
	manifest swarmcheck.Manifest,
) error {
	if err := manifest.Validate(ctx); err != nil {
		return err
	}
	if err := inspector.validateSignedBase(ctx, manifest); err != nil {
		return err
	}
	if err := inspector.requireTargetRef(ctx, manifest.TargetRef); err != nil {
		return err
	}
	head, err := inspector.resolve(ctx, "HEAD")
	if err != nil {
		return err
	}
	if head != manifest.Base.Commit {
		return invalid("primary HEAD is not the signed swarm base")
	}
	if err := inspector.requireClean(ctx, false); err != nil {
		return err
	}
	worktrees, err := inspector.worktrees(ctx)
	if err != nil {
		return err
	}
	for _, writer := range manifest.Writers {
		worktree, ok := worktrees[writer.Worktree]
		if !ok || !worktree.Detached || worktree.Head != manifest.Base.Commit {
			return invalid(
				fmt.Sprintf("writer %s is not a detached worktree at the signed base", writer.Lane),
			)
		}
		writerInspector := NewInspector(
			filepath.Join(inspector.Root, filepath.FromSlash(string(writer.Worktree))),
			inspector.Runner,
		)
		if err := writerInspector.requireClean(ctx, true); err != nil {
			return invalid(fmt.Sprintf("writer %s worktree is not clean: %v", writer.Lane, err))
		}
	}
	for _, reserve := range manifest.Reserves {
		worktree, ok := worktrees[reserve.Worktree]
		if !ok || !worktree.Detached || worktree.Head != manifest.Base.Commit {
			return invalid(
				fmt.Sprintf(
					"remediation reserve %s is not detached at the signed base",
					reserve.Name,
				),
			)
		}
		reserveInspector := NewInspector(
			filepath.Join(inspector.Root, filepath.FromSlash(string(reserve.Worktree))),
			inspector.Runner,
		)
		if err := reserveInspector.requireClean(ctx, true); err != nil {
			return invalid(
				fmt.Sprintf("remediation reserve %s worktree is not clean: %v", reserve.Name, err),
			)
		}
	}
	return nil
}

// ValidateWriting proves a stopped writer did not stage, merge, or mutate
// paths outside its leases. Shared paths are allowed only to their selected
// closed-area owner.
func (inspector *Inspector) ValidateWriting(
	ctx context.Context,
	manifest swarmcheck.Manifest,
	lane swarmcheck.Lane,
) error {
	if err := manifest.Validate(ctx); err != nil {
		return err
	}
	if err := inspector.validateSignedBase(ctx, manifest); err != nil {
		return err
	}
	assignment, ok := swarmcheck.ResolveWritingAssignment(manifest, lane)
	if !ok {
		return invalid(fmt.Sprintf("unknown writer lane %s", lane))
	}
	cleanRoot := filepath.Clean(inspector.Root)
	expectedSuffix := filepath.Clean(filepath.FromSlash(string(assignment.Worktree)))
	if cleanRoot != expectedSuffix &&
		!strings.HasSuffix(cleanRoot, string(filepath.Separator)+expectedSuffix) {
		return invalid(
			fmt.Sprintf("writer %s inspection must use its derived detached worktree", lane),
		)
	}
	status, err := inspector.status(ctx, true)
	if err != nil {
		return err
	}
	if status.Staged || status.Unmerged || status.Sparse || status.AssumeUnchanged {
		return invalid(
			fmt.Sprintf("writer %s has staged, unmerged, sparse, or assume-unchanged state", lane),
		)
	}
	head, err := inspector.resolve(ctx, "HEAD")
	if err != nil || head != assignment.Head {
		return invalid(fmt.Sprintf("writer %s is not at the active attempt base", lane))
	}
	attachment, err := inspector.headAttachment(ctx)
	if err != nil || !attachment.Detached {
		return invalid(fmt.Sprintf("writer %s worktree is not detached", lane))
	}
	for _, changed := range status.Paths {
		allowed := swarmcheck.ChangeAllowed(manifest, lane, changed.Path)
		if assignment.Attempt.Commit != "" {
			allowed = swarmcheck.ChangeAllowedForSnapshot(
				manifest,
				assignment.Attempt,
				changed.Path,
			)
		}
		if !allowed {
			return invalid(
				fmt.Sprintf("writer %s changed path outside its lease: %s", lane, changed.Path),
			)
		}
	}
	return nil
}

func (inspector *Inspector) validateActiveWorktrees(
	ctx context.Context,
	manifest swarmcheck.Manifest,
) error {
	for _, active := range manifest.Active {
		assignment, exists := swarmcheck.ResolveWritingAssignment(manifest, active.Lane)
		if !exists || assignment.Attempt.Commit != active.Snapshot {
			return invalid(fmt.Sprintf("active lane %s lacks its writing assignment", active.Lane))
		}
		worktreeInspector := NewInspector(
			filepath.Join(inspector.Root, filepath.FromSlash(string(assignment.Worktree))),
			inspector.Runner,
		)
		if err := worktreeInspector.ValidateWriting(ctx, manifest, active.Lane); err != nil {
			return invalid(
				fmt.Sprintf("active lane %s stopped worktree is invalid: %v", active.Lane, err),
			)
		}
	}
	return nil
}

// ValidateSnapshot proves the primary-created snapshot is signed, immutable,
// has its exact declared attempt-chain parent, and changes only the lane's
// allowed paths.
func (inspector *Inspector) ValidateSnapshot(
	ctx context.Context,
	manifest swarmcheck.Manifest,
	snapshot swarmcheck.Snapshot,
) error {
	if err := manifest.Validate(ctx); err != nil {
		return err
	}
	if err := validateManifestSnapshot(manifest, snapshot); err != nil {
		return err
	}
	if err := inspector.validateSignedBase(ctx, manifest); err != nil {
		return err
	}
	if err := inspector.validateSnapshotEvidence(ctx, manifest, snapshot); err != nil {
		return err
	}
	return inspector.requireSnapshotRef(ctx, snapshot, true)
}

// ValidateWritingEvidence proves every recorded partial attempt/report pair
// from Git objects, temporary refs, lease-bounded diffs, and artifact bytes.
func (inspector *Inspector) ValidateWritingEvidence(
	ctx context.Context,
	manifest swarmcheck.Manifest,
) error {
	if manifest.Phase != swarmcheck.PhaseWriting {
		return invalid("writing evidence validation requires the writing phase")
	}
	if err := manifest.Validate(ctx); err != nil {
		return err
	}
	if err := inspector.validateSignedBase(ctx, manifest); err != nil {
		return err
	}
	for _, snapshot := range manifest.Snapshots {
		if err := validateManifestSnapshot(manifest, snapshot); err != nil {
			return err
		}
		if err := inspector.validateSnapshotEvidence(ctx, manifest, snapshot); err != nil {
			return err
		}
		if err := inspector.requireSnapshotRef(ctx, snapshot, true); err != nil {
			return err
		}
	}
	return inspector.ValidateReports(ctx, manifest)
}

func (inspector *Inspector) validateSnapshotEvidence(
	ctx context.Context,
	manifest swarmcheck.Manifest,
	snapshot swarmcheck.Snapshot,
) error {
	evidence, err := inspector.commit(ctx, string(snapshot.Commit))
	if err != nil {
		return err
	}
	if evidence.Commit != snapshot.Commit ||
		!sameValues(evidence.Parents, []swarmcheck.CommitID{snapshot.Parent}) ||
		evidence.Tree != snapshot.Tree ||
		evidence.Signature != snapshot.Signature ||
		evidence.SignerFingerprint != snapshot.SignerFingerprint ||
		evidence.SignerFingerprint != manifest.PrimarySigner ||
		!swarmcheckSignatureOK(evidence.Signature) {
		return invalid(
			fmt.Sprintf("snapshot %s does not prove signed immutable base lineage", snapshot.Lane),
		)
	}
	changed, err := inspector.diff(ctx, snapshot.Parent, snapshot.Commit)
	if err != nil {
		return err
	}
	if err := inspector.validateChangedModes(
		ctx,
		snapshot.Parent,
		snapshot.Commit,
		changed,
	); err != nil {
		return err
	}
	for _, path := range changed {
		if !swarmcheck.ChangeAllowedForSnapshot(manifest, snapshot, path.Path) {
			return invalid(
				fmt.Sprintf(
					"snapshot %s changed path outside its lease: %s",
					snapshot.Lane,
					path.Path,
				),
			)
		}
	}
	return nil
}

func (inspector *Inspector) requireSnapshotRef(
	ctx context.Context,
	snapshot swarmcheck.Snapshot,
	present bool,
) error {
	result, err := inspector.git(
		ctx,
		"for-each-ref",
		"--format=%(objectname)",
		"--",
		string(snapshot.Ref),
	)
	if err != nil {
		return err
	}
	rawValue := strings.TrimSpace(string(result.Stdout))
	var value swarmcheck.CommitID
	if rawValue != "" {
		value, err = swarmcheck.ParseCommitID(rawValue)
		if err != nil {
			return err
		}
	}
	if present && value != snapshot.Commit {
		return invalid(
			fmt.Sprintf("snapshot %s ref does not resolve to its immutable commit", snapshot.Lane),
		)
	}
	if !present && value != "" {
		return invalid(
			fmt.Sprintf("complete phase retains temporary snapshot ref %s", snapshot.Ref),
		)
	}
	return nil
}

func validateManifestSnapshot(manifest swarmcheck.Manifest, snapshot swarmcheck.Snapshot) error {
	declared, ok := snapshotByCommit(manifest, snapshot.Commit)
	if !ok || declared != snapshot {
		return invalid(
			fmt.Sprintf("snapshot %s is not the exact manifest lane snapshot", snapshot.Lane),
		)
	}
	if snapshot.Ref != swarmcheck.SnapshotRef(manifest.Wave, snapshot.Lane, snapshot.Attempt) ||
		!swarmcheckSignatureOK(snapshot.Signature) ||
		snapshot.SignerFingerprint != manifest.PrimarySigner {
		return invalid(fmt.Sprintf("snapshot %s manifest evidence is invalid", snapshot.Lane))
	}
	return nil
}

func snapshotByCommit(
	manifest swarmcheck.Manifest,
	commit swarmcheck.CommitID,
) (swarmcheck.Snapshot, bool) {
	for _, snapshot := range manifest.Snapshots {
		if snapshot.Commit == commit {
			return snapshot, true
		}
	}
	return swarmcheck.Snapshot{}, false
}

// ValidateReports checks every manifest-declared report as a bounded regular
// non-symlink file beneath the derived wave artifact root.
func (inspector *Inspector) ValidateReports(
	ctx context.Context,
	manifest swarmcheck.Manifest,
) error {
	if err := manifest.Validate(ctx); err != nil {
		return err
	}
	for _, report := range manifest.WriterReports {
		if err := inspector.requireArtifact(
			ctx,
			manifest.Wave,
			report.Path,
			report.Digest,
		); err != nil {
			return err
		}
	}
	for _, report := range manifest.ReviewReports {
		if err := inspector.requireArtifact(
			ctx,
			manifest.Wave,
			report.Path,
			report.Digest,
		); err != nil {
			return err
		}
	}
	for _, report := range manifest.AuditReports {
		if err := inspector.requireArtifact(
			ctx,
			manifest.Wave,
			report.Path,
			report.ReportDigest,
		); err != nil {
			return err
		}
	}
	for _, gate := range manifest.Gates {
		if err := inspector.requireGateArtifact(ctx, manifest, gate); err != nil {
			return err
		}
	}
	return nil
}

// ValidateIntegration checks signatures, lease-bounded snapshots, and the
// applied prefix recorded by the primary.
func (inspector *Inspector) ValidateIntegration(
	ctx context.Context,
	manifest swarmcheck.Manifest,
) error {
	if err := manifest.Validate(ctx); err != nil {
		return err
	}
	if err := inspector.validateSignedBase(ctx, manifest); err != nil {
		return err
	}
	for _, snapshot := range manifest.Snapshots {
		if err := validateManifestSnapshot(manifest, snapshot); err != nil {
			return err
		}
		if err := inspector.validateSnapshotEvidence(ctx, manifest, snapshot); err != nil {
			return err
		}
		if manifest.Phase != swarmcheck.PhaseComplete {
			if err := inspector.requireSnapshotRef(ctx, snapshot, true); err != nil {
				return err
			}
		}
	}
	if err := inspector.validateStoredIntegrationConflicts(ctx, manifest); err != nil {
		return err
	}
	if manifest.Phase == swarmcheck.PhaseReview || manifest.Phase == swarmcheck.PhaseIntegrate {
		if err := inspector.validateActiveWorktrees(ctx, manifest); err != nil {
			return err
		}
	}
	if manifest.Phase == swarmcheck.PhaseComplete {
		if err := inspector.requireWaveResourcesAbsent(ctx, manifest); err != nil {
			return err
		}
	}
	if err := inspector.requireTargetRef(ctx, manifest.TargetRef); err != nil {
		return err
	}
	var expectedFinalTree swarmcheck.TreeID
	if manifest.Phase == swarmcheck.PhaseIntegrate || manifest.Phase == swarmcheck.PhaseComplete {
		prefixCount := len(manifest.Integration)
		if manifest.Phase == swarmcheck.PhaseIntegrate &&
			swarmcheck.HasPendingConflictRemediation(manifest) {
			prefixCount = len(manifest.Applied)
		}
		prefixTrees, err := inspector.expectedPrefixTreesThrough(ctx, manifest, prefixCount)
		if err != nil {
			return err
		}
		if err := validateAppliedTrees(manifest, prefixTrees); err != nil {
			return err
		}
		if len(prefixTrees) != 0 {
			expectedFinalTree = prefixTrees[len(prefixTrees)-1]
		}
		if manifest.Phase == swarmcheck.PhaseComplete &&
			(manifest.Delivery == nil || manifest.Delivery.Tree != expectedFinalTree) {
			return invalid("complete phase delivery does not bind the computed final tree")
		}
	}
	if manifest.Delivery != nil {
		if err := inspector.validateDeliveryEvidence(ctx, manifest, expectedFinalTree); err != nil {
			return err
		}
	}
	return inspector.ValidateReports(ctx, manifest)
}

func (inspector *Inspector) validateDeliveryEvidence(
	ctx context.Context,
	manifest swarmcheck.Manifest,
	expectedTree swarmcheck.TreeID,
) error {
	if manifest.Delivery == nil {
		return invalid("delivery receipt is absent")
	}
	evidence, err := inspector.commit(ctx, string(manifest.Delivery.Commit))
	if err != nil {
		return err
	}
	if evidence.Commit != manifest.Delivery.Commit ||
		!sameValues(evidence.Parents, swarmcheck.ExpectedDeliveryParents(manifest)) ||
		evidence.Tree != manifest.Delivery.Tree || evidence.Tree != expectedTree ||
		evidence.Signature != manifest.Delivery.Signature ||
		evidence.SignerFingerprint != manifest.PrimarySigner ||
		manifest.Delivery.SignerFingerprint != evidence.SignerFingerprint ||
		!swarmcheckSignatureOK(evidence.Signature) {
		return invalid("delivery receipt does not prove the signed primary delivery commit")
	}
	refCommit, err := inspector.resolve(ctx, string(manifest.TargetRef))
	if err != nil || refCommit != manifest.Delivery.Commit {
		return invalid("delivery target ref does not resolve to the signed delivery commit")
	}
	return nil
}

func (inspector *Inspector) validateSignedBase(
	ctx context.Context,
	manifest swarmcheck.Manifest,
) error {
	base, err := inspector.commit(ctx, string(manifest.Base.Commit))
	if err != nil {
		return err
	}
	if base.Commit != manifest.Base.Commit || base.Tree != manifest.Base.Tree ||
		base.Signature != manifest.Base.Signature ||
		base.SignerFingerprint != manifest.Base.SignerFingerprint ||
		base.SignerFingerprint != manifest.PrimarySigner ||
		!swarmcheckSignatureOK(base.Signature) {
		return invalid("live signed base does not match the manifest")
	}
	return nil
}

func writerForLane(manifest swarmcheck.Manifest, lane swarmcheck.Lane) (swarmcheck.Writer, bool) {
	for _, writer := range manifest.Writers {
		if writer.Lane == lane {
			return writer, true
		}
	}
	return swarmcheck.Writer{}, false
}

func invalid(message string) error {
	return errs.New(errs.KindValidationFailed, message)
}
