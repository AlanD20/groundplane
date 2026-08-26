package swarmcheck

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

func validateReports(manifest Manifest, snapshots snapshotSet) error {
	if len(manifest.WriterReports) > maxReportReferences ||
		len(manifest.ReviewReports) > maxReportReferences ||
		len(manifest.AuditReports) > maxReportReferences {
		return invalid("report collection exceeds bound")
	}
	usedPaths := make(map[RepoPath]struct{})
	addPath := func(value RepoPath) error {
		if _, exists := usedPaths[value]; exists {
			return invalid("report paths must be globally unique")
		}
		usedPaths[value] = struct{}{}
		return nil
	}
	reviewers := make(map[Lane]Reviewer, len(manifest.Reviewers))
	for _, reviewer := range manifest.Reviewers {
		reviewers[reviewer.Lane] = reviewer
	}
	seenWriterSnapshots := make(map[CommitID]struct{}, len(manifest.WriterReports))
	for _, report := range manifest.WriterReports {
		snapshot, err := validateArtifactReport(manifest, report, snapshots)
		if err != nil {
			return err
		}
		if report.Identity != snapshot.Author {
			return invalid("writer report identity does not match the attempt author")
		}
		if _, exists := seenWriterSnapshots[report.Snapshot]; exists {
			return invalid("writer reports must contain at most one report per attempt")
		}
		seenWriterSnapshots[report.Snapshot] = struct{}{}
		if err := addPath(report.Path); err != nil {
			return err
		}
	}
	seenReviewSnapshots := make(map[CommitID]struct{}, len(manifest.ReviewReports))
	for _, report := range manifest.ReviewReports {
		_, err := validateArtifactReport(manifest, report, snapshots)
		if err != nil {
			return err
		}
		reviewer, exists := reviewers[report.Lane]
		if !exists || report.Identity != reviewer.Identity {
			return invalid("review report identity does not match the assigned reviewer")
		}
		if _, exists := seenReviewSnapshots[report.Snapshot]; exists {
			return invalid("review reports must contain at most one report per attempt")
		}
		seenReviewSnapshots[report.Snapshot] = struct{}{}
		if err := addPath(report.Path); err != nil {
			return err
		}
	}

	auditors := make(map[AuditRole]CrossAudit, len(manifest.CrossAudits))
	for _, auditor := range manifest.CrossAudits {
		auditors[auditor.Role] = auditor
	}
	seenAuditRoles := make(map[string]struct{}, len(manifest.AuditReports))
	for _, report := range manifest.AuditReports {
		auditor, exists := auditors[report.Role]
		if !exists || report.Wave != manifest.Wave || report.Identity != auditor.Identity ||
			report.Base != manifest.Base.Commit ||
			!completeKnownAttemptSet(report.Snapshots, snapshots) ||
			report.SnapshotDigest != auditSnapshotDigest(report.Snapshots) ||
			!validDigest(report.ReportDigest) {
			return invalid("audit report binding is invalid")
		}
		auditKey := string(report.Role) + "\x00" + string(report.SnapshotDigest)
		if _, exists := seenAuditRoles[auditKey]; exists {
			return invalid("audit history must contain at most one report per role and snapshot set")
		}
		seenAuditRoles[auditKey] = struct{}{}
		if err := ValidateArtifactPath(manifest.Wave, report.Path); err != nil {
			return invalid(fmt.Sprintf("audit report path: %v", err))
		}
		if err := addPath(report.Path); err != nil {
			return err
		}
	}
	for _, gate := range manifest.Gates {
		if err := ValidateArtifactPath(manifest.Wave, gate.Artifact); err != nil {
			return invalid(fmt.Sprintf("gate artifact path: %v", err))
		}
		if err := addPath(gate.Artifact); err != nil {
			return err
		}
	}
	return nil
}

func validateArtifactReport(
	manifest Manifest,
	report ArtifactReport,
	snapshots snapshotSet,
) (Snapshot, error) {
	if err := ValidateArtifactPath(manifest.Wave, report.Path); err != nil {
		return Snapshot{}, invalid(fmt.Sprintf("artifact report path: %v", err))
	}
	snapshot, exists := snapshots.byCommit[report.Snapshot]
	if report.Wave != manifest.Wave || report.Base != manifest.Base.Commit ||
		!validIdentifier(report.Lane) || !validRefSegment(report.Attempt) ||
		!validIdentity(report.Identity) || !validDigest(report.Digest) || !exists ||
		report.Lane != snapshot.Lane || report.Attempt != snapshot.Attempt {
		return Snapshot{}, invalid("artifact report does not bind the exact lane attempt")
	}
	return snapshot, nil
}

func validatePhaseReceipts(
	manifest Manifest,
	writers map[Lane]Writer,
	snapshots snapshotSet,
	conflicts map[CommitID]struct{},
) error {
	phase := phaseRank(manifest.Phase)
	if err := validateAttemptReportPairs(manifest, snapshots); err != nil {
		return err
	}
	if phase >= phaseRank(PhaseReview) && len(snapshots.activeByLane) != len(writers) {
		return invalid("review and later phases require one active attempt for every writer")
	}
	reviews, err := validateReviewPairs(manifest, snapshots)
	if err != nil {
		return err
	}
	allActiveApproved := activeReviewsApproved(snapshots, reviews)
	approvedAudits, rejectedAudits, err := validateAuditPairs(manifest, snapshots, reviews)
	if err != nil {
		return err
	}
	if err := validateReplacementRejections(snapshots, reviews, rejectedAudits, conflicts); err != nil {
		return err
	}
	if phase >= phaseRank(PhaseIntegrate) {
		if err := validateAppliedPrefix(manifest, snapshots); err != nil {
			return err
		}
		remediatingConflict := currentConflictRemediation(manifest, snapshots)
		if (!allActiveApproved || approvedAudits != 3) && !remediatingConflict {
			return invalid(
				"integrate and later phases require all active paired and cross-audit approvals",
			)
		}
		if remediatingConflict && (manifest.Delivery != nil || len(manifest.Gates) != 0) {
			return invalid("conflict remediation forbids gates and delivery until current approvals")
		}
	}
	if err := validateDeliveryAndGates(manifest, writers, snapshots); err != nil {
		return err
	}
	if manifest.Phase == PhaseComplete &&
		(len(manifest.Applied) != len(writers) || manifest.Delivery == nil || len(manifest.Gates) != 2) {
		return invalid(
			"complete phase requires every application, the delivery, and both closed gates",
		)
	}
	return nil
}

func currentConflictRemediation(manifest Manifest, snapshots snapshotSet) bool {
	if manifest.Phase != PhaseIntegrate || len(manifest.Applied) >= len(manifest.Integration) {
		return false
	}
	nextLane := manifest.Integration[len(manifest.Applied)]
	attempts := snapshots.byLane[nextLane]
	active, exists := snapshots.activeByLane[nextLane]
	if !exists || len(attempts) < 2 || attempts[len(attempts)-1].Commit != active.Commit {
		return false
	}
	for _, conflict := range manifest.Conflicts {
		if conflict.Lane != nextLane || len(conflict.PrefixSnapshots) != len(manifest.Applied) {
			continue
		}
		for index, attempt := range attempts {
			if attempt.Commit == conflict.Snapshot && index < len(attempts)-1 {
				return true
			}
		}
	}
	return false
}

// HasPendingConflictRemediation reports whether integrate must validate only
// the retained applied prefix because the next lane is conflicted or its
// replacement chain has not completed current review and audit approval.
func HasPendingConflictRemediation(manifest Manifest) bool {
	if manifest.Phase != PhaseIntegrate || len(manifest.Applied) >= len(manifest.Integration) {
		return false
	}
	nextLane := manifest.Integration[len(manifest.Applied)]
	var selected CommitID
	for _, active := range manifest.Active {
		if active.Lane == nextLane {
			selected = active.Snapshot
			break
		}
	}
	for _, conflict := range manifest.Conflicts {
		if conflict.Lane != nextLane || len(conflict.PrefixSnapshots) != len(manifest.Applied) {
			continue
		}
		if selected == conflict.Snapshot {
			return true
		}
		descended := false
		seenConflict := false
		for _, snapshot := range manifest.Snapshots {
			if snapshot.Lane != nextLane {
				continue
			}
			if snapshot.Commit == conflict.Snapshot {
				seenConflict = true
			}
			if seenConflict && snapshot.Commit == selected {
				descended = true
				break
			}
		}
		if descended && !currentApprovalsComplete(manifest) {
			return true
		}
	}
	return false
}

func currentApprovalsComplete(manifest Manifest) bool {
	approvedReviews := make(map[CommitID]struct{}, len(manifest.Reviews))
	for _, review := range manifest.Reviews {
		if review.Approved {
			approvedReviews[review.Snapshot] = struct{}{}
		}
	}
	active := make([]CommitID, 0, len(manifest.Active))
	for _, selected := range manifest.Active {
		if _, approved := approvedReviews[selected.Snapshot]; !approved {
			return false
		}
		active = append(active, selected.Snapshot)
	}
	sort.Slice(active, func(left, right int) bool { return active[left] < active[right] })
	roles := make(map[AuditRole]struct{}, 3)
	for _, audit := range manifest.Audits {
		if audit.Approved && equalValues(audit.Snapshots, active) {
			roles[audit.Role] = struct{}{}
		}
	}
	return len(active) != 0 && len(roles) == 3
}

func validateAttemptReportPairs(manifest Manifest, snapshots snapshotSet) error {
	reports := make(map[CommitID]ArtifactReport, len(manifest.WriterReports))
	for _, report := range manifest.WriterReports {
		reports[report.Snapshot] = report
	}
	if len(reports) != len(snapshots.byCommit) {
		return invalid("snapshot attempts and writer reports must be added as atomic pairs")
	}
	for commit := range snapshots.byCommit {
		if _, exists := reports[commit]; !exists {
			return invalid("snapshot attempt is missing its atomic writer report")
		}
	}
	return nil
}

func validateReviewPairs(
	manifest Manifest,
	snapshots snapshotSet,
) (map[CommitID]ReviewReceipt, error) {
	reports := make(map[CommitID]ArtifactReport, len(manifest.ReviewReports))
	for _, report := range manifest.ReviewReports {
		reports[report.Snapshot] = report
	}
	if len(reports) != len(manifest.Reviews) {
		return nil, invalid("review reports and receipts must be added as atomic attempt pairs")
	}
	reviewers := make(map[Lane]Reviewer, len(manifest.Reviewers))
	for _, reviewer := range manifest.Reviewers {
		reviewers[reviewer.Lane] = reviewer
	}
	result := make(map[CommitID]ReviewReceipt, len(manifest.Reviews))
	for _, review := range manifest.Reviews {
		snapshot, snapshotExists := snapshots.byCommit[review.Snapshot]
		reviewer, reviewerExists := reviewers[review.Lane]
		report, reportExists := reports[review.Snapshot]
		if !snapshotExists || !reviewerExists || !reportExists || review.Lane != snapshot.Lane ||
			review.Attempt != snapshot.Attempt || review.Reviewer != reviewer.Identity ||
			review.Report != report.Path || review.ReportDigest != report.Digest {
			return nil, invalid(
				"review receipt does not bind its assigned reviewer, report, and attempt",
			)
		}
		if _, exists := result[review.Snapshot]; exists {
			return nil, invalid("review receipts must contain at most one receipt per attempt")
		}
		result[review.Snapshot] = review
	}
	return result, nil
}

func validateReplacementRejections(
	snapshots snapshotSet,
	reviews map[CommitID]ReviewReceipt,
	rejectedAudits map[CommitID]struct{},
	conflicts map[CommitID]struct{},
) error {
	for _, attempts := range snapshots.byLane {
		for index := 1; index < len(attempts); index++ {
			previous := attempts[index-1]
			review, exists := reviews[previous.Commit]
			_, auditRejected := rejectedAudits[previous.Commit]
			_, integrationConflict := conflicts[previous.Commit]
			if (!exists || review.Approved) && !auditRejected && !integrationConflict {
				return invalid(
					"replacement attempt requires a rejecting paired review, bound audit, or recomputable integration conflict",
				)
			}
		}
	}
	return nil
}

func activeReviewsApproved(snapshots snapshotSet, reviews map[CommitID]ReviewReceipt) bool {
	if len(snapshots.activeByLane) == 0 {
		return false
	}
	for _, snapshot := range snapshots.activeByLane {
		review, exists := reviews[snapshot.Commit]
		if !exists || !review.Approved {
			return false
		}
	}
	return true
}

func validateAuditPairs(
	manifest Manifest,
	snapshots snapshotSet,
	reviews map[CommitID]ReviewReceipt,
) (int, map[CommitID]struct{}, error) {
	reports := make(map[RepoPath]AuditReport, len(manifest.AuditReports))
	for _, report := range manifest.AuditReports {
		reports[report.Path] = report
	}
	if len(reports) != len(manifest.Audits) {
		return 0, nil, invalid("audit reports and receipts must be added as atomic history pairs")
	}
	expectedSnapshots := sortedActiveSnapshotCommits(snapshots)
	seenReports := make(map[RepoPath]struct{}, len(manifest.Audits))
	seenSets := make(map[string]struct{}, len(manifest.Audits))
	rejected := make(map[CommitID]struct{})
	approved := 0
	for _, receipt := range manifest.Audits {
		report, exists := reports[receipt.Report]
		if !exists || receipt.Wave != report.Wave || receipt.Identity != report.Identity ||
			receipt.Role != report.Role || receipt.Base != report.Base ||
			!equalValues(receipt.Snapshots, report.Snapshots) ||
			receipt.SnapshotDigest != report.SnapshotDigest || receipt.Report != report.Path ||
			receipt.SnapshotDigest != auditSnapshotDigest(receipt.Snapshots) ||
			receipt.ReportDigest != report.ReportDigest {
			return 0, nil, invalid(
				"audit receipt does not bind its assigned auditor and immutable attempt set",
			)
		}
		if _, exists := seenReports[receipt.Report]; exists {
			return 0, nil, invalid("audit report has more than one receipt")
		}
		seenReports[receipt.Report] = struct{}{}
		setKey := string(receipt.Role) + "\x00" + string(receipt.SnapshotDigest)
		if _, exists := seenSets[setKey]; exists {
			return 0, nil, invalid(
				"audit history must contain at most one receipt per role and snapshot set",
			)
		}
		seenSets[setKey] = struct{}{}
		for _, commit := range receipt.Snapshots {
			review, exists := reviews[commit]
			if !exists || !review.Approved {
				return 0, nil, invalid(
					"audit history requires approved paired reviews for its complete attempt set",
				)
			}
		}
		if receipt.Approved && equalValues(receipt.Snapshots, expectedSnapshots) {
			approved++
		}
		if !receipt.Approved {
			for _, commit := range receipt.Snapshots {
				rejected[commit] = struct{}{}
			}
		}
	}
	return approved, rejected, nil
}

func completeKnownAttemptSet(values []CommitID, snapshots snapshotSet) bool {
	if len(values) != len(snapshots.byLane) || !sortedUnique(values) {
		return false
	}
	lanes := make(map[Lane]struct{}, len(values))
	for _, commit := range values {
		snapshot, exists := snapshots.byCommit[commit]
		if !exists {
			return false
		}
		if _, exists := lanes[snapshot.Lane]; exists {
			return false
		}
		lanes[snapshot.Lane] = struct{}{}
	}
	return len(lanes) == len(snapshots.byLane)
}

func validateDeliveryAndGates(
	manifest Manifest,
	writers map[Lane]Writer,
	snapshots snapshotSet,
) error {
	if len(manifest.Gates) > 2 {
		return invalid("gate receipts exceed the closed gate policy")
	}
	if manifest.Delivery == nil {
		if len(manifest.Gates) != 0 {
			return invalid("gate receipts require a signed delivery candidate")
		}
		return nil
	}
	if len(manifest.Applied) != len(writers) || !validObjectID(manifest.Delivery.Commit) ||
		!validObjectID(manifest.Delivery.Tree) || !validSignature(manifest.Delivery.Signature) ||
		manifest.Delivery.SignerFingerprint != manifest.PrimarySigner ||
		manifest.Delivery.TargetRef != manifest.TargetRef ||
		!equalValues(manifest.Delivery.Parents, ExpectedDeliveryParents(manifest)) {
		return invalid("delivery receipt is not bound to the ordered active snapshot parent set")
	}
	finalTree := manifest.Applied[len(manifest.Applied)-1].PrimaryIndexTree
	if manifest.Delivery.Tree != finalTree {
		return invalid("delivery tree does not match the final applied index tree")
	}
	seen := make(map[GateID]struct{}, len(manifest.Gates))
	for _, gate := range manifest.Gates {
		if !validGateID(gate.Gate) || gate.Wave != manifest.Wave ||
			gate.Candidate != manifest.Delivery.Commit ||
			gate.Tree != manifest.Delivery.Tree ||
			gate.Artifact != GateArtifactPath(manifest.Wave, gate.Gate) ||
			!validDigest(gate.Digest) {
			return invalid("gate receipt does not bind the closed gate and delivery candidate")
		}
		if _, exists := seen[gate.Gate]; exists {
			return invalid("gate receipts must contain each closed gate at most once")
		}
		seen[gate.Gate] = struct{}{}
	}
	return nil
}

func validatePhaseShape(manifest Manifest) error {
	switch manifest.Phase {
	case PhaseDispatch:
		if hasSnapshotOrLaterEvidence(manifest) {
			return invalid("dispatch phase cannot contain snapshot or later evidence")
		}
	case PhaseWriting:
		if len(manifest.ReviewReports) != 0 || len(manifest.AuditReports) != 0 || len(manifest.Reviews) != 0 ||
			len(manifest.Audits) != 0 ||
			len(manifest.Conflicts) != 0 ||
			len(manifest.Applied) != 0 ||
			len(manifest.Gates) != 0 ||
			manifest.Delivery != nil {
			return invalid(
				"writing phase accepts only atomic snapshot attempt and writer report pairs",
			)
		}
	case PhaseReview:
		if len(manifest.Conflicts) != 0 || len(manifest.Applied) != 0 ||
			len(manifest.Gates) != 0 || manifest.Delivery != nil {
			return invalid("review phase cannot contain integration, gate, or delivery evidence")
		}
	}
	return nil
}

func hasSnapshotOrLaterEvidence(manifest Manifest) bool {
	return len(manifest.Snapshots) != 0 || len(manifest.Active) != 0 || len(manifest.Conflicts) != 0 ||
		len(manifest.WriterReports) != 0 ||
		len(manifest.ReviewReports) != 0 || len(manifest.AuditReports) != 0 ||
		len(manifest.Reviews) != 0 || len(manifest.Audits) != 0 ||
		len(manifest.Applied) != 0 ||
		len(manifest.Gates) != 0 ||
		manifest.Delivery != nil
}

func phaseRank(phase Phase) int {
	switch phase {
	case PhaseDispatch:
		return 1
	case PhaseWriting:
		return 2
	case PhaseReview:
		return 3
	case PhaseIntegrate:
		return 4
	case PhaseComplete:
		return 5
	default:
		return 0
	}
}

func validateAppliedPrefix(manifest Manifest, snapshots snapshotSet) error {
	if len(manifest.Applied) > len(manifest.Integration) {
		return invalid("too many applied receipts")
	}
	for index, receipt := range manifest.Applied {
		lane := manifest.Integration[index]
		snapshot, exists := snapshots.activeByLane[lane]
		if !exists || receipt.Lane != lane || receipt.Snapshot != snapshot.Commit ||
			!validObjectID(receipt.PrimaryIndexTree) {
			return invalid(
				"applied receipts must form an ordered active-snapshot prefix with index trees",
			)
		}
	}
	return nil
}

// ExpectedDeliveryParents returns the required signed delivery parent order:
// the wave base first, then each active snapshot in integration order.
func ExpectedDeliveryParents(manifest Manifest) []CommitID {
	active := make(map[Lane]CommitID, len(manifest.Active))
	for _, snapshot := range manifest.Active {
		active[snapshot.Lane] = snapshot.Snapshot
	}
	parents := make([]CommitID, 0, len(manifest.Integration)+1)
	parents = append(parents, manifest.Base.Commit)
	for _, lane := range manifest.Integration {
		parents = append(parents, active[lane])
	}
	return parents
}

func sortedActiveSnapshotCommits(snapshots snapshotSet) []CommitID {
	result := make([]CommitID, 0, len(snapshots.activeByLane))
	for _, snapshot := range snapshots.activeByLane {
		result = append(result, snapshot.Commit)
	}
	sort.Slice(result, func(left, right int) bool { return result[left] < result[right] })
	return result
}

func sortedUnique(values []CommitID) bool {
	if len(values) == 0 || len(values) > maxCollection {
		return false
	}
	for index, value := range values {
		if !validObjectID(value) || index > 0 && values[index-1] >= value {
			return false
		}
	}
	return true
}

func equalValues[T comparable](left, right []T) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func auditSnapshotDigest(snapshots []CommitID) Digest {
	values := make([]string, len(snapshots))
	for index, snapshot := range snapshots {
		values[index] = string(snapshot)
	}
	hash := sha256.Sum256([]byte(strings.Join(values, "\n")))
	return Digest(hex.EncodeToString(hash[:]))
}
