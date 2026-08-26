package swarmcheck

import (
	"context"
	"fmt"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	manifestVersion     = 1
	maxManifestBytes    = 1 << 20
	maxWriters          = 10
	maxReserves         = 2
	maxCollection       = 64
	maxStringBytes      = 4096
	maxPathBytes        = 512
	maxIdentifierBytes  = 128
	maxJSONDepth        = 16
	maxReportReferences = 128
)

type snapshotSet struct {
	byCommit     map[CommitID]Snapshot
	byLane       map[Lane][]Snapshot
	activeByLane map[Lane]Snapshot
}

func (manifest Manifest) Validate(ctx context.Context) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	if manifest.Version != manifestVersion {
		return invalid(fmt.Sprintf("manifest version must be %d", manifestVersion))
	}
	if !validRefSegment(manifest.Wave) || !validFingerprint(manifest.PrimarySigner) {
		return invalid("manifest wave is invalid")
	}
	if !validTargetRef(manifest.TargetRef) {
		return invalid("manifest target ref is invalid")
	}
	if err := validatePhase(manifest.Phase); err != nil {
		return err
	}
	if err := validateBase(manifest.Base, manifest.PrimarySigner); err != nil {
		return err
	}
	if len(manifest.Writers) == 0 || len(manifest.Writers) > maxWriters ||
		len(manifest.Reserves) > maxReserves || len(manifest.Snapshots) > maxCollection ||
		len(manifest.Active) > maxWriters || len(manifest.Conflicts) > maxCollection {
		return invalid(
			"manifest writer, reserve, snapshot, or active selection collection is invalid",
		)
	}
	writers, identities, err := validateWriters(manifest)
	if err != nil {
		return err
	}
	reserves, err := validateReserves(manifest, writers, identities)
	if err != nil {
		return err
	}
	if err := validateReviewers(manifest, writers, identities); err != nil {
		return err
	}
	if err := validateAuditors(manifest, identities); err != nil {
		return err
	}
	if _, err := validateSharedAreas(manifest, writers); err != nil {
		return err
	}
	if err := validateIntegration(manifest, writers); err != nil {
		return err
	}
	snapshots, err := validateSnapshots(manifest, writers, reserves)
	if err != nil {
		return err
	}
	conflicts, err := validateIntegrationConflicts(manifest, snapshots)
	if err != nil {
		return err
	}
	if err := validateReports(manifest, snapshots); err != nil {
		return err
	}
	if err := validatePhaseReceipts(manifest, writers, snapshots, conflicts); err != nil {
		return err
	}
	return validatePhaseShape(manifest)
}

// ValidateManifest is the package-level form used by callers that do not
// need repository context.
func ValidateManifest(manifest Manifest) error {
	return manifest.Validate(context.Background())
}

func validateWriters(manifest Manifest) (map[Lane]Writer, map[Identity]struct{}, error) {
	writers := make(map[Lane]Writer, len(manifest.Writers))
	identities := make(map[Identity]struct{}, len(manifest.Writers)+len(manifest.Reserves))
	var leases []RepoPath
	for index, writer := range manifest.Writers {
		if !validRefSegment(writer.Lane) || writer.Lane == "primary" {
			return nil, nil, invalid(fmt.Sprintf("writers[%d].lane is invalid", index))
		}
		if _, exists := writers[writer.Lane]; exists {
			return nil, nil, invalid("writer lanes must be unique")
		}
		if !validIdentity(writer.Identity) {
			return nil, nil, invalid(fmt.Sprintf("writers[%d].identity is invalid", index))
		}
		if _, exists := identities[writer.Identity]; exists {
			return nil, nil, invalid("agent identities must be unique")
		}
		identities[writer.Identity] = struct{}{}
		if writer.Worktree != WorktreePath(manifest.Wave, writer.Lane) ||
			writer.Base != manifest.Base.Commit {
			return nil, nil, invalid(
				fmt.Sprintf("writers[%d] must use the derived worktree at the signed base", index),
			)
		}
		if len(writer.Leases) == 0 || len(writer.Leases) > maxCollection {
			return nil, nil, invalid(fmt.Sprintf("writers[%d].leases is invalid", index))
		}
		for _, lease := range writer.Leases {
			if err := validateLeasePath(lease); err != nil {
				return nil, nil, invalid(fmt.Sprintf("writer %s lease: %v", writer.Lane, err))
			}
			if overlapsPrimaryOnlyPath(lease) {
				return nil, nil, invalid(
					fmt.Sprintf("writer %s lease is primary-only: %s", writer.Lane, lease),
				)
			}
			for _, previous := range leases {
				if pathsOverlap(previous, lease) {
					return nil, nil, invalid(
						fmt.Sprintf("writer leases overlap: %s and %s", previous, lease),
					)
				}
			}
			leases = append(leases, lease)
		}
		writers[writer.Lane] = writer
	}
	return writers, identities, nil
}

func validateIntegrationConflicts(
	manifest Manifest,
	snapshots snapshotSet,
) (map[CommitID]struct{}, error) {
	result := make(map[CommitID]struct{}, len(manifest.Conflicts))
	for index, conflict := range manifest.Conflicts {
		snapshot, exists := snapshots.byCommit[conflict.Snapshot]
		if !exists || conflict.Lane != snapshot.Lane || conflict.Attempt != snapshot.Attempt ||
			!validObjectID(conflict.PrefixCommit) {
			return nil, invalid(fmt.Sprintf("integration_conflicts[%d] is invalid", index))
		}
		prefixLength := len(conflict.PrefixSnapshots)
		if prefixLength >= len(manifest.Integration) || prefixLength > len(manifest.Applied) ||
			conflict.Lane != manifest.Integration[prefixLength] {
			return nil, invalid("integration conflict does not bind the complete preceding prefix")
		}
		for prefixIndex, commit := range conflict.PrefixSnapshots {
			applied := manifest.Applied[prefixIndex]
			if commit != applied.Snapshot || applied.Lane != manifest.Integration[prefixIndex] {
				return nil, invalid(
					"integration conflict prefix differs from the retained applied receipts",
				)
			}
		}
		if _, exists := result[conflict.Snapshot]; exists {
			return nil, invalid("integration conflict evidence must be unique per attempt")
		}
		result[conflict.Snapshot] = struct{}{}
	}
	return result, nil
}

func validateReserves(
	manifest Manifest,
	writers map[Lane]Writer,
	identities map[Identity]struct{},
) (map[Identity]struct{}, error) {
	reserveIdentities := make(map[Identity]struct{}, len(manifest.Reserves))
	seenNames := make(map[ReserveName]struct{}, len(manifest.Reserves))
	worktrees := make(map[RepoPath]struct{}, len(writers)+len(manifest.Reserves))
	for _, writer := range writers {
		worktrees[writer.Worktree] = struct{}{}
	}
	for index, reserve := range manifest.Reserves {
		if !validRefSegment(reserve.Name) || reserve.Name == "primary" ||
			!validIdentity(reserve.Identity) ||
			reserve.Worktree != ReserveWorktreePath(manifest.Wave, reserve.Name) {
			return nil, invalid(fmt.Sprintf("remediation_reserves[%d] is invalid", index))
		}
		if _, exists := seenNames[reserve.Name]; exists {
			return nil, invalid("remediation reserve names must be unique")
		}
		if _, exists := identities[reserve.Identity]; exists {
			return nil, invalid("all agent identities must be unique")
		}
		if _, exists := worktrees[reserve.Worktree]; exists {
			return nil, invalid("writer and remediation worktree paths must be globally unique")
		}
		seenNames[reserve.Name] = struct{}{}
		worktrees[reserve.Worktree] = struct{}{}
		identities[reserve.Identity] = struct{}{}
		reserveIdentities[reserve.Identity] = struct{}{}
	}
	return reserveIdentities, nil
}

func validateReviewers(
	manifest Manifest,
	writers map[Lane]Writer,
	identities map[Identity]struct{},
) error {
	if len(manifest.Reviewers) != len(writers) {
		return invalid("there must be exactly one reviewer per writer")
	}
	seenLanes := make(map[Lane]struct{}, len(manifest.Reviewers))
	for index, reviewer := range manifest.Reviewers {
		if _, exists := writers[reviewer.Lane]; !exists {
			return invalid(fmt.Sprintf("reviewers[%d] references unknown lane", index))
		}
		if _, exists := seenLanes[reviewer.Lane]; exists {
			return invalid("reviewer lanes must be unique")
		}
		seenLanes[reviewer.Lane] = struct{}{}
		if !validIdentity(reviewer.Identity) {
			return invalid(fmt.Sprintf("reviewers[%d].identity is invalid", index))
		}
		if _, exists := identities[reviewer.Identity]; exists {
			return invalid("writer, reserve, and reviewer identities must be distinct")
		}
		identities[reviewer.Identity] = struct{}{}
	}
	return nil
}

func validateAuditors(manifest Manifest, identities map[Identity]struct{}) error {
	if len(manifest.CrossAudits) != 3 {
		return invalid("manifest must contain exactly three cross-audits")
	}
	roles := map[AuditRole]struct{}{AuditProduct: {}, AuditArchitecture: {}, AuditOperations: {}}
	for index, audit := range manifest.CrossAudits {
		if _, exists := roles[audit.Role]; !exists {
			return invalid(fmt.Sprintf("cross_audits[%d] has an invalid role", index))
		}
		delete(roles, audit.Role)
		if !validIdentity(audit.Identity) {
			return invalid(fmt.Sprintf("cross_audits[%d].identity is invalid", index))
		}
		if _, exists := identities[audit.Identity]; exists {
			return invalid("all agent identities must be unique")
		}
		identities[audit.Identity] = struct{}{}
	}
	if len(roles) != 0 {
		return invalid("cross-audit roles must cover the closed role set")
	}
	return nil
}

func validateSharedAreas(manifest Manifest, writers map[Lane]Writer) (map[RepoPath]Lane, error) {
	if len(manifest.SharedAreas) > maxCollection {
		return nil, invalid("shared areas exceed bound")
	}
	seenNames := make(map[SharedAreaName]struct{}, len(manifest.SharedAreas))
	owners := make(map[RepoPath]Lane)
	var allPaths []RepoPath
	for index, area := range manifest.SharedAreas {
		if !validIdentifier(area.Name) || area.Name == "primary" {
			return nil, invalid(fmt.Sprintf("shared_areas[%d].name is invalid", index))
		}
		if _, exists := seenNames[area.Name]; exists {
			return nil, invalid("shared area names must be unique")
		}
		seenNames[area.Name] = struct{}{}
		compiledPaths, compiled := compiledSharedAreas[area.Name]
		if !compiled || !equalValues(area.Paths, compiledPaths) {
			return nil, invalid(
				fmt.Sprintf("shared area %s is not a compiled exact path set", area.Name),
			)
		}
		if _, exists := writers[area.OwnerLane]; !exists {
			return nil, invalid(fmt.Sprintf("shared area %s owner lane is unknown", area.Name))
		}
		for _, value := range area.Paths {
			if err := validateLeasePath(value); err != nil || IsPrimaryOnlyPath(value) {
				return nil, invalid(
					fmt.Sprintf("shared area %s contains invalid path %s", area.Name, value),
				)
			}
			for _, previous := range allPaths {
				if pathsOverlap(previous, value) {
					return nil, invalid(
						fmt.Sprintf("shared paths overlap: %s and %s", previous, value),
					)
				}
			}
			allPaths = append(allPaths, value)
			owners[value] = area.OwnerLane
			owned := false
			for _, lease := range writers[area.OwnerLane].Leases {
				if pathWithin(value, lease) {
					owned = true
				}
			}
			if !owned {
				return nil, invalid(
					fmt.Sprintf("shared area %s path %s is outside owner lease", area.Name, value),
				)
			}
		}
	}
	return owners, nil
}

func validateIntegration(manifest Manifest, writers map[Lane]Writer) error {
	if len(manifest.Integration) != len(writers) {
		return invalid("integration order must contain every writer exactly once")
	}
	seen := make(map[Lane]struct{}, len(manifest.Integration))
	for _, lane := range manifest.Integration {
		if _, exists := writers[lane]; !exists {
			return invalid("integration references unknown lane")
		}
		if _, exists := seen[lane]; exists {
			return invalid("integration lanes must be unique")
		}
		seen[lane] = struct{}{}
	}
	return nil
}

func validateSnapshots(
	manifest Manifest,
	writers map[Lane]Writer,
	reserves map[Identity]struct{},
) (snapshotSet, error) {
	result := snapshotSet{
		byCommit:     make(map[CommitID]Snapshot, len(manifest.Snapshots)),
		byLane:       make(map[Lane][]Snapshot, len(writers)),
		activeByLane: make(map[Lane]Snapshot, len(manifest.Active)),
	}
	seenAttempts := make(map[string]struct{}, len(manifest.Snapshots))
	for index, snapshot := range manifest.Snapshots {
		writer, exists := writers[snapshot.Lane]
		if !exists || !validRefSegment(snapshot.Attempt) || !validIdentity(snapshot.Author) {
			return snapshotSet{}, invalid(
				fmt.Sprintf("snapshots[%d] lane, attempt, or author is invalid", index),
			)
		}
		attemptKey := string(snapshot.Lane) + "\x00" + string(snapshot.Attempt)
		if _, exists := seenAttempts[attemptKey]; exists {
			return snapshotSet{}, invalid("snapshot attempts must be unique per lane")
		}
		if _, exists := result.byCommit[snapshot.Commit]; exists {
			return snapshotSet{}, invalid("snapshot commit ids must be globally unique")
		}
		attempts := result.byLane[snapshot.Lane]
		expectedParent := manifest.Base.Commit
		var expectedReplaces CommitID
		if len(attempts) == 0 {
			if snapshot.Author != writer.Identity {
				return snapshotSet{}, invalid(
					"initial snapshot attempt must be authored by its assigned writer",
				)
			}
		} else {
			previous := attempts[len(attempts)-1]
			expectedParent = previous.Commit
			expectedReplaces = previous.Commit
			if snapshot.Author != writer.Identity {
				if _, authorized := reserves[snapshot.Author]; !authorized {
					return snapshotSet{}, invalid(
						"replacement snapshot author is not the writer or a remediation reserve",
					)
				}
			}
		}
		if snapshot.Author == writer.Identity {
			if snapshot.RemediationLease != "" {
				return snapshotSet{}, invalid(
					"writer-authored attempts cannot declare a remediation lease",
				)
			}
		} else if err := validateRemediationLease(manifest, writer, snapshot); err != nil {
			return snapshotSet{}, err
		}
		if snapshot.Ref != SnapshotRef(manifest.Wave, snapshot.Lane, snapshot.Attempt) ||
			!validObjectID(snapshot.Commit) || !validObjectID(snapshot.Parent) ||
			!validObjectID(snapshot.Tree) || snapshot.Parent != expectedParent ||
			snapshot.Replaces != expectedReplaces || !validSignature(snapshot.Signature) ||
			snapshot.SignerFingerprint != manifest.PrimarySigner {
			return snapshotSet{}, invalid(
				fmt.Sprintf(
					"snapshot %s/%s chain evidence is invalid",
					snapshot.Lane,
					snapshot.Attempt,
				),
			)
		}
		seenAttempts[attemptKey] = struct{}{}
		result.byCommit[snapshot.Commit] = snapshot
		result.byLane[snapshot.Lane] = append(attempts, snapshot)
	}
	for _, active := range manifest.Active {
		if !validRefSegment(active.Lane) || !validRefSegment(active.Attempt) {
			return snapshotSet{}, invalid("active snapshot selection is invalid")
		}
		if _, exists := result.activeByLane[active.Lane]; exists {
			return snapshotSet{}, invalid("active snapshots must select each lane at most once")
		}
		attempts := result.byLane[active.Lane]
		if len(attempts) == 0 {
			return snapshotSet{}, invalid("active snapshot references a lane without attempts")
		}
		tail := attempts[len(attempts)-1]
		if active.Attempt != tail.Attempt || active.Snapshot != tail.Commit {
			return snapshotSet{}, invalid(
				"active snapshot must select the exact immutable chain tail",
			)
		}
		result.activeByLane[active.Lane] = tail
	}
	if len(result.activeByLane) != len(result.byLane) {
		return snapshotSet{}, invalid(
			"every lane with attempts must select exactly one active tail",
		)
	}
	return result, nil
}

func validateRemediationLease(manifest Manifest, writer Writer, snapshot Snapshot) error {
	value := snapshot.RemediationLease
	if err := validateLeasePath(value); err != nil || overlapsPrimaryOnlyPath(value) {
		return invalid("reserve-authored replacement has an invalid remediation lease")
	}
	withinOriginal := false
	for _, lease := range writer.Leases {
		if pathWithin(value, lease) {
			withinOriginal = true
			break
		}
	}
	if !withinOriginal {
		return invalid("remediation lease is outside the original lane lease union")
	}
	for _, other := range manifest.Writers {
		if other.Lane == writer.Lane {
			continue
		}
		for _, lease := range other.Leases {
			if pathsOverlap(value, lease) {
				return invalid("remediation lease overlaps another writer lane")
			}
		}
	}
	for _, sharedPaths := range compiledSharedAreas {
		for _, shared := range sharedPaths {
			if pathsOverlap(value, shared) {
				return invalid("remediation lease overlaps a shared area")
			}
		}
	}
	return nil
}

func validatePhase(phase Phase) error {
	switch phase {
	case PhaseDispatch, PhaseWriting, PhaseReview, PhaseIntegrate, PhaseComplete:
		return nil
	default:
		return invalid("manifest phase is invalid")
	}
}

func validateBase(base Base, primarySigner Fingerprint) error {
	if !validObjectID(base.Commit) || !validObjectID(base.Tree) ||
		!validSignature(base.Signature) ||
		base.SignerFingerprint != primarySigner {
		return invalid("base must be a signed verified commit and tree")
	}
	return nil
}

func validObjectID[T ~string](value T) bool {
	return validHexIdentifier(value, 40, 64)
}

func validSignature(value string) bool { return value == "G" || value == "U" || value == "Y" }

func validAuditRole(value AuditRole) bool {
	switch value {
	case AuditProduct, AuditArchitecture, AuditOperations:
		return true
	default:
		return false
	}
}

func validFingerprint[T ~string](value T) bool {
	raw := string(value)
	return raw == strings.ToLower(raw) && validHexIdentifier(value, 40, 64)
}

func validHexIdentifier[T ~string](value T, lengths ...int) bool {
	raw := string(value)
	validLength := false
	for _, length := range lengths {
		if len(raw) == length {
			validLength = true
			break
		}
	}
	if !validLength {
		return false
	}
	for _, character := range []byte(raw) {
		if !((character >= '0' && character <= '9') || character >= 'a' && character <= 'f' ||
			character >= 'A' && character <= 'F') {
			return false
		}
	}
	return true
}

// ParseCommitID validates one Git commit identifier at a process boundary.
func ParseCommitID(value string) (CommitID, error) {
	if !validObjectID(value) {
		return "", invalid("Git commit id is invalid")
	}
	return CommitID(value), nil
}

// ParseTreeID validates one Git tree identifier at a process boundary.
func ParseTreeID(value string) (TreeID, error) {
	if !validObjectID(value) {
		return "", invalid("Git tree id is invalid")
	}
	return TreeID(value), nil
}

// ParseFingerprint validates one signing fingerprint at a process boundary.
func ParseFingerprint(value string) (Fingerprint, error) {
	if !validFingerprint(value) {
		return "", invalid("Git signing fingerprint is invalid")
	}
	return Fingerprint(value), nil
}

// ParseRef validates one attached branch ref at a process boundary.
func ParseRef(value string) (Ref, error) {
	ref := Ref(value)
	if !validTargetRef(ref) {
		return "", invalid("Git branch ref is invalid")
	}
	return ref, nil
}

func validDigest[T ~string](value T) bool { return validFingerprint(value) && len(value) == 64 }

func validTargetRef(value Ref) bool {
	raw := string(value)
	if len(raw) <= len("refs/heads/") || len(raw) > maxPathBytes || !isASCII(raw) ||
		!strings.HasPrefix(raw, "refs/heads/") || strings.ContainsAny(raw, "\\ ~^:?*[\t\r\n") ||
		strings.Contains(raw, "..") || strings.Contains(raw, "@{") ||
		strings.HasSuffix(raw, "/") || strings.HasSuffix(raw, ".") {
		return false
	}
	for _, segment := range strings.Split(strings.TrimPrefix(raw, "refs/heads/"), "/") {
		if segment == "" || strings.HasPrefix(segment, ".") || strings.HasSuffix(segment, ".lock") {
			return false
		}
	}
	for index := 0; index < len(raw); index++ {
		if raw[index] < 0x21 || raw[index] == 0x7f {
			return false
		}
	}
	return true
}

func validIdentity(value Identity) bool { return validIdentifier(value) }

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func invalid(message string) error { return errs.New(errs.KindValidationFailed, message) }
