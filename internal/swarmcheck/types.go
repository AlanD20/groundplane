// Package swarmcheck validates the bounded, primary-owned manifest used to
// coordinate pre-MVP implementation waves.
package swarmcheck

// CommitID is a validated Git commit object identifier.
type CommitID string

// TreeID is a validated Git tree object identifier.
type TreeID string

// Lane is a validated writer-lane identifier.
type Lane string

// Identity is a validated actor identity.
type Identity string

// Fingerprint is a validated lowercase Git signing-key fingerprint.
type Fingerprint string

// Digest is a validated lowercase SHA-256 digest.
type Digest string

// RepoPath is a validated canonical repository-relative path.
type RepoPath string

// Ref is a validated full Git ref.
type Ref string

// WaveID is a validated swarm-wave identifier.
type WaveID string

// AttemptID is a validated immutable lane-attempt identifier.
type AttemptID string

// ReserveName is a validated remediation-reserve name.
type ReserveName string

// SharedAreaName is a validated compiled shared-area name.
type SharedAreaName string

// ChangedPath is one validated tracked, untracked, or deleted repository path.
type ChangedPath struct {
	Path RepoPath
}

// Phase is the only lifecycle phase a swarm wave may occupy.
type Phase string

const (
	PhaseDispatch  Phase = "dispatch"
	PhaseWriting   Phase = "writing"
	PhaseReview    Phase = "review"
	PhaseIntegrate Phase = "integrate"
	PhaseComplete  Phase = "complete"
)

// AuditRole is one of the three independent cross-contract audit positions.
type AuditRole string

const (
	AuditProduct      AuditRole = "product-parity"
	AuditArchitecture AuditRole = "architecture-boundaries"
	AuditOperations   AuditRole = "operations-and-delivery"
)

// Manifest is the complete durable state for one swarm wave.
type Manifest struct {
	Version       int                   `json:"version"`
	Wave          WaveID                `json:"wave"`
	Phase         Phase                 `json:"phase"`
	PrimarySigner Fingerprint           `json:"primary_signer"`
	TargetRef     Ref                   `json:"target_ref"`
	Base          Base                  `json:"base"`
	Writers       []Writer              `json:"writers"`
	Reserves      []RemediationReserve  `json:"remediation_reserves"`
	Reviewers     []Reviewer            `json:"reviewers"`
	CrossAudits   []CrossAudit          `json:"cross_audits"`
	SharedAreas   []SharedArea          `json:"shared_areas"`
	Integration   []Lane                `json:"integration"`
	Snapshots     []Snapshot            `json:"snapshots"`
	Active        []ActiveSnapshot      `json:"active_snapshots"`
	Conflicts     []IntegrationConflict `json:"integration_conflicts"`
	WriterReports []ArtifactReport      `json:"writer_reports"`
	ReviewReports []ArtifactReport      `json:"review_reports"`
	AuditReports  []AuditReport         `json:"audit_reports"`
	Reviews       []ReviewReceipt       `json:"reviews"`
	Audits        []AuditReceipt        `json:"audits"`
	Applied       []AppliedReceipt      `json:"applied"`
	Gates         []GateReceipt         `json:"gates"`
	Delivery      *DeliveryReceipt      `json:"delivery"`
}

// Base identifies the one signed commit from which every writer worktree and
// snapshot must descend directly.
type Base struct {
	Commit            CommitID    `json:"commit"`
	Tree              TreeID      `json:"tree"`
	Signature         string      `json:"signature"`
	SignerFingerprint Fingerprint `json:"signer_fingerprint"`
}

// Writer owns one derived detached worktree and one or more canonical leases.
type Writer struct {
	Lane     Lane       `json:"lane"`
	Identity Identity   `json:"identity"`
	Worktree RepoPath   `json:"worktree"`
	Base     CommitID   `json:"base"`
	Leases   []RepoPath `json:"leases"`
}

// RemediationReserve is an identity and temporary worktree authorized to
// author a replacement attempt after an immutable paired rejection.
type RemediationReserve struct {
	Name     ReserveName `json:"name"`
	Identity Identity    `json:"identity"`
	Worktree RepoPath    `json:"worktree"`
}

// Reviewer is a read-only identity assigned to one writer lane.
type Reviewer struct {
	Lane     Lane     `json:"lane"`
	Identity Identity `json:"identity"`
}

// CrossAudit is a read-only identity assigned to one cross-contract role.
type CrossAudit struct {
	Role     AuditRole `json:"role"`
	Identity Identity  `json:"identity"`
}

// SharedArea is a closed set of exact paths whose ownership is selected in
// the manifest. It is deliberately not a wildcard or directory prefix.
type SharedArea struct {
	Name      SharedAreaName `json:"name"`
	Paths     []RepoPath     `json:"paths"`
	OwnerLane Lane           `json:"owner_lane"`
}

// Snapshot is the primary-created immutable signed commit for one writer.
type Snapshot struct {
	Lane              Lane        `json:"lane"`
	Attempt           AttemptID   `json:"attempt"`
	Author            Identity    `json:"author"`
	Ref               Ref         `json:"ref"`
	Commit            CommitID    `json:"commit"`
	Parent            CommitID    `json:"parent"`
	Replaces          CommitID    `json:"replaces"`
	Tree              TreeID      `json:"tree"`
	Signature         string      `json:"signature"`
	SignerFingerprint Fingerprint `json:"signer_fingerprint"`
	RemediationLease  RepoPath    `json:"remediation_lease"`
}

// WritingAssignment selects the worktree, expected HEAD, and optional active
// attempt whose stopped bytes are being inspected for one lane.
type WritingAssignment struct {
	Worktree RepoPath
	Head     CommitID
	Attempt  Snapshot
}

// ActiveSnapshot selects the one immutable tail used by review, audits,
// integration, gates, and delivery for a lane.
type ActiveSnapshot struct {
	Lane     Lane      `json:"lane"`
	Attempt  AttemptID `json:"attempt"`
	Snapshot CommitID  `json:"snapshot"`
}

// IntegrationConflict identifies one machine-recomputable conflict between
// an immutable attempt and the exact ordered prefix that preceded its lane.
type IntegrationConflict struct {
	Lane            Lane       `json:"lane"`
	Attempt         AttemptID  `json:"attempt"`
	Snapshot        CommitID   `json:"snapshot"`
	PrefixSnapshots []CommitID `json:"prefix_snapshots"`
	PrefixCommit    CommitID   `json:"prefix_commit"`
}

// ArtifactReport binds an on-disk report to the exact wave and source state
// it describes. Artifact paths are validated relative to the derived wave
// artifact root by ValidateArtifactPath.
type ArtifactReport struct {
	Wave     WaveID    `json:"wave"`
	Lane     Lane      `json:"lane"`
	Attempt  AttemptID `json:"attempt"`
	Identity Identity  `json:"identity"`
	Base     CommitID  `json:"base"`
	Snapshot CommitID  `json:"snapshot"`
	Path     RepoPath  `json:"path"`
	Digest   Digest    `json:"digest"`
}

// ReviewReceipt is the primary-recorded approval for one exact snapshot.
type ReviewReceipt struct {
	Lane         Lane      `json:"lane"`
	Attempt      AttemptID `json:"attempt"`
	Reviewer     Identity  `json:"reviewer"`
	Snapshot     CommitID  `json:"snapshot"`
	Approved     bool      `json:"approved"`
	Report       RepoPath  `json:"report"`
	ReportDigest Digest    `json:"report_digest"`
}

// AuditReport binds a cross-audit artifact to the sorted snapshot set.
type AuditReport struct {
	Wave           WaveID     `json:"wave"`
	Identity       Identity   `json:"identity"`
	Role           AuditRole  `json:"role"`
	Base           CommitID   `json:"base"`
	Snapshots      []CommitID `json:"snapshots"`
	SnapshotDigest Digest     `json:"snapshot_digest"`
	ReportDigest   Digest     `json:"report_digest"`
	Path           RepoPath   `json:"path"`
}

// AuditReceipt is the primary-recorded result of one cross-audit.
type AuditReceipt struct {
	Wave           WaveID     `json:"wave"`
	Identity       Identity   `json:"identity"`
	Role           AuditRole  `json:"role"`
	Base           CommitID   `json:"base"`
	Snapshots      []CommitID `json:"snapshots"`
	SnapshotDigest Digest     `json:"snapshot_digest"`
	Report         RepoPath   `json:"report"`
	ReportDigest   Digest     `json:"report_digest"`
	Approved       bool       `json:"approved"`
}

// AppliedReceipt records one ordered snapshot application and the primary
// index tree captured immediately after it for crash recovery.
type AppliedReceipt struct {
	Lane             Lane     `json:"lane"`
	Snapshot         CommitID `json:"snapshot"`
	PrimaryIndexTree TreeID   `json:"primary_index_tree"`
}

// GateID is one compiled repository or operator validation command.
type GateID string

const (
	GateRepositoryCI     GateID = "repository-ci"
	GateOperatorVerifier GateID = "operator-verifier"
)

// GateReceipt binds a machine-produced gate artifact to the signed delivery
// candidate. Pass/fail and command identity are derived from that artifact.
type GateReceipt struct {
	Wave      WaveID   `json:"wave"`
	Gate      GateID   `json:"gate"`
	Candidate CommitID `json:"candidate"`
	Tree      TreeID   `json:"tree"`
	Artifact  RepoPath `json:"artifact"`
	Digest    Digest   `json:"digest"`
}

// DeliveryReceipt is the primary's final signed commit receipt on TargetRef.
type DeliveryReceipt struct {
	Commit            CommitID    `json:"commit"`
	Parents           []CommitID  `json:"parents"`
	Tree              TreeID      `json:"tree"`
	Signature         string      `json:"signature"`
	SignerFingerprint Fingerprint `json:"signer_fingerprint"`
	TargetRef         Ref         `json:"target_ref"`
}

// Validate checks a manifest's closed schema and all phase-independent
// invariants. Git-backed evidence is checked by internal/swarmgit.
