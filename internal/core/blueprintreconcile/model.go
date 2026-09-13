// Package blueprintreconcile selects still-required Blueprint work and safe
// supersession handoffs. It performs no publication, cancellation or host effects.
package blueprintreconcile

import (
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Fingerprint identifies complete normalized effective inputs. The Controller
// includes consumed configuration generations and bindings, not YAML formatting
// or unrelated dependency settings. It must not expose secret plaintext hashes.
type Fingerprint [sha256.Size]byte

type ResourceKey struct {
	Kind ids.Kind
	ID   string
}

// Unit groups inseparable effects. Reads and Writes are the exact resource
// access sets; After names other desired units whose inputs must be applied
// before this unit starts. Ordering dependencies alone do not alter Fingerprint.
type Unit struct {
	Target      ResourceKey
	Fingerprint Fingerprint
	Reads       []ResourceKey
	Writes      []ResourceKey
	After       []ResourceKey
}

type AppliedState string

const (
	Absent    AppliedState = "absent"
	Applied   AppliedState = "applied"
	Diverged  AppliedState = "diverged"
	Uncertain AppliedState = "uncertain"
)

// AppliedUnit records acknowledged execution, not an earlier desired document.
// Fingerprint retains the last successful inputs, if any, even when later effects
// invalidate them. Absent requires explicit absence authority. Diverged means
// stopped executors and accounted effects allow repair, not that input was applied.
// Uncertain effects still block conflicting work. AffectedWrites comes from the
// observed effects, never from the possibly different latest unit.
// Accounted divergence is recorded separately for each changed write resource;
// its AffectedWrites contains only Target, so settling one cannot clear another.
// Unfinished executors also retain their complete claims in Executions.
type AppliedUnit struct {
	Target         ResourceKey
	State          AppliedState
	Fingerprint    Fingerprint
	AffectedWrites []ResourceKey
}

type ExecutionState string

const (
	Pending  ExecutionState = "pending"
	Running  ExecutionState = "running"
	Draining ExecutionState = "draining"
)

// Execution is one private unit's sealed plan inside its public Apply Task.
// Epoch is zero while Pending and positive after assignment; it fences redispatch.
// Draining retains claims until cancellation and effect accounting are proven.
type Execution struct {
	PlanID string
	TaskID string
	Epoch  int64
	Unit   Unit
	State  ExecutionState
}

// Snapshot contains one validated latest desired set and a consistent read of
// applied and unfinished execution state. Publication identity/revision fences
// belong to its persistence caller; this value is not execution authorization.
type Snapshot struct {
	Desired    []Unit
	Applied    []AppliedUnit
	Executions []Execution
}

type Selection struct {
	Ready           []ResourceKey
	Satisfied       []ResourceKey
	Waiting         []ResourceKey
	ResolveEffects  []ResourceKey
	CancelPending   []string
	CancelRunning   []string
	ContinueRunning []string
}
