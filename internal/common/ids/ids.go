// Package ids is the ONE id implementation used everywhere a stable id
// is needed — Controller-generated ids and mock fixtures alike. Shape:
// <kind>_<26-char ULID>. No hyphens, no shas, no slugs; the ULID's
// 48-bit timestamp prefix makes ids chronologically sortable (etcd
// ranges, deploy history, the activity journal order for free) and its
// 80 bits of randomness make them collision-safe. See mvp.md, "Stable
// identifiers (locked)".
package ids

import (
	"crypto/rand"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

var (
	monotonicMu      sync.Mutex
	monotonicEntropy = ulid.Monotonic(rand.Reader, 0)
)

// Kind is the short, readable prefix on every id.
type Kind string

const (
	KindTenant         Kind = "tnt"
	KindProject        Kind = "prj"
	KindEnvironment    Kind = "env"
	KindService        Kind = "svc"
	KindDeployment     Kind = "dep" // blueprint.md: deployment_id: dep_01J...
	KindEnvEntry       Kind = "ev"
	KindVolume         Kind = "vol"
	KindAttach         Kind = "att"
	KindRoute          Kind = "rte"
	KindSecret         Kind = "sec"
	KindConnector      Kind = "con"
	KindRunner         Kind = "run"
	KindScript         Kind = "scr"
	KindBackupSource   Kind = "spt"  // blueprint.md: source_id: spt_01J...
	KindRecoveryPoint  Kind = "rp"   // blueprint.md: recovery_point_id: rp_01J...
	KindTask           Kind = "task" // blueprint.md: task_id: task_01J... (not the old 3-letter "tsk")
	KindOperation      Kind = "op"   // blueprint.md: operation_id: op_01J... — stable across a task's retries
	KindPlan           Kind = "plan" // blueprint.md: plan_id: plan_01J... — the ExecutionPlan
	KindAgent          Kind = "agt"
	KindNetwork        Kind = "net" // blueprint.md: "network zone maps directly to a Compose network"; x-gp-network id: net_01J...
	KindBackingService Kind = "bks"
	KindReleaseGroup   Kind = "rg"
	KindComponent      Kind = "component"
)

// New returns a fresh id: <kind>_<ULID>, using crypto/rand entropy and
// the current time — the normal, non-fixture path.
func New(kind Kind) string {
	monotonicMu.Lock()
	defer monotonicMu.Unlock()
	return string(kind) + "_" + ulid.MustNew(ulid.Timestamp(time.Now()), monotonicEntropy).String()
}

// NewAt is New with an explicit timestamp — for mock fixtures, which use
// static, reproducible ULIDs rather than generated ones (per the locked
// rule that fixtures never call time.Now()).
func NewAt(kind Kind, t time.Time, entropySeed int64) string {
	entropy := ulid.Monotonic(newSeededReader(entropySeed), 0)
	return string(kind) + "_" + ulid.MustNew(ulid.Timestamp(t), entropy).String()
}

// newSeededReader gives fixtures reproducible ULIDs across test runs.
func newSeededReader(seed int64) *deterministicReader {
	return &deterministicReader{state: uint64(seed)}
}

type deterministicReader struct{ state uint64 }

func (d *deterministicReader) Read(p []byte) (int, error) {
	for i := range p {
		d.state = d.state*6364136223846793005 + 1442695040888963407
		p[i] = byte(d.state >> 56)
	}
	return len(p), nil
}
