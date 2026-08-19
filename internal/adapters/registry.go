// Package adapters is the ONE registry every backing-service kind
// registers into. Adapters are declarative, typed knowledge — a fixed
// shape to fill in, in one package — never a switch in core. Adding a
// kind = implement Adapter + export a Register() function, called
// explicitly from internal/app.NewController (never an init() — see
// standards.md's banned-patterns list, section 11). See
// architecture.md, "The adapter seam (the contract shape)", and mvp.md,
// "The backing-service contract".
package adapters

import (
	"fmt"
	"sort"
)

// StepOp is a member of the known step catalog the Agent executes. A
// step kind that doesn't exist here is an ADDITIVE CORE EXTENSION — it
// must be added to this catalog and the contract shape, never worked
// around by an adapter-side bypass (the locked "no arbitrary command
// execution" rule). See architecture.md, "The guardrail" — the full
// catalog spans both compose/orchestration steps (issued by the
// renderer's ExecutionPlan, see internal/controller/plan.go) and the
// adapter-issued steps below, which is the subset this package's
// Adapter interface composes.
type StepOp string

const (
	// Compose/orchestration steps — issued by the ExecutionPlan, not by
	// an adapter; listed here because they share the one enforced step
	// catalog with adapter-issued steps.
	StepComposeUp        StepOp = "compose_up"
	StepComposeDown      StepOp = "compose_down"
	StepWaitHealthy      StepOp = "wait_healthy"
	StepSwitchAlias      StepOp = "switch_alias"
	StepSwitchRoute      StepOp = "switch_route"
	StepWriteFile        StepOp = "write_file"
	StepReload           StepOp = "reload"
	StepJoinNetwork      StepOp = "join_network"
	StepProvisionNetwork StepOp = "provision_network"
	// StepRunScript is the explicit, operator-authored automation
	// exception, with its own task boundary — never a generic exec
	// escape hatch.
	StepRunScript StepOp = "run_script"

	// Adapter-issued steps — what internal/adapters kinds compose their
	// Provision/Grant/Detach/Backup sequences from.
	StepExec    StepOp = "exec"
	StepSQL     StepOp = "sql"
	StepDump    StepOp = "dump"
	StepRestore StepOp = "restore"
	StepEncrypt StepOp = "encrypt"
	StepUpload  StepOp = "upload"
	StepVerify  StepOp = "verify"
	StepPrune   StepOp = "prune"
	StepAck     StepOp = "ack"
)

// Step is one typed, parameterized operation in a provision/detach/
// backup/restore procedure. Placeholders (<db>, <role>, <generated>) are
// filled by the Controller before the Agent executes the concrete
// procedure.
type Step struct {
	Op     StepOp            `json:"op"`
	Params map[string]string `json:"params,omitempty"`
}

// ProvisionParams is what the Controller fills in before handing a
// Step slice to the Agent: <db>, <role>, <generated> resolved to the
// attach's actual names.
type ProvisionParams struct {
	Database string // <service-name>_<first-6-of-attach-id>
	Role     string // same as Database in the MVP (one role per attach)
	Password string // Controller-generated, URL-safe
	GrantOn  string // set only for grant steps: the OTHER attach's database
}

// BackupStrategy names the dump/restore commands the adapter's steps
// wrap. Advisory metadata; the actual Step sequence is what executes.
type BackupStrategy struct {
	Dump    string
	Restore string
}

// Adapter is the contract every backing-service kind implements.
type Adapter interface {
	Key() string          // e.g. "postgres:16" — looked up by core.Service.Adapter
	Label() string        // display only
	DefaultImage() string // e.g. "postgres:16-alpine"
	FactsPrefix() string  // e.g. "pg16_" — empty for Manual()
	URLScheme() string    // e.g. "pgsql://" — empty for Manual()
	Manual() bool         // true => network-only attach, no facts, no backups (see mvp.md, "The manual adapter")

	ProvisionSteps(p ProvisionParams) []Step
	GrantSteps(p ProvisionParams) []Step // p.GrantOn set — access to another attach's database
	DetachSteps(p ProvisionParams) []Step

	BackupStrategy() BackupStrategy
}

var registry = map[string]Adapter{}

// Register adds a kind to the registry. Call each adapter package's explicit
// Register function from internal/app. This is the ONLY entry point: the
// Console, CLI, and API learn kinds from this map; core never switches on kind.
func Register(a Adapter) {
	if _, exists := registry[a.Key()]; exists {
		panic(fmt.Sprintf("adapters: duplicate registration for key %q", a.Key()))
	}
	registry[a.Key()] = a
}

// Get looks up an adapter by key (core.Service.Adapter).
func Get(key string) (Adapter, bool) {
	a, ok := registry[key]
	return a, ok
}

// All returns every registered adapter, for the Console's "New backing
// service" adapter picker and the CLI's `backing-service create` help.
func All() []Adapter {
	out := make([]Adapter, 0, len(registry))
	for _, a := range registry {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Key() < out[j].Key()
	})
	return out
}
