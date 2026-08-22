// Package adapters is the ONE registry every backing-service kind
// registers into. Adapters are declarative, typed knowledge — a fixed
// shape to fill in, in one package — never a switch in core. Adding a
// kind = implement Adapter + export a Register() function, called
// explicitly from internal/app.NewController and NewAgent (never an init() — see
// standards.md's banned-patterns list, section 11). See
// architecture.md, "The adapter seam (the contract shape)", and mvp.md,
// "The backing-service contract".
package adapters

import (
	"bytes"
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

// Step is one locally compiled operation in a provision/detach/backup/restore
// procedure. Secret-bearing input stays mutable and never enters argv.
type Step struct {
	Op       StepOp
	Database string
	Program  string
	Args     []string
	Stdin    []byte
}

// ProvisionParams contains the Controller-resolved identity transported by a
// typed Agent procedure. The Agent passes it to the same compiled adapter.
type ProvisionParams struct {
	Database string // <service-name>_<first-6-of-attach-id>
	Role     string // same as Database in the MVP (one role per attach)
	Password []byte // Controller-generated, URL-safe; caller-owned and mutable
	GrantOn  string // set only for grant steps: the OTHER attach's database
}

// BackupStrategy names the dump/restore commands the adapter's steps
// wrap. Advisory metadata; the actual Step sequence is what executes.
type BackupStrategy struct {
	Dump    string
	Restore string
}

type FactField string

const (
	FactURL      FactField = "URL"
	FactHost     FactField = "HOST"
	FactPort     FactField = "PORT"
	FactDatabase FactField = "DATABASE"
	FactRole     FactField = "ROLE"
	FactPassword FactField = "PASSWORD"
)

// FactDefinition is one adapter-declared output key. Secret marks values that
// must never enter listable Attach metadata.
type FactDefinition struct {
	Field  FactField
	Secret bool
}

// FactParams is the complete resolved identity used to render one own or
// granted Attach fact set. Password remains caller-owned mutable bytes.
type FactParams struct {
	Host     string
	Port     string
	Database string
	Role     string
	Password []byte
}

// Fact owns its Value. Callers must ClearFacts after encryption or consumption.
type Fact struct {
	Key    string
	Value  []byte
	Secret bool
}

// Adapter is the contract every backing-service kind implements.
type Adapter interface {
	Key() string          // e.g. "postgres:16" — looked up by core.Service.Adapter
	Label() string        // display only
	DefaultImage() string // e.g. "postgres:16-alpine"
	FactsPrefix() string  // e.g. "pg16_" — empty for Manual()
	URLScheme() string    // e.g. "pgsql://" — empty for Manual()
	Port() string         // e.g. "5432" — empty for Manual()
	FactSchema() []FactDefinition
	Manual() bool // true => network-only attach, no facts, no backups (see mvp.md, "The manual adapter")
	SupportsGrants() bool

	ProvisionSteps(p ProvisionParams) []Step
	GrantSteps(p ProvisionParams) []Step // p.GrantOn set — access to another attach's database
	RevokeSteps(p ProvisionParams) []Step
	DetachSteps(p ProvisionParams) []Step

	BackupStrategy() BackupStrategy
}

// ClearSteps clears every secret-bearing input buffer compiled by an adapter.
func ClearSteps(steps []Step) {
	for index := range steps {
		clear(steps[index].Stdin)
		steps[index].Stdin = nil
		steps[index].Args = nil
	}
}

// BuildFacts renders one adapter-declared fact set without converting the
// generated password or URL to immutable strings.
func BuildFacts(adapter Adapter, params FactParams) ([]Fact, error) {
	if adapter == nil {
		return nil, fmt.Errorf("adapter is required")
	}
	schema := adapter.FactSchema()
	if adapter.Manual() {
		if len(schema) != 0 || adapter.FactsPrefix() != "" || adapter.URLScheme() != "" {
			return nil, fmt.Errorf("manual adapter must not declare facts")
		}
		return []Fact{}, nil
	}
	if len(schema) == 0 || !validFactPrefix(adapter.FactsPrefix()) || len(params.Password) == 0 ||
		!validFactAtom(params.Host) || !validFactAtom(params.Port) || !validFactAtom(params.Role) ||
		!validFactBytes(params.Password) {
		return nil, fmt.Errorf("adapter fact input is invalid")
	}

	facts := make([]Fact, 0, len(schema))
	seen := make(map[FactField]struct{}, len(schema))
	failed := true
	defer func() {
		if failed {
			ClearFacts(facts)
		}
	}()
	for _, definition := range schema {
		if _, duplicate := seen[definition.Field]; duplicate {
			return nil, fmt.Errorf("adapter fact schema contains a duplicate field")
		}
		seen[definition.Field] = struct{}{}
		value, err := renderFactValue(adapter.URLScheme(), definition.Field, params)
		if err != nil {
			return nil, err
		}
		facts = append(facts, Fact{
			Key: adapter.FactsPrefix() + string(definition.Field), Value: value, Secret: definition.Secret,
		})
	}
	failed = false
	return facts, nil
}

// ClearFacts clears and releases every fact value buffer.
func ClearFacts(facts []Fact) {
	for index := range facts {
		clear(facts[index].Value)
		facts[index].Value = nil
	}
}

func renderFactValue(scheme string, field FactField, params FactParams) ([]byte, error) {
	switch field {
	case FactHost:
		return []byte(params.Host), nil
	case FactPort:
		return []byte(params.Port), nil
	case FactDatabase:
		if !validFactAtom(params.Database) {
			return nil, fmt.Errorf("adapter database fact is invalid")
		}
		return []byte(params.Database), nil
	case FactRole:
		return []byte(params.Role), nil
	case FactPassword:
		return append([]byte(nil), params.Password...), nil
	case FactURL:
		return renderFactURL(scheme, params)
	default:
		return nil, fmt.Errorf("adapter fact schema contains an unknown field")
	}
}

func renderFactURL(scheme string, params FactParams) ([]byte, error) {
	if scheme != "pgsql://" && scheme != "redis://" {
		return nil, fmt.Errorf("adapter fact URL scheme is invalid")
	}
	var output bytes.Buffer
	output.Grow(len(scheme) + len(params.Role) + len(params.Password) + len(params.Host) + len(params.Port) +
		len(params.Database) + 4)
	output.WriteString(scheme)
	output.WriteString(params.Role)
	output.WriteByte(':')
	output.Write(params.Password)
	output.WriteByte('@')
	output.WriteString(params.Host)
	output.WriteByte(':')
	output.WriteString(params.Port)
	if scheme == "pgsql://" {
		if !validFactAtom(params.Database) {
			return nil, fmt.Errorf("adapter database fact is invalid")
		}
		output.WriteByte('/')
		output.WriteString(params.Database)
	}
	return output.Bytes(), nil
}

func validFactPrefix(value string) bool {
	if len(value) < 2 || value[len(value)-1] != '_' {
		return false
	}
	for index := range len(value) - 1 {
		character := value[index]
		if character != '_' && (character < 'A' || character > 'Z') &&
			(character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func validFactAtom(value string) bool {
	if value == "" {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if character != '_' && character != '-' && character != '.' &&
			(character < 'A' || character > 'Z') && (character < 'a' || character > 'z') &&
			(character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func validFactBytes(value []byte) bool {
	if len(value) == 0 {
		return false
	}
	for _, character := range value {
		if character != '_' && character != '-' && character != '.' &&
			(character < 'A' || character > 'Z') && (character < 'a' || character > 'z') &&
			(character < '0' || character > '9') {
			return false
		}
	}
	return true
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
