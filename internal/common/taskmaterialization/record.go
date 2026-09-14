// Package taskmaterialization defines the closed, non-secret file source record
// shared by Task planning and retained runtime configuration. It owns no I/O.
package taskmaterialization

type SourceKind string

const (
	SourceBlueprintFile        SourceKind = "blueprint_file"
	SourceComponentFile        SourceKind = "component_file"
	SourceEntryValue           SourceKind = "entry_value"
	SourceGeneratedEnvironment SourceKind = "generated_environment"
	SourceRemoval              SourceKind = "removal"
)

type EntryValueStorage string

const (
	EntryValueStoragePlain  EntryValueStorage = "plain"
	EntryValueStorageSecret EntryValueStorage = "secret"
)

type OutputKind string

const (
	OutputGeneratedEnvironment OutputKind = "generated_env"
	OutputPlainFile            OutputKind = "plain_file"
	OutputSecretFile           OutputKind = "secret_file"
	OutputRemoveGeneratedEnv   OutputKind = "remove_generated_env"
	OutputRemovePlainFile      OutputKind = "remove_plain_file"
	OutputRemoveSecretFile     OutputKind = "remove_secret_file"
)

// Record is the durable Controller-only source ledger for
// one metadata-only Agent materialization step. It never contains value bytes.
type Record struct {
	StepID            string     `json:"step_id"`
	MaterializationID string     `json:"materialization_id"`
	EnvironmentID     string     `json:"environment_id"`
	Destination       string     `json:"destination"`
	ServiceID         string     `json:"service_id,omitempty"`
	ServiceName       string     `json:"service_name,omitempty"`
	OutputKind        OutputKind `json:"output_kind"`
	UID               uint32     `json:"uid"`
	GID               uint32     `json:"gid"`
	Mode              uint32     `json:"mode"`
	Length            uint64     `json:"length"`
	SHA256            string     `json:"sha256"`
	Source            Source     `json:"source"`
}

// Source is a closed tagged union. Exactly one pointer must
// match Kind so the durable JSON cannot acquire fallback resolution behavior.
type Source struct {
	Kind                 SourceKind                          `json:"kind"`
	BlueprintFile        *BlueprintFileValueReference        `json:"blueprint_file,omitempty"`
	ComponentFile        *ComponentFileValueReference        `json:"component_file,omitempty"`
	EntryValue           *EntryValueReference                `json:"entry_value,omitempty"`
	GeneratedEnvironment *GeneratedEnvironmentValueReference `json:"generated_environment,omitempty"`
}

type BlueprintFileValueReference struct {
	RevisionID string `json:"revision_id"`
	Path       string `json:"path"`
}

// ComponentFileValueReference names one deterministic generated file.
// RevisionID pins the immutable Blueprint input while ComponentID prevents a
// path collision from authorizing content rendered for another Component.
// RouteTaskID selects that Task's immutable candidate projection;
// empty selects the active projection used by ordinary reconciliation.
type ComponentFileValueReference struct {
	RevisionID  string `json:"revision_id"`
	ComponentID string `json:"component_id"`
	Path        string `json:"path"`
	RouteTaskID string `json:"route_task_id,omitempty"`
}

// EntryValueReference names one immutable value generation. Storage is
// explicit so plain desired values and encrypted subordinate values cannot be
// resolved through the wrong repository.
type EntryValueReference struct {
	EntryID           string            `json:"entry_id"`
	ValueGenerationID string            `json:"value_generation_id"`
	Storage           EntryValueStorage `json:"storage"`
}

type GeneratedEnvironmentValueReference struct {
	FormatVersion uint32                               `json:"format_version"`
	Values        []GeneratedEnvironmentEntryReference `json:"values"`
}

type GeneratedEnvironmentEntryReference struct {
	Name   string                `json:"name"`
	Value  EntryValueReference   `json:"value"`
	Secret *SecretValueReference `json:"secret,omitempty"`
}

// SecretValueReference pins one reusable Secret without copying value
// bytes into desired state or the durable Task journal.
type SecretValueReference struct {
	SecretID         string `json:"secret_id"`
	Revision         int64  `json:"revision"`
	CiphertextSHA256 string `json:"ciphertext_sha256"`
}
