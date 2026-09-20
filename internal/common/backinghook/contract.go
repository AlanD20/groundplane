// Package backinghook defines the operator-hook boundary, shared by the
// Controller and Agent. It does not execute commands or own persistence.
package backinghook

type Event string

const (
	Attach     Event = "attach"
	Detach     Event = "detach"
	BeforeStop Event = "before-stop"
	AfterStart Event = "after-start"
)

// Definition is authored configuration. Input values are resolved by the
// Controller, not interpolated into the command text.
type Definition struct {
	Command        []string `json:"command" yaml:"command"`
	TimeoutSeconds uint32   `json:"timeout_seconds" yaml:"timeout_seconds"`
}

type FactDefinition struct {
	Key    string `json:"key" yaml:"key"`
	Secret bool   `json:"secret" yaml:"secret"`
}

type InputGeneration string

const GeneratePassword InputGeneration = "password"

// InputDefinition is one explicitly authored input source. Value is a pointer
// so an intentionally empty literal remains distinct from an omitted source.
type InputDefinition struct {
	Key       string          `json:"key" yaml:"key"`
	Value     *string         `json:"value,omitempty" yaml:"value,omitempty"`
	SecretRef string          `json:"secret_ref,omitempty" yaml:"secret_ref,omitempty"`
	Generate  InputGeneration `json:"generate,omitempty" yaml:"generate,omitempty"`
}

// Configuration is optional on a custom backing Service. Missing hooks are
// no-ops; facts require an attach hook that produces their values.
type Configuration struct {
	Attach     *Definition       `json:"attach,omitempty" yaml:"attach,omitempty"`
	Detach     *Definition       `json:"detach,omitempty" yaml:"detach,omitempty"`
	BeforeStop *Definition       `json:"before_stop,omitempty" yaml:"before_stop,omitempty"`
	AfterStart *Definition       `json:"after_start,omitempty" yaml:"after_start,omitempty"`
	Facts      []FactDefinition  `json:"facts,omitempty" yaml:"facts,omitempty"`
	Inputs     []InputDefinition `json:"inputs,omitempty" yaml:"inputs,omitempty"`
}

type Value struct {
	Key   string
	Value []byte
}

type Context struct {
	Event            Event
	BackingServiceID string
	AttachID         string
	TenantID         string
	ProjectID        string
	EnvironmentID    string
	ServiceID        string
}

// Input is the resolved hook input. Values and prior Facts are private,
// caller-owned buffers; they never belong in ordinary Task event payloads.
type Input struct {
	Context Context
	Values  []Value
	Facts   []Value
}

type Fact struct {
	Key    string
	Value  []byte
	Secret bool
}

type Output struct {
	Facts []Fact
}

func (output *Output) Clear() {
	for index := range output.Facts {
		clear(output.Facts[index].Value)
	}
	output.Facts = nil
}
