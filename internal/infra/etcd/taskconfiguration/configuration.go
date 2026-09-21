package taskconfiguration

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
)

// TaskConfiguration binds a candidate source set to its exact acknowledged
// predecessor. A nil Prior with revision zero records acknowledged absence.
type TaskConfiguration struct {
	Current           runtimeconfiguration.Reference  `json:"current,omitempty"`
	Prior             *runtimeconfiguration.Reference `json:"prior"`
	PriorRevision     int64                           `json:"prior_revision"`
	SecretPins        *TaskSecretPinSet               `json:"secret_pins,omitempty"`
	BackingHookInputs *TaskBackingHookInputSet        `json:"backing_hook_inputs,omitempty"`
}

func CloneTaskConfiguration(configuration *TaskConfiguration) *TaskConfiguration {
	if configuration == nil {
		return nil
	}
	cloned := *configuration
	if configuration.SecretPins != nil {
		pins := *configuration.SecretPins
		cloned.SecretPins = &pins
	}
	if configuration.BackingHookInputs != nil {
		inputs := *configuration.BackingHookInputs
		inputs.SecretSources = append([]tasksecretpinrecord.Record(nil), inputs.SecretSources...)
		cloned.BackingHookInputs = &inputs
	}
	if configuration.Prior != nil {
		prior := *configuration.Prior
		cloned.Prior = &prior
	}
	return &cloned
}
