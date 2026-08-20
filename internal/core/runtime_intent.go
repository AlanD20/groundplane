package core

import "fmt"

// ServiceRuntimeIntent is Controller-owned operational state. It is stored
// separately from Service so Blueprint input cannot author or overwrite it.
type ServiceRuntimeIntent string

const (
	ServiceRuntimeIntentRunning ServiceRuntimeIntent = "running"
	ServiceRuntimeIntentStopped ServiceRuntimeIntent = "stopped"
	ServiceRuntimeIntentAbsent  ServiceRuntimeIntent = "absent"
)

// Validate rejects unset and unknown runtime intents before persistence or
// projection into the public API.
func (i ServiceRuntimeIntent) Validate() error {
	switch i {
	case ServiceRuntimeIntentRunning, ServiceRuntimeIntentStopped, ServiceRuntimeIntentAbsent:
		return nil
	default:
		return fmt.Errorf("invalid service runtime intent %q", i)
	}
}

// ServiceRuntime is the durable Controller-owned runtime projection for one
// desired Service. It deliberately has no YAML tags and is not a Blueprint
// document.
type ServiceRuntime struct {
	ServiceID     string               `json:"service_id"`
	RuntimeIntent ServiceRuntimeIntent `json:"runtime_intent"`
}

// Validate enforces a complete per-service runtime record.
func (r ServiceRuntime) Validate() error {
	if r.ServiceID == "" {
		return fmt.Errorf("service runtime: service_id is required")
	}
	if err := r.RuntimeIntent.Validate(); err != nil {
		return fmt.Errorf("service runtime: %w", err)
	}
	return nil
}
