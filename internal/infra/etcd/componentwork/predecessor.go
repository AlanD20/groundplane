package componentwork

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// RuntimePredecessor retains applied metadata independently of current desired
// state. Its artifact is already retained once in ComponentWorkInput.
type RuntimePredecessor struct {
	Component        componentrecord.Record                   `json:"component"`
	RevisionID       string                                   `json:"revision_id"`
	RenderGeneration uint64                                   `json:"render_generation"`
	Zones            []zonerecord.Record                      `json:"zones,omitempty"`
	Execution        blueprintunits.AppliedExecution          `json:"execution"`
	Proxy            *componentplanning.ProxyServicePreflight `json:"proxy,omitempty"`
}

func CaptureRuntimePredecessor(runtime componentplanning.Runtime) (RuntimePredecessor, error) {
	if err := componentplanning.ValidateRuntime(runtime); err != nil {
		return RuntimePredecessor{}, err
	}
	return RuntimePredecessor{Component: runtime.Component, RevisionID: runtime.RevisionID,
		RenderGeneration: runtime.RenderGeneration, Zones: runtime.Zones, Execution: runtime.Execution, Proxy: runtime.ProxyPreflight}, nil
}

func (captured RuntimePredecessor) Runtime(input *agentpb.ComponentWorkInput) (componentplanning.Runtime, error) {
	if input == nil || input.Predecessor == nil || captured.Component.Desired.ID != input.ComponentId {
		return componentplanning.Runtime{}, conflict("Component predecessor metadata has no matching captured runtime")
	}
	var artifact []byte
	if input.Predecessor.Runtime != nil {
		var err error
		artifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(input.Predecessor.Runtime)
		if err != nil {
			return componentplanning.Runtime{}, err
		}
	}
	runtime := componentplanning.Runtime{EnvironmentID: input.EnvironmentId, Component: captured.Component,
		RevisionID: captured.RevisionID, RenderGeneration: captured.RenderGeneration, Zones: captured.Zones,
		Execution: captured.Execution, ProxyPreflight: captured.Proxy, Artifact: artifact}
	if input.Predecessor.Absent == captured.Component.Desired.Enabled || componentplanning.ValidateRuntime(runtime) != nil {
		clear(artifact)
		return componentplanning.Runtime{}, conflict("Component predecessor metadata differs from captured runtime")
	}
	return runtime, nil
}

func (capture Capture) matchPredecessor(input *agentpb.ComponentWorkInput) error {
	if input.Predecessor == nil || (capture.Predecessor == nil) != (capture.RuntimeRevision == 0) {
		return conflict("Component predecessor metadata is incomplete")
	}
	if capture.Predecessor == nil {
		if !input.Predecessor.Absent {
			return conflict("Component predecessor artifact has no applied metadata")
		}
		return nil
	}
	runtime, err := capture.Predecessor.Runtime(input)
	clear(runtime.Artifact)
	return err
}
