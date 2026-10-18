package agent

import (
	"runtime"

	"github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (c *Client) sendReady(stream agentStream) error {
	source := version.Provenance()
	if err := source.Validate(); err != nil {
		return err
	}
	var provenance *agentpb.BuildProvenance
	if source.Known() {
		provenance = &agentpb.BuildProvenance{Kind: source.Kind, Commit: source.Commit}
	}
	clean, err := c.terminalDeliveryClean()
	if err != nil {
		return err
	}
	clean = clean && c.staging != nil && c.staging.ready && !c.staging.reinspect.Load()
	capacity := c.availableWorkCapacity()
	if !clean {
		capacity = 0
	}
	variant := ""
	if runtime.GOARCH == "arm64" {
		variant = "v8"
	}
	return stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Ready{
		Ready: &agentpb.Ready{
			Capacity: int32(capacity), Version: version.Value,
			Source:                provenance,
			TerminalDeliveryClean: &clean,
			OperatingSystem:       runtime.GOOS, Architecture: runtime.GOARCH,
			ArchitectureVariant: variant,
		},
	}})
}

func (c *Client) availableWorkCapacity() int {
	capacity := c.pool.Capacity()
	if c.executionWorkers != nil {
		capacity -= c.executionWorkers.limit - c.executionWorkers.capacity()
	}
	return max(0, capacity)
}
