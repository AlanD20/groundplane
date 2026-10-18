package componentwork

import (
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Capture is Controller settlement authority, not an executable plan. It is
// retained with the wire input so admission and recovery do not reread intent.
type Capture struct {
	Route           *RouteSettlement                         `json:"route,omitempty"`
	Removal         bool                                     `json:"removal"`
	Current         componentrecord.Record                   `json:"current"`
	CurrentRevision int64                                    `json:"current_revision"`
	Candidate       componentrecord.Record                   `json:"candidate"`
	RuntimeRevision int64                                    `json:"runtime_revision"`
	Predecessor     *RuntimePredecessor                      `json:"predecessor,omitempty"`
	Configuration   runtimeconfiguration.WorkCapture         `json:"configuration"`
	Secrets         *tasksecretpins.Binding                  `json:"secrets,omitempty"`
	Zones           []zonerecord.Record                      `json:"zones"`
	Proxy           *componentplanning.ProxyServicePreflight `json:"proxy,omitempty"`
}

func (capture Capture) matches(view workqueue.View, input *agentpb.ComponentWorkInput) error {
	task, item := view.Task.Task, view.Item.Item
	if input == nil || item.Operation != workexecution.Component || item.Placement.Executor != workexecution.Agent ||
		capture.Removal && !input.Remove ||
		capture.Configuration.Matches(task, item) != nil ||
		componentrecord.ValidateRecord(capture.Current) != nil || componentrecord.ValidateRecord(capture.Candidate) != nil ||
		capture.Current.Desired.ID != item.TargetID || capture.Candidate.Desired.ID != item.TargetID ||
		capture.Current.Desired.Owner != capture.Candidate.Desired.Owner ||
		capture.Current.Desired.OwnerID != task.Scope.EnvironmentID || capture.Candidate.Desired.OwnerID != task.Scope.EnvironmentID ||
		capture.CurrentRevision <= 0 || capture.CurrentRevision > capture.Configuration.ReadRevision ||
		capture.RuntimeRevision < 0 || capture.RuntimeRevision > capture.Configuration.ReadRevision ||
		capture.Configuration.ReadRevision > view.ReadRevision ||
		input.ComponentId != item.TargetID || input.EnvironmentId != task.Scope.EnvironmentID ||
		input.RenderGeneration != capture.Configuration.Generation || input.Remove == capture.Candidate.Desired.Enabled {
		return conflict("Component captured state differs from its work or executable input")
	}
	if len(input.Files) != 0 && !componentConfigurationMatches(input.Configuration, capture.Configuration.Current) ||
		(input.PriorConfiguration == nil) != (capture.Configuration.Prior == nil) ||
		capture.Configuration.Prior != nil && !componentConfigurationMatches(input.PriorConfiguration, *capture.Configuration.Prior) {
		return conflict("Component configuration references differ from captured sources")
	}
	if err := capture.matchPredecessor(input); err != nil {
		return err
	}
	if capture.Route != nil {
		if capture.Removal || capture.Proxy != nil || capture.Route.matches(view, input) != nil {
			return conflict("Route work cannot remove or replace its router Component")
		}
	} else if task.Action != workexecution.ActionApply {
		return conflict("Component work requires a Blueprint or captured Route action")
	}
	destinations := make([]string, len(input.Files))
	for index, file := range input.Files {
		if file == nil {
			return conflict("Component captured file descriptor is missing")
		}
		destinations[index] = file.Destination
	}
	if !slices.Equal(destinations, capture.Configuration.Destinations) {
		return conflict("Component configuration destinations differ from captured files")
	}
	if capture.Secrets != nil && (capture.Secrets.Validate() != nil || capture.Secrets.OperationID != input.OperationId ||
		capture.Secrets.TaskID != task.ID || capture.Secrets.WorkItemID != item.ID) {
		return conflict("Component Secret retention belongs to another operation")
	}
	previous := ""
	proxyChanged := capture.Current.Desired.Kind != capture.Candidate.Desired.Kind && capture.Candidate.Desired.Kind.IsEnvironmentProxy()
	if proxyChanged != (capture.Proxy != nil) || capture.Proxy != nil &&
		componentplanning.ValidateAgentProxyCapture(view, *capture.Proxy) != nil {
		return conflict("Component proxy change is missing its captured Service dependencies")
	}
	for _, zone := range capture.Zones {
		if zonerecord.ValidateRecord(zone) != nil || zone.EnvironmentID != task.Scope.EnvironmentID || zone.Desired.ID <= previous {
			return conflict("Component captured Zones are invalid or repeated")
		}
		previous = zone.Desired.ID
	}
	return nil
}

func componentConfigurationMatches(wire *agentpb.WorkConfigurationReference, reference runtimeconfiguration.Reference) bool {
	return wire != nil && wire.ConfigurationId == reference.ID && wire.EnvironmentId == reference.EnvironmentID &&
		wire.Generation == reference.Generation && hex.EncodeToString(wire.Sha256) == reference.SHA256
}
