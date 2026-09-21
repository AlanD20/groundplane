package environmentchanges

import (
	"bytes"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/core"
)

func SameServiceRemovalProjection(left, right projectionrecord.EnvironmentComposeProjection) bool {
	return left.EnvironmentID == right.EnvironmentID && left.RevisionID == right.RevisionID &&
		left.RenderGeneration == right.RenderGeneration && SameServiceRemovalBytes(left.ComposeArtifact, right.ComposeArtifact) &&
		SameServiceRemovalBytes(left.NormalizedCompose, right.NormalizedCompose) &&
		SameServiceRemovalBlueprintFiles(left.RuntimeFiles, right.RuntimeFiles) &&
		SameServiceRemovalServiceExtensions(left.ServiceExtensions, right.ServiceExtensions) &&
		SameServiceRemovalDesiredZones(left.DesiredZones, right.DesiredZones) &&
		SameServiceRemovalDesiredServices(left.DesiredServices, right.DesiredServices) &&
		SameServiceRemovalDesiredRoutes(left.DesiredRoutes, right.DesiredRoutes) &&
		SameServiceRemovalComparableSlices(left.Volumes, right.Volumes) &&
		SameServiceRemovalComparableSlices(left.VolumeMounts, right.VolumeMounts) &&
		SameServiceRemovalComponents(left.Components, right.Components) &&
		SameServiceRemovalEntries(left.Entries, right.Entries) &&
		SameServiceRemovalDependencyPlans(left.ServiceDependencyPlans, right.ServiceDependencyPlans)
}

func SameServiceRemovalDesiredZones(left, right []projectionrecord.EnvironmentZoneProjection) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].EnvironmentID != right[index].EnvironmentID || left[index].Desired != right[index].Desired {
			return false
		}
	}
	return true
}

func SameServiceRemovalDesiredServices(left, right []servicerecord.EnvironmentServiceProjection) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].EnvironmentID != right[index].EnvironmentID ||
			left[index].BackingNetworkID != right[index].BackingNetworkID ||
			!SameServiceRemovalDesired(left[index].Desired, right[index].Desired) {
			return false
		}
	}
	return true
}

func SameServiceRemovalDesiredRoutes(left, right []projectionrecord.EnvironmentRouteProjection) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].EnvironmentID != right[index].EnvironmentID ||
			left[index].Desired != right[index].Desired ||
			left[index].DesiredGeneration != right[index].DesiredGeneration {
			return false
		}
	}
	return true
}

func SameServiceRemovalDesired(left, right core.Service) bool {
	if left.ID != right.ID || left.Name != right.Name || left.Image != right.Image ||
		left.Strategy != right.Strategy || left.OnFailure != right.OnFailure ||
		left.Healthcheck != right.Healthcheck || left.Resources != right.Resources ||
		left.Restart != right.Restart || left.Logging != right.Logging || left.Replicas != right.Replicas ||
		left.Adapter != right.Adapter || left.FactsPrefix != right.FactsPrefix || left.Label != right.Label ||
		!backinghook.EqualConfiguration(left.Hooks, right.Hooks) ||
		!SameServiceRemovalComparableSlices(left.Zones, right.Zones) ||
		!SameServiceRemovalComparableSlices(left.Command, right.Command) ||
		!SameServiceRemovalComparableSlices(left.Mounts, right.Mounts) ||
		!SameServiceRemovalComparableSlices(left.Expose, right.Expose) ||
		!sameServiceRemovalEnvEntries(left.Environment, right.Environment) ||
		!sameServiceRemovalStringSlices(left.Aliases, right.Aliases) ||
		!sameServiceRemovalDependencies(left.DependsOn, right.DependsOn) {
		return false
	}
	return true
}

func sameServiceRemovalEnvEntries(left, right []core.EnvEntry) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if !sameServiceRemovalEnvEntry(left[index], right[index]) {
			return false
		}
	}
	return true
}

func sameServiceRemovalStringSlices(left, right map[string][]string) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for key, leftValues := range left {
		rightValues, exists := right[key]
		if !exists || !SameServiceRemovalComparableSlices(leftValues, rightValues) {
			return false
		}
	}
	return true
}

func sameServiceRemovalDependencies(left, right map[string]core.ServiceDependency) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for name, leftDependency := range left {
		rightDependency, exists := right[name]
		if !exists || leftDependency.Condition != rightDependency.Condition ||
			!SameServiceRemovalComparableSlices(leftDependency.Phases, rightDependency.Phases) {
			return false
		}
	}
	return true
}

func SameServiceRemovalBlueprintFiles(left, right []core.BlueprintFile) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Path != right[index].Path ||
			!SameServiceRemovalBytes(left[index].Content, right[index].Content) {
			return false
		}
	}
	return true
}

func SameServiceRemovalServiceExtensions(
	left,
	right map[string]core.ServiceExtensionSpec,
) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for name, leftExtension := range left {
		rightExtension, exists := right[name]
		if !exists || !sameServiceRemovalServiceExtension(leftExtension, rightExtension) {
			return false
		}
	}
	return true
}

func sameServiceRemovalServiceExtension(left, right core.ServiceExtensionSpec) bool {
	if left.Release == nil || right.Release == nil {
		if left.Release != nil || right.Release != nil {
			return false
		}
	} else if *left.Release != *right.Release {
		return false
	}
	if (left.DependsOn == nil) != (right.DependsOn == nil) || len(left.DependsOn) != len(right.DependsOn) {
		return false
	}
	for name, leftDependency := range left.DependsOn {
		rightDependency, exists := right.DependsOn[name]
		if !exists || leftDependency.Condition != rightDependency.Condition ||
			!SameServiceRemovalComparableSlices(leftDependency.Phases, rightDependency.Phases) {
			return false
		}
	}
	return true
}

func SameServiceRemovalBytes(left, right []byte) bool {
	return (left == nil) == (right == nil) && bytes.Equal(left, right)
}

func SameServiceRemovalComparableSlices[E comparable](left, right []E) bool {
	return (left == nil) == (right == nil) && slices.Equal(left, right)
}

func SameServiceRemovalComponents(left, right []componentrecord.Record) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if !sameServiceRemovalComponentRecord(left[index], right[index]) {
			return false
		}
	}
	return true
}

func SameServiceRemovalEntries(left, right []entryrecord.Record) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if !sameServiceRemovalEntryRecord(left[index], right[index]) {
			return false
		}
	}
	return true
}

func SameServiceRemovalDependencyPlans(left, right core.ServiceDependencyPlans) bool {
	return sameServiceRemovalDependencyPlan(left.DeployDependencyPlan, right.DeployDependencyPlan) &&
		sameServiceRemovalDependencyPlan(left.RollbackDependencyPlan, right.RollbackDependencyPlan)
}

func sameServiceRemovalDependencyPlan(left, right core.ServiceDependencyPhasePlan) bool {
	return left.Phase == right.Phase &&
		SameServiceRemovalComparableSlices(left.OrderedServices, right.OrderedServices) &&
		SameServiceRemovalComparableSlices(left.Edges, right.Edges)
}

func sameServiceRemovalComponentRecord(left, right componentrecord.Record) bool {
	return left.Desired.ID == right.Desired.ID && left.Desired.Owner == right.Desired.Owner &&
		left.Desired.OwnerID == right.Desired.OwnerID && left.Desired.Kind == right.Desired.Kind &&
		left.Desired.Enabled == right.Desired.Enabled &&
		sameServiceRemovalComponentConfig(left.Desired.Config, right.Desired.Config) &&
		SameServiceRemovalComparableSlices(left.Runtime.GeneratedServices, right.Runtime.GeneratedServices) &&
		left.Runtime.PinnedIPv4 == right.Runtime.PinnedIPv4 && left.Runtime.Healthy == right.Runtime.Healthy
}

func sameServiceRemovalComponentConfig(left, right core.ComponentConfig) bool {
	if (left.Caddy == nil) != (right.Caddy == nil) ||
		(left.CloudflareTunnel == nil) != (right.CloudflareTunnel == nil) ||
		(left.CoreDNS == nil) != (right.CoreDNS == nil) {
		return false
	}
	if left.Caddy != nil &&
		(left.Caddy.CaddyfileTemplate != right.Caddy.CaddyfileTemplate || left.Caddy.Alias != right.Caddy.Alias ||
			!slices.Equal(left.Caddy.ZoneIDs, right.Caddy.ZoneIDs)) {
		return false
	}
	if left.CloudflareTunnel != nil && (left.CloudflareTunnel.SecretID != right.CloudflareTunnel.SecretID ||
		!slices.Equal(left.CloudflareTunnel.ZoneIDs, right.CloudflareTunnel.ZoneIDs)) {
		return false
	}
	if left.CoreDNS == nil {
		return true
	}
	if left.CoreDNS.UpstreamAuto != right.CoreDNS.UpstreamAuto ||
		left.CoreDNS.TailnetDelegation != right.CoreDNS.TailnetDelegation ||
		!SameServiceRemovalComparableSlices(left.CoreDNS.UpstreamResolvers, right.CoreDNS.UpstreamResolvers) ||
		(left.CoreDNS.Forwarders == nil) != (right.CoreDNS.Forwarders == nil) ||
		len(left.CoreDNS.Forwarders) != len(right.CoreDNS.Forwarders) {
		return false
	}
	for index := range left.CoreDNS.Forwarders {
		leftForwarder := left.CoreDNS.Forwarders[index]
		rightForwarder := right.CoreDNS.Forwarders[index]
		if leftForwarder.Domain != rightForwarder.Domain ||
			!SameServiceRemovalComparableSlices(leftForwarder.Resolvers, rightForwarder.Resolvers) {
			return false
		}
	}
	return true
}

func sameServiceRemovalEntryRecord(left, right entryrecord.Record) bool {
	return left.EnvironmentID == right.EnvironmentID && left.BlueprintKey == right.BlueprintKey &&
		left.CurrentValueGenerationID == right.CurrentValueGenerationID &&
		sameServiceRemovalEnvEntry(left.Entry, right.Entry)
}

func sameServiceRemovalEnvEntry(left, right core.EnvEntry) bool {
	return left.ID == right.ID && left.Kind == right.Kind && left.Key == right.Key && left.Path == right.Path &&
		sameServiceRemovalOptionalUint32(left.UID, right.UID) &&
		sameServiceRemovalOptionalUint32(left.GID, right.GID) && left.Secret == right.Secret &&
		sameServiceRemovalEntrySource(left.Source, right.Source) &&
		SameServiceRemovalComparableSlices(left.Exposure, right.Exposure)
}

func sameServiceRemovalOptionalUint32(left, right *uint32) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func sameServiceRemovalEntrySource(left, right core.EntrySource) bool {
	if left.Kind != right.Kind || left.Literal != right.Literal || left.SecretRef != right.SecretRef {
		return false
	}
	if left.Fact == nil || right.Fact == nil {
		return left.Fact == nil && right.Fact == nil
	}
	return *left.Fact == *right.Fact
}
