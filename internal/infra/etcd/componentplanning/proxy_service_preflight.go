package componentplanning

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/core/serviceports"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ProxyServicePreflight pins complete authored membership, including pending
// removals, before the router has any effects. Source hashes are comparisons,
// not executable runtime authority: each Service child still seals its own
// native predecessor and recovery recipe.
type ProxyServicePreflight struct {
	EnvironmentID  string                        `json:"environment_id"`
	ParentTaskID   string                        `json:"parent_task_id"`
	ReadRevision   int64                         `json:"read_revision"`
	Implementation serviceproxy.Implementation   `json:"implementation"`
	Services       []ProxyServiceSource          `json:"services"`
	CandidateInput *ProxyCandidateInputReference `json:"candidate_input,omitempty"`
	SourceHold     *ProxySourceHold              `json:"source_hold,omitempty"`
}

type ProxyServiceSource struct {
	ServiceID           string                        `json:"service_id"`
	Removal             bool                          `json:"removal"`
	ReplacementRequired bool                          `json:"replacement_required"`
	Replacement         *ProxyReplacementImage        `json:"replacement,omitempty"`
	Rollout             *ProxyServiceRolloutReference `json:"rollout,omitempty"`
	Native              ProxySourceVersion            `json:"native"`
	Lifecycle           ProxySourceVersion            `json:"lifecycle"`
	Release             ProxySourceVersion            `json:"release"`
}

type ProxySourceVersion struct {
	Revision int64  `json:"revision"`
	SHA256   string `json:"sha256,omitempty"`
}

// PrepareFrozenProxyServicePreflight does not need the pending Environment
// runtime projection. Membership comes from this parent's immutable identities;
// predecessor selection comes only from acknowledged native runtime at the same
// fixed snapshot as the Component. Missing sources remain explicit absences.
func (repository *Planner) PrepareFrozenProxyServicePreflight(
	ctx context.Context,
	authority FrozenProjectionAuthority,
	parentID string,
	kind core.ComponentKind,
	desired []core.Service,
) (FrozenProjectionAuthority, error) {
	if repository == nil || repository.store == nil || ctx == nil ||
		authority.unitRuntimeCondition == nil || authority.readRevision <= 0 ||
		!kind.IsEnvironmentProxy() || parentID != authority.desiredRevisionID || ids.Validate(ids.KindTask, parentID) != nil {
		return FrozenProjectionAuthority{}, errs.New(
			errs.KindValidationFailed,
			"router Service preflight input is invalid",
		)
	}
	owned, found, err := blueprints.ReadOwnedIdentitiesRevision(ctx, repository.store,
		authority.environmentID, parentID, authority.readRevision)
	if err != nil {
		return FrozenProjectionAuthority{}, err
	}
	if !found || owned.ReadRevision != authority.readRevision {
		return FrozenProjectionAuthority{}, errs.New(errs.KindStateConflict, "router Service membership is unavailable")
	}
	requested := make(map[string]core.Service, len(desired))
	for _, service := range desired {
		if _, duplicate := requested[service.ID]; duplicate {
			return FrozenProjectionAuthority{}, errs.New(
				errs.KindValidationFailed,
				"router Service preflight membership is invalid",
			)
		}
		requested[service.ID] = service
	}
	if len(requested) != len(owned.Record.Services) {
		return FrozenProjectionAuthority{}, errs.New(
			errs.KindStateConflict,
			"router Service preflight omits authored membership",
		)
	}
	preflight := ProxyServicePreflight{EnvironmentID: authority.environmentID,
		ParentTaskID: parentID, ReadRevision: authority.readRevision,
		Implementation: serviceproxy.Implementation(kind)}
	conditions := []keyvalue.Condition{{Key: blueprints.EnvironmentBlueprintOwnedIdentitiesKey(
		authority.environmentID, parentID), ModRevision: owned.Revision}}
	for _, identity := range owned.Record.Services {
		service, exists := requested[identity.ID]
		if !exists || service.Name != identity.Name {
			return FrozenProjectionAuthority{}, errs.New(errs.KindStateConflict, "router Service identity changed")
		}
		preflight.Services = append(preflight.Services, ProxyServiceSource{ServiceID: identity.ID,
			ReplacementRequired: service.Adapter == "" && len(service.ProxyExposures()) != 0})
	}
	omissionCount := 0
	for _, omission := range owned.Record.PendingOmissions {
		if omission.Kind != ids.KindService {
			continue
		}
		omissionCount++
		// Immutable omission intent survives cleanup. Only a durable absence
		// receipt at this snapshot can exclude its historical Release from
		// runtime capture; missing runtime records alone never prove removal.
		target := blueprintunits.ResourceKey{Kind: ids.KindService, ID: omission.ID}
		key := blueprintunits.AppliedKey(authority.environmentID, target)
		read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
			Keys: []string{key}, Revision: authority.readRevision,
		})
		if err != nil {
			return FrozenProjectionAuthority{}, err
		}
		if read == nil || read.ReadRevision != authority.readRevision || len(read.Values) != 1 {
			return FrozenProjectionAuthority{}, errs.New(
				errs.KindInternal,
				"router Service omission read is incomplete",
			)
		}
		absent := false
		if read.Values[0] != nil {
			receipt, err := blueprintunits.DecodeApplied(read.Values[0].Value)
			if err != nil || receipt.EnvironmentID != authority.environmentID || receipt.Target != target {
				keyvalue.ClearValues(read.Values)
				return FrozenProjectionAuthority{}, errs.New(
					errs.KindStateConflict,
					"router Service omission receipt is invalid",
				)
			}
			absent = receipt.State == blueprintunits.Absent
		}
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])})
		keyvalue.ClearValues(read.Values)
		if !absent {
			preflight.Services = append(preflight.Services, ProxyServiceSource{ServiceID: omission.ID, Removal: true})
		}
	}
	slices.SortFunc(preflight.Services, func(a, b ProxyServiceSource) int {
		return cmp.Compare(a.ServiceID, b.ServiceID)
	})
	fenceKey := releases.ReleaseFenceSetKey(authority.environmentID)
	fence, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{fenceKey}, Revision: authority.readRevision},
	)
	if err != nil {
		return FrozenProjectionAuthority{}, err
	}
	if fence == nil || fence.ReadRevision != authority.readRevision || len(fence.Values) != 1 {
		return FrozenProjectionAuthority{}, errs.New(errs.KindInternal, "router release fence read is incomplete")
	}
	activeRelease := fence.Values[0] != nil
	keyvalue.ClearValues(fence.Values)
	if activeRelease {
		return FrozenProjectionAuthority{}, errs.New(errs.KindResourceInUse,
			"finish the active Environment release before preparing its router")
	}
	conditions = append(conditions, keyvalue.Condition{Key: fenceKey})
	for index := range preflight.Services {
		source := &preflight.Services[index]
		keys := proxyServiceSourceKeys(source.ServiceID)
		read, err := repository.store.GetMany(
			ctx,
			keyvalue.GetManyRequest{Keys: keys, Revision: authority.readRevision},
		)
		if err != nil {
			return FrozenProjectionAuthority{}, err
		}
		if read == nil || read.ReadRevision != authority.readRevision || len(read.Values) != len(keys) {
			return FrozenProjectionAuthority{}, errs.New(errs.KindInternal, "router Service source read is incomplete")
		}
		captureProxySourceVersions(source, read.Values)
		if read.Values[1] != nil {
			lifecycle, err := services.DecodeServiceRuntimeRecord(read.Values[1].Value)
			if err != nil {
				keyvalue.ClearValues(read.Values)
				return FrozenProjectionAuthority{}, err
			}
			if lifecycle.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
				source.ReplacementRequired = false
			}
		}
		err = validateProxyServiceSource(preflight, source, requested[source.ServiceID], read.Values)
		keyvalue.ClearValues(read.Values)
		if err != nil {
			return FrozenProjectionAuthority{}, err
		}
		for offset, evidence := range source.versions() {
			conditions = append(conditions, keyvalue.Condition{Key: keys[offset], ModRevision: evidence.Revision})
		}
	}
	if err := preflight.validateSources(); err != nil {
		return FrozenProjectionAuthority{}, err
	}
	authority.proxyServicePreflight = &preflight
	authority.proxyServiceConditions = conditions
	authority.proxyServiceConditionCount = 2 + 3*len(preflight.Services) + omissionCount
	return authority, nil
}

func proxyServiceSourceKeys(serviceID string) []string {
	return []string{
		serviceruntimerecord.Key(serviceID),
		services.ServiceRuntimeKey(serviceID),
		releases.ReleaseProjectionKey(serviceID),
	}
}

func (source ProxyServiceSource) versions() []ProxySourceVersion {
	return []ProxySourceVersion{source.Native, source.Lifecycle, source.Release}
}

func captureProxySourceVersions(source *ProxyServiceSource, values []*keyvalue.KeyValue) {
	versions := []*ProxySourceVersion{&source.Native, &source.Lifecycle, &source.Release}
	for index, value := range values {
		if value != nil {
			digest := sha256.Sum256(value.Value)
			*versions[index] = ProxySourceVersion{Revision: value.ModRevision, SHA256: hex.EncodeToString(digest[:])}
		}
	}
}

func validateProxyServiceSource(preflight ProxyServicePreflight, source *ProxyServiceSource,
	desired core.Service, values []*keyvalue.KeyValue) error {
	var projection domain.ServiceProjection
	if values[2] != nil {
		var err error
		projection, err = releases.DecodeReleaseRecord[domain.ServiceProjection](
			values[2].Value,
			"service-release-projection",
		)
		if err != nil || projection.EnvironmentID != preflight.EnvironmentID ||
			projection.ServiceID != source.ServiceID {
			return errs.New(errs.KindStateConflict, "router Service release source is invalid")
		}
		if projection.ActiveOperationID != "" {
			return errs.New(
				errs.KindStateConflict,
				"finish the active Service release before changing its Environment router",
			)
		}
	}
	if values[1] != nil {
		lifecycle, err := services.DecodeServiceRuntimeRecord(values[1].Value)
		if err != nil || lifecycle.EnvironmentID != preflight.EnvironmentID || lifecycle.ServiceID != source.ServiceID {
			return errs.New(errs.KindStateConflict, "router Service lifecycle source is invalid")
		}
		if lifecycle.Runtime.UnconfirmedLifecycleTaskID != "" {
			return errs.New(
				errs.KindStateConflict,
				"finish the active Service lifecycle Task before changing its Environment router",
			)
		}
	}
	var previous *domain.ProxySelection
	if values[0] != nil {
		record, err := releases.DecodeReleaseRecord[serviceruntimerecord.Record](
			values[0].Value,
			"service-acknowledged-runtime",
		)
		if err != nil || serviceruntimerecord.Validate(record) != nil ||
			record.EnvironmentID != preflight.EnvironmentID ||
			record.Runtime.ServiceID != source.ServiceID ||
			values[1] == nil ||
			values[2] == nil ||
			projection.ServingReleaseID != "" && projection.ServingReleaseID != record.Runtime.ReleaseID {
			return errs.New(
				errs.KindStateConflict,
				"router Service acknowledged runtime disagrees with its lifecycle or release source",
			)
		}
		runtime, err := executionplan.OpenNativeServingRuntime(preflight.EnvironmentID, source.ServiceID,
			record.Runtime.CurrentArtifact, record.Runtime.RetainedPriorArtifact)
		if err != nil {
			return err
		}
		previous = &domain.ProxySelection{}
		if runtime.Proxy != nil {
			configuration, err := executionplan.OpenProxyConfiguration(runtime.Proxy)
			if err != nil {
				return err
			}
			previous.Implementation = configuration.Implementation
			for _, port := range configuration.Ports {
				previous.Ports = append(
					previous.Ports,
					domain.ProxyPort{Port: port.Number, Protocol: serviceports.Protocol(port.Protocol)},
				)
			}
		}
	} else if projection.ServingReleaseID != "" {
		return errs.New(errs.KindStateConflict, "router Service has a serving Release but no acknowledged native runtime")
	}
	if source.Removal || desired.Adapter != "" {
		return nil
	}
	strategy, err := domain.DeploymentStrategy("", string(desired.Strategy))
	if err != nil {
		return err
	}
	if desired.Replicas < 1 || uint64(desired.Replicas) > uint64(^uint32(0)) {
		return errs.New(errs.KindValidationFailed, "router Service preflight replica count is invalid")
	}
	if err := domain.ValidateDeploymentReadiness(
		desired.Name, strategy, uint32(desired.Replicas), desired.Healthcheck.Enabled(), desired.ProxyExposures(),
	); err != nil {
		return err
	}
	selection := domain.ProxySelection{}
	if exposures := desired.ProxyExposures(); len(exposures) != 0 {
		selection.Implementation = preflight.Implementation
		selection.Ports, err = domain.ProxyPorts(exposures)
		if err != nil {
			return err
		}
	}
	if err := domain.ValidateProxyTransition(desired.Name, strategy, selection, previous); err != nil {
		return err
	}
	// New declarations have no forwarding process to replace. A previously
	// settled Service may already use the candidate engine after a partial
	// Apply. Both still retain their source comparisons and normal Deploy;
	// only a running predecessor on the other engine needs switch coordination.
	source.ReplacementRequired = source.ReplacementRequired && previous != nil &&
		previous.Implementation != "" && previous.Implementation != selection.Implementation
	return nil
}

func (preflight ProxyServicePreflight) validateSources() error {
	if ids.Validate(ids.KindEnvironment, preflight.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, preflight.ParentTaskID) != nil || preflight.ReadRevision <= 0 ||
		preflight.Implementation != serviceproxy.Caddy && preflight.Implementation != serviceproxy.Envoy ||
		len(preflight.Services) > 512 {
		return errs.New(errs.KindStateConflict, "router Service preflight authority is invalid")
	}
	for index, source := range preflight.Services {
		if ids.Validate(ids.KindService, source.ServiceID) != nil ||
			index > 0 && preflight.Services[index-1].ServiceID >= source.ServiceID {
			return errs.New(errs.KindStateConflict, "router Service preflight membership is invalid")
		}
		if source.Removal && source.ReplacementRequired {
			return errs.New(errs.KindStateConflict, "omitted Service cannot carry a proxy replacement")
		}
		if source.ReplacementRequired &&
			(source.Native.Revision == 0 || source.Lifecycle.Revision == 0 || source.Release.Revision == 0) {
			return errs.New(errs.KindStateConflict, "proxy replacement has no acknowledged Service predecessor")
		}
		for _, evidence := range source.versions() {
			digest, err := hex.DecodeString(evidence.SHA256)
			if evidence.Revision < 0 || evidence.Revision > preflight.ReadRevision ||
				evidence.Revision == 0 && evidence.SHA256 != "" ||
				evidence.Revision > 0 &&
					(err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != evidence.SHA256) {
				return errs.New(errs.KindStateConflict, "router Service preflight source evidence is invalid")
			}
		}
	}
	return nil
}

func cloneProxyServicePreflight(value *ProxyServicePreflight) *ProxyServicePreflight {
	if value == nil {
		return nil
	}
	clone := *value
	if value.CandidateInput != nil {
		input := *value.CandidateInput
		clone.CandidateInput = &input
	}
	if value.SourceHold != nil {
		hold := *value.SourceHold
		clone.SourceHold = &hold
	}
	clone.Services = slices.Clone(value.Services)
	for index := range clone.Services {
		clone.Services[index].Replacement = cloneProxyReplacementImage(value.Services[index].Replacement)
		if value.Services[index].Rollout != nil {
			rollout := *value.Services[index].Rollout
			clone.Services[index].Rollout = &rollout
		}
	}
	return &clone
}
