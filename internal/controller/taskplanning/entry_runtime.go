package taskplanning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// EntryMutationRuntime captures the runtime onto which Entry decorations are
// applied. The epoch fences its selection in the desired publication.
type EntryMutationRuntime struct {
	Projection        etcd.EnvironmentComposeProjection
	EpochRevision     int64
	RunningServiceIDs []string
	Sources           []etcd.Versioned[serviceruntimerecord.Record]
}

func (resolver *TaskPlanResolver) CaptureEntryMutationRuntime(
	ctx context.Context, current etcd.Versioned[etcd.EnvironmentComposeProjection],
) (EntryMutationRuntime, error) {
	if resolver == nil || resolver.releases == nil {
		return EntryMutationRuntime{}, errs.New(errs.KindInternal, "Entry runtime capture is not configured")
	}
	scope, err := resolver.releases.LoadPlanningScopeAtRevision(ctx, current.Record.EnvironmentID, current.ReadRevision)
	if err != nil {
		return EntryMutationRuntime{}, err
	}
	// The head and immutable-root readers report revisions of different etcd
	// keys. Compare desired identity and bytes at the pinned read, not those keys.
	if scope.Compose.Record.RevisionID != current.Record.RevisionID ||
		!bytes.Equal(scope.Compose.Record.ComposeArtifact, current.Record.ComposeArtifact) {
		return EntryMutationRuntime{}, errs.New(errs.KindStateConflict, "Entry desired runtime source changed")
	}
	baseline, err := entryMutationArtifact(current.Record)
	if err != nil {
		return EntryMutationRuntime{}, err
	}
	attaches, err := resolver.releases.LoadPlanningAttaches(ctx, scope)
	if err != nil {
		return EntryMutationRuntime{}, err
	}
	joins, err := ResolveAttachNetworkJoins(current.Record.EnvironmentID, current.Record, attaches, "")
	if err != nil {
		return EntryMutationRuntime{}, err
	}
	baseline, err = MutateAttachNetworkArtifact(ctx, baseline, current.Record, joins, baseline.ArtifactId)
	if err != nil {
		return EntryMutationRuntime{}, err
	}
	var running []string
	serving := make(map[string]string)
	for _, desired := range current.Record.DesiredServices {
		planning, err := resolver.releases.LoadPlanningServices(ctx, scope, []string{desired.Desired.ID})
		if err != nil {
			return EntryMutationRuntime{}, err
		}
		if planning[0].Projection.ServingReleaseID == "" {
			continue
		}
		if planning[0].Service.Record.Runtime.RuntimeIntent == core.ServiceRuntimeIntentRunning {
			running = append(running, desired.Desired.ID)
			serving[desired.Desired.ID] = planning[0].Projection.ServingReleaseID
		}
	}
	sort.Strings(running)
	runtimeSources, err := resolver.releases.LoadAcknowledgedServiceRuntimesAtRevision(
		ctx, current.Record.EnvironmentID, running, scope.ReadRevision,
	)
	if err != nil {
		return EntryMutationRuntime{}, err
	}
	var sources []*agentpb.ComposeArtifact
	for _, source := range runtimeSources {
		if source.Record.Runtime.ReleaseID != serving[source.Record.Runtime.ServiceID] {
			return EntryMutationRuntime{}, errs.New(errs.KindStateConflict, "Entry acknowledged runtime is not serving")
		}
		for _, value := range [][]byte{
			source.Record.Runtime.CurrentArtifact,
			source.Record.Runtime.RetainedPriorArtifact,
		} {
			if len(value) == 0 {
				continue
			}
			artifact := new(agentpb.ComposeArtifact)
			if proto.Unmarshal(value, artifact) != nil {
				return EntryMutationRuntime{}, errs.New(errs.KindInternal, "Entry acknowledged runtime is corrupt")
			}
			sources = append(sources, artifact)
		}
	}
	if len(running) > 0 {
		baseline, err = entryRuntimeWithoutReplacedProxyConfigs(baseline, sources)
		if err != nil {
			return EntryMutationRuntime{}, err
		}
		baseline, err = RetainBlueprintNativeRuntimeSources(baseline, sources, running)
		if err != nil {
			return EntryMutationRuntime{}, err
		}
	}
	baseline, err = mutateEnvironmentEntryArtifact(baseline, current.Record,
		EnvironmentEntryArtifactMutation{ArtifactID: baseline.ArtifactId, Entries: current.Record.Entries})
	if err != nil {
		return EntryMutationRuntime{}, err
	}
	projection := current.Record
	projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(baseline)
	if err != nil {
		return EntryMutationRuntime{}, errs.Wrap(errs.KindInternal, err)
	}
	return EntryMutationRuntime{
		Projection: projection, EpochRevision: scope.EnvironmentEpochRevision,
		RunningServiceIDs: running, Sources: runtimeSources,
	}, nil
}

// Only a captured stable proxy can replace its old, exclusively owned config.
// The ordinary retained-resource merger still checks every other resource.
func entryRuntimeWithoutReplacedProxyConfigs(
	baseline *agentpb.ComposeArtifact, sources []*agentpb.ComposeArtifact,
) (*agentpb.ComposeArtifact, error) {
	selected := make(map[string]bool)
	for _, source := range sources {
		for _, service := range source.Services {
			if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
				selected[service.ServiceId] = true
			}
		}
	}
	var document yaml.Node
	if yaml.Unmarshal(baseline.CanonicalYaml, &document) != nil || len(document.Content) != 1 ||
		document.Content[0].Kind != yaml.MappingNode {
		return nil, errs.New(errs.KindValidationFailed, "Entry runtime YAML is invalid")
	}
	root := document.Content[0]
	services, err := serviceArtifactMapping(root)
	if err != nil {
		return nil, err
	}
	for _, service := range baseline.Services {
		if !selected[service.ServiceId] ||
			service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		configName := "gp-proxy-" + strings.ToLower(service.ServiceId)
		configIndex, serviceIndex := mappingIndex(root, "configs"), mappingIndex(services, service.ComposeName)
		if service.OwnerComponentId != "" || configIndex < 0 || serviceIndex < 0 {
			return nil, errs.New(errs.KindStateConflict, "Entry proxy config ownership changed")
		}
		configs := root.Content[configIndex+1]
		index := mappingIndex(configs, configName)
		if index < 0 {
			return nil, errs.New(errs.KindStateConflict, "Entry proxy config is absent")
		}
		config := configs.Content[index+1]
		contentIndex := mappingIndex(config, "content")
		digest := sha256.Sum256(service.ProxyConfigJson)
		if len(config.Content) != 2 || contentIndex < 0 || len(service.ProxyConfigJson) == 0 ||
			!bytes.Equal(digest[:], service.ProxyConfigSha256) ||
			config.Content[contentIndex+1].Kind != yaml.ScalarNode ||
			config.Content[contentIndex+1].Value != string(service.ProxyConfigJson) {
			return nil, errs.New(errs.KindStateConflict, "Entry proxy config differs from sealed metadata")
		}
		for index := 0; index < len(services.Content); index += 2 {
			references := make(map[string]map[string]bool)
			if err := retainedServiceResourceReferences(services.Content[index+1], references); err != nil {
				return nil, err
			}
			if index == serviceIndex {
				if len(references["configs"]) != 1 || !references["configs"][configName] {
					return nil, errs.New(errs.KindStateConflict, "Entry proxy config binding changed")
				}
			} else if references["configs"][configName] {
				return nil, errs.New(errs.KindStateConflict, "Entry proxy config is shared with another Service")
			}
		}
		removeMappingValue(configs, configName)
	}
	owned := proto.CloneOf(baseline)
	owned.CanonicalYaml, err = yaml.Marshal(&document)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(owned.CanonicalYaml)
	owned.YamlSha256 = digest[:]
	return owned, nil
}
