package controller

import (
	"crypto/sha256"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

type EnvironmentEntryArtifactMutation struct {
	RevisionID       string
	ArtifactID       string
	PlanID           string
	RenderGeneration uint64
	Entries          []entryrecord.Record
}

func ProjectEnvironmentEntryMutation(
	current etcd.EnvironmentComposeProjection,
	mutation EnvironmentEntryArtifactMutation,
) (etcd.EnvironmentComposeProjection, []EnvironmentEntryMaterialization, error) {
	if ids.Validate(ids.KindEnvironment, current.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, mutation.RevisionID) != nil ||
		ids.Validate(ids.KindConfig, mutation.ArtifactID) != nil ||
		ids.Validate(ids.KindPlan, mutation.PlanID) != nil || mutation.RenderGeneration == 0 {
		return etcd.EnvironmentComposeProjection{}, nil, errs.New(
			errs.KindInternal, "Environment Entry mutation projection input is invalid",
		)
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(current.ComposeArtifact, artifact); err != nil ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != current.EnvironmentID {
		return etcd.EnvironmentComposeProjection{}, nil, errs.New(
			errs.KindInternal, "Environment Entry baseline artifact is corrupt",
		)
	}
	materializations, err := projectEnvironmentEntryMaterializations(current, mutation.Entries)
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, nil, err
	}
	mutated, err := mutateEnvironmentEntryArtifact(artifact, current, mutation)
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, nil, err
	}
	artifactValue, err := (proto.MarshalOptions{Deterministic: true}).Marshal(mutated)
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	candidate := current
	candidate.RevisionID = mutation.RevisionID
	candidate.RenderGeneration = mutation.RenderGeneration
	candidate.ComposeArtifact = artifactValue
	candidate.NormalizedCompose = append([]byte(nil), current.NormalizedCompose...)
	candidate.DesiredZones = append([]etcd.EnvironmentZoneProjection(nil), current.DesiredZones...)
	candidate.DesiredServices = append([]etcd.EnvironmentServiceProjection(nil), current.DesiredServices...)
	candidate.DesiredRoutes = append([]etcd.EnvironmentRouteProjection(nil), current.DesiredRoutes...)
	candidate.Volumes = append([]etcd.EnvironmentVolumeIdentity(nil), current.Volumes...)
	candidate.VolumeMounts = append([]etcd.EnvironmentServiceVolumeMount(nil), current.VolumeMounts...)
	candidate.Components = append([]componentrecord.Record(nil), current.Components...)
	candidate.Entries = append([]entryrecord.Record(nil), mutation.Entries...)
	sort.Slice(candidate.Entries, func(left, right int) bool {
		return candidate.Entries[left].Entry.ID < candidate.Entries[right].Entry.ID
	})
	candidate.ServiceDependencyPlans = current.ServiceDependencyPlans.Clone()
	return candidate, materializations, nil
}

func projectEnvironmentEntryMaterializations(
	projection etcd.EnvironmentComposeProjection,
	entries []entryrecord.Record,
) ([]EnvironmentEntryMaterialization, error) {
	identities, err := ComposeIdentitySnapshotFromProjection(projection)
	if err != nil {
		return nil, err
	}
	project := &composetypes.Project{
		Name: "entry-projection", Services: make(composetypes.Services, len(identities.Services)),
		DisabledServices: make(composetypes.Services),
	}
	for _, service := range identities.Services {
		project.Services[service.Name] = composetypes.ServiceConfig{Name: service.Name}
	}
	result, err := ProjectEnvironmentEntries(
		project, projection.EnvironmentID, artifactVolumeDirectory(projection), identities.Services, entries,
	)
	if err != nil {
		return nil, err
	}
	return result.Materializations, nil
}

func artifactVolumeDirectory(projection etcd.EnvironmentComposeProjection) string {
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(projection.ComposeArtifact, artifact) != nil {
		return ""
	}
	return artifact.GetAuthorizedVolumeDir()
}

func mutateEnvironmentEntryArtifact(
	current *agentpb.ComposeArtifact,
	projection etcd.EnvironmentComposeProjection,
	mutation EnvironmentEntryArtifactMutation,
) (*agentpb.ComposeArtifact, error) {
	owned := proto.Clone(current).(*agentpb.ComposeArtifact)
	if err := validateRuntimeServiceOwnership(owned); err != nil {
		return nil, err
	}
	var document yaml.Node
	if yaml.Unmarshal(owned.GetCanonicalYaml(), &document) != nil || len(document.Content) != 1 ||
		document.Content[0].Kind != yaml.MappingNode {
		return nil, errs.New(errs.KindInternal, "normalized Compose artifact YAML is corrupt")
	}
	root := document.Content[0]
	services, err := serviceArtifactMapping(root)
	if err != nil {
		return nil, err
	}
	identities, err := ComposeIdentitySnapshotFromProjection(projection)
	if err != nil {
		return nil, err
	}
	runtimeTargets, err := entryArtifactRuntimeTargets(owned, identities.Services, services)
	if err != nil {
		return nil, err
	}
	managedEnvPaths := entryArtifactManagedEnvironmentPaths(
		projection.EnvironmentID, owned.GetAuthorizedVolumeDir(), identities.Services,
	)
	oldMounts := entryArtifactFileMounts(owned.GetAuthorizedVolumeDir(), projection.Entries)
	newMounts := entryArtifactFileMounts(owned.GetAuthorizedVolumeDir(), mutation.Entries)
	serviceEnvironment := entryArtifactServiceEnvironment(mutation.Entries)
	for index := 0; index+1 < len(services.Content); index += 2 {
		name := services.Content[index].Value
		if !entryArtifactIsWorkload(name, runtimeTargets) {
			continue
		}
		service := services.Content[index+1]
		if err := rewriteEntryArtifactEnvironmentFiles(
			service, managedEnvPaths,
			filepath.Join(owned.GetAuthorizedVolumeDir(), filepath.FromSlash(EnvFileName(projection.EnvironmentID))),
			entryArtifactServiceEnvironmentPath(
				owned.GetAuthorizedVolumeDir(), projection.EnvironmentID, name, runtimeTargets, serviceEnvironment,
			),
		); err != nil {
			return nil, err
		}
		if err := rewriteEntryArtifactMounts(service, name, runtimeTargets, oldMounts, newMounts); err != nil {
			return nil, err
		}
	}
	canonical, err := yaml.Marshal(&document)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(canonical)
	owned.ArtifactId = mutation.ArtifactID
	owned.CanonicalYaml = canonical
	owned.YamlSha256 = digest[:]
	return owned, nil
}

func entryArtifactRuntimeTargets(
	artifact *agentpb.ComposeArtifact,
	identities []ComposeResourceIdentity,
	services *yaml.Node,
) (map[string][]string, error) {
	logical := make(map[string]string, len(identities))
	for _, identity := range identities {
		logical[identity.ID] = identity.Name
	}
	result := make(map[string][]string, len(identities))
	seenRuntime := make(map[string]struct{}, len(artifact.GetServices()))
	for _, service := range artifact.GetServices() {
		name := service.GetComposeName()
		logicalName, exists := logical[service.GetServiceId()]
		if !exists || mappingIndex(services, name) < 0 {
			return nil, errs.New(errs.KindInternal, "Environment Entry runtime Service identity is inconsistent")
		}
		seenRuntime[name] = struct{}{}
		if service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			result[logicalName] = append(result[logicalName], name)
		}
	}
	if len(seenRuntime) != len(services.Content)/2 {
		return nil, errs.New(errs.KindInternal, "Environment Entry runtime Service metadata is incomplete")
	}
	for name := range result {
		sort.Strings(result[name])
	}
	return result, nil
}

func entryArtifactIsWorkload(name string, targets map[string][]string) bool {
	for _, names := range targets {
		for _, candidate := range names {
			if candidate == name {
				return true
			}
		}
	}
	return false
}

func entryArtifactManagedEnvironmentPaths(
	environmentID string,
	volumeDir string,
	services []ComposeResourceIdentity,
) map[string]struct{} {
	result := map[string]struct{}{
		filepath.Join(volumeDir, filepath.FromSlash(EnvFileName(environmentID))): {},
	}
	for _, service := range services {
		result[filepath.Join(volumeDir, filepath.FromSlash(ServiceEnvFileName(environmentID, service.Name)))] = struct{}{}
	}
	return result
}

type entryArtifactMount struct {
	source  string
	target  string
	exposes map[string]struct{}
}

func entryArtifactFileMounts(volumeDir string, entries []entryrecord.Record) []entryArtifactMount {
	result := make([]entryArtifactMount, 0)
	for _, record := range entries {
		entry := record.Entry
		if entry.Kind != core.EntryKindFile {
			continue
		}
		exposes := make(map[string]struct{}, len(entry.Exposure))
		for _, service := range entry.Exposure {
			exposes[service] = struct{}{}
		}
		result = append(result, entryArtifactMount{
			source: filepath.Join(volumeDir, filepath.FromSlash(entry.Path)),
			target: path.Join("/", entry.Path), exposes: exposes,
		})
	}
	return result
}

func entryArtifactServiceEnvironment(entries []entryrecord.Record) map[string]struct{} {
	result := make(map[string]struct{})
	for _, record := range entries {
		entry := record.Entry
		if entry.Kind != core.EntryKindEnv || entry.ExposesAll() {
			continue
		}
		for _, service := range entry.Exposure {
			result[service] = struct{}{}
		}
	}
	return result
}

func entryArtifactServiceEnvironmentPath(
	volumeDir string,
	environmentID string,
	runtimeName string,
	runtimeTargets map[string][]string,
	serviceEnvironment map[string]struct{},
) string {
	for logicalName := range serviceEnvironment {
		for _, candidate := range runtimeTargets[logicalName] {
			if candidate == runtimeName {
				return filepath.Join(
					volumeDir, filepath.FromSlash(ServiceEnvFileName(environmentID, logicalName)),
				)
			}
		}
	}
	return ""
}

func rewriteEntryArtifactEnvironmentFiles(
	service *yaml.Node,
	managed map[string]struct{},
	canonical string,
	scoped string,
) error {
	retained := make([]*yaml.Node, 0)
	if index := mappingIndex(service, "env_file"); index >= 0 {
		sequence := service.Content[index+1]
		if sequence.Kind != yaml.SequenceNode {
			return errs.New(errs.KindInternal, "normalized Compose env_file is corrupt")
		}
		for _, item := range sequence.Content {
			value, err := entryArtifactEnvironmentFilePath(item)
			if err != nil {
				return err
			}
			if _, remove := managed[value]; !remove {
				retained = append(retained, item)
			}
		}
	}
	sequence := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
	sequence.Content = append(sequence.Content, entryArtifactEnvironmentFileNode(canonical))
	sequence.Content = append(sequence.Content, retained...)
	if scoped != "" {
		sequence.Content = append(sequence.Content, entryArtifactEnvironmentFileNode(scoped))
	}
	setMappingNode(service, "env_file", sequence)
	return nil
}

func entryArtifactEnvironmentFilePath(node *yaml.Node) (string, error) {
	if node.Kind == yaml.ScalarNode {
		return node.Value, nil
	}
	if node.Kind != yaml.MappingNode {
		return "", errs.New(errs.KindInternal, "normalized Compose env_file item is corrupt")
	}
	index := mappingIndex(node, "path")
	if index < 0 || node.Content[index+1].Kind != yaml.ScalarNode {
		return "", errs.New(errs.KindInternal, "normalized Compose env_file path is corrupt")
	}
	return node.Content[index+1].Value, nil
}

func entryArtifactEnvironmentFileNode(value string) *yaml.Node {
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	appendMappingValue(node, "path", scalarNode(value))
	appendMappingValue(node, "required", &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "true"})
	return node
}

func rewriteEntryArtifactMounts(
	service *yaml.Node,
	runtimeName string,
	runtimeTargets map[string][]string,
	oldMounts []entryArtifactMount,
	newMounts []entryArtifactMount,
) error {
	retained := make([]*yaml.Node, 0)
	if index := mappingIndex(service, "volumes"); index >= 0 {
		sequence := service.Content[index+1]
		if sequence.Kind != yaml.SequenceNode {
			return errs.New(errs.KindInternal, "normalized Compose Service volumes are corrupt")
		}
		for _, item := range sequence.Content {
			source, target, err := entryArtifactMountIdentity(item)
			if err != nil {
				return err
			}
			remove := false
			for _, mount := range oldMounts {
				if mount.source == source && mount.target == target {
					remove = true
					break
				}
			}
			if !remove {
				retained = append(retained, item)
			}
		}
	}
	for _, mount := range newMounts {
		if !entryArtifactMountExposesRuntime(mount, runtimeName, runtimeTargets) {
			continue
		}
		for _, item := range retained {
			_, target, err := entryArtifactMountIdentity(item)
			if err != nil {
				return err
			}
			if target == mount.target {
				return errs.New(
					errs.KindNameConflict,
					"Environment file Entry conflicts with an existing Service mount",
				)
			}
		}
		retained = append(retained, entryArtifactMountNode(mount))
	}
	if len(retained) == 0 {
		removeMappingValue(service, "volumes")
		return nil
	}
	setMappingNode(service, "volumes", &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Content: retained})
	return nil
}

func entryArtifactMountExposesRuntime(
	mount entryArtifactMount,
	runtimeName string,
	runtimeTargets map[string][]string,
) bool {
	if _, all := mount.exposes["all"]; all {
		return true
	}
	for logicalName := range mount.exposes {
		for _, candidate := range runtimeTargets[logicalName] {
			if candidate == runtimeName {
				return true
			}
		}
	}
	return false
}

func entryArtifactMountIdentity(node *yaml.Node) (string, string, error) {
	if node.Kind == yaml.ScalarNode {
		parts := strings.Split(node.Value, ":")
		if len(parts) < 2 {
			return "", "", errs.New(errs.KindInternal, "normalized Compose compact mount is corrupt")
		}
		return strings.Join(parts[:len(parts)-1], ":"), parts[len(parts)-1], nil
	}
	if node.Kind != yaml.MappingNode {
		return "", "", errs.New(errs.KindInternal, "normalized Compose mount is corrupt")
	}
	source := mappingScalar(node, "source")
	target := mappingScalar(node, "target")
	if target == "" {
		return "", "", errs.New(errs.KindInternal, "normalized Compose mount target is absent")
	}
	return source, target, nil
}

func mappingScalar(node *yaml.Node, key string) string {
	index := mappingIndex(node, key)
	if index < 0 || node.Content[index+1].Kind != yaml.ScalarNode {
		return ""
	}
	return node.Content[index+1].Value
}

func entryArtifactMountNode(mount entryArtifactMount) *yaml.Node {
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	appendMappingValue(node, "type", scalarNode("bind"))
	appendMappingValue(node, "source", scalarNode(mount.source))
	appendMappingValue(node, "target", scalarNode(mount.target))
	appendMappingValue(node, "read_only", &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "true"})
	bind := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	appendMappingValue(bind, "create_host_path", &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "false"})
	appendMappingValue(node, "bind", bind)
	return node
}
