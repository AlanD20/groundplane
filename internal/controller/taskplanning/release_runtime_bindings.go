package taskplanning

import (
	"context"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// Release reconstruction keeps authored Compose separate from execution-only
// bindings. Both Entry exposure and Attach membership come from the immutable
// release projection; neither is rediscovered from current mutable resources.
func loadPinnedEnvironmentProject(
	ctx context.Context,
	projection etcd.EnvironmentComposeProjection,
	volumeDir string,
	releases map[string]composerender.ComposeReleaseIdentity,
) (*composetypes.Project, error) {
	project, err := composerender.LoadNormalizedEnvironmentProject(ctx, projection)
	if err != nil || len(releases) == 0 {
		return project, err
	}
	identities, err := composerender.ComposeIdentitySnapshotFromProjection(projection)
	if err != nil {
		return nil, err
	}
	entries, err := composerender.ProjectEnvironmentEntries(
		project, projection.EnvironmentID, volumeDir, identities.Services, projection.Entries,
	)
	if err != nil {
		return nil, err
	}
	joins, err := sealedReleaseAttachJoins(projection)
	if err != nil {
		return nil, err
	}
	if _, err := ProjectAttachNetworks(entries.Project, projection, joins); err != nil {
		return nil, err
	}
	return entries.Project, nil
}

func sealedReleaseAttachJoins(projection etcd.EnvironmentComposeProjection) ([]etcd.AttachTaskNetworkJoin, error) {
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil ||
		artifact.OwnerId != projection.EnvironmentID ||
		artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT {
		return nil, errs.New(errs.KindInternal, "sealed release binding artifact is invalid")
	}
	var document yaml.Node
	if err := yaml.Unmarshal(artifact.CanonicalYaml, &document); err != nil || len(document.Content) != 1 {
		return nil, errs.New(errs.KindInternal, "sealed release binding YAML is invalid")
	}
	services, err := composerender.ServiceArtifactMapping(document.Content[0])
	if err != nil {
		return nil, err
	}
	selected := make(map[string]bool, len(projection.DesiredServices))
	for _, service := range projection.DesiredServices {
		selected[service.Desired.ID] = true
	}
	networks := &yaml.Node{Kind: yaml.MappingNode}
	if index := composerender.MappingIndex(document.Content[0], "networks"); index >= 0 {
		networks = document.Content[0].Content[index+1]
	}
	members := make(map[string]map[string]bool)
	for _, service := range artifact.Services {
		if !selected[service.ServiceId] {
			continue
		}
		index := composerender.MappingIndex(services, service.ComposeName)
		if index < 0 {
			return nil, errs.New(errs.KindInternal, "sealed release binding Service is absent")
		}
		references := make(map[string]map[string]bool)
		if err := composerender.RetainedServiceResourceReferences(services.Content[index+1], references); err != nil {
			return nil, err
		}
		for name := range references["networks"] {
			networkID, managed := managedAttachNetworkID(name)
			if !managed {
				continue
			}
			index := composerender.MappingIndex(networks, name)
			dockerName, nameErr := networkname.New(networkID)
			if index < 0 || nameErr != nil ||
				composerender.MappingScalar(networks.Content[index+1], "external") != "true" ||
				composerender.MappingScalar(networks.Content[index+1], "name") != dockerName {
				return nil, errs.New(errs.KindInternal, "sealed release Attach network identity diverges")
			}
			if members[networkID] == nil {
				members[networkID] = make(map[string]bool)
			}
			members[networkID][service.ServiceId] = true
		}
	}
	joins := make([]etcd.AttachTaskNetworkJoin, 0, len(members))
	for networkID, services := range members {
		join := etcd.AttachTaskNetworkJoin{NetworkID: networkID}
		for serviceID := range services {
			join.ServiceIDs = append(join.ServiceIDs, serviceID)
		}
		sort.Strings(join.ServiceIDs)
		joins = append(joins, join)
	}
	sort.Slice(joins, func(i, j int) bool { return joins[i].NetworkID < joins[j].NetworkID })
	return joins, nil
}
