package taskplanning

import (
	"slices"
	"sort"

	"github.com/AlanD20/groundplane/internal/controller/composerender"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func entryMutationArtifact(projection projectionrecord.EnvironmentComposeProjection) (*agentpb.ComposeArtifact, error) {
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.OwnerId != projection.EnvironmentID {
		return nil, errs.New(errs.KindInternal, "Entry mutation artifact owner is invalid")
	}
	return artifact, nil
}

func entryMutationConsumerIDs(
	baseline, candidate projectionrecord.EnvironmentComposeProjection, artifact *agentpb.ComposeArtifact,
	runningServiceIDs []string,
) ([]string, error) {
	running := make(map[string]bool, len(runningServiceIDs))
	for _, id := range runningServiceIDs {
		found := false
		for _, service := range artifact.Services {
			if service.GetServiceId() == id &&
				service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
				found = true
				break
			}
		}
		if !found {
			return nil, errs.New(errs.KindInternal, "Entry running Service is absent from captured runtime")
		}
		running[id] = true
	}
	previous := make(map[string]entryrecord.Record, len(baseline.Entries))
	for _, entry := range baseline.Entries {
		previous[entry.Entry.ID] = entry
	}
	exposed := make(map[string]bool)
	for _, entry := range candidate.Entries {
		prior, exists := previous[entry.Entry.ID]
		delete(previous, entry.Entry.ID)
		if exists &&
			prior.CurrentValueGenerationID == entry.CurrentValueGenerationID &&
			slices.Equal(prior.Entry.Exposure, entry.Entry.Exposure) {
			continue
		}
		for _, name := range entry.Entry.Exposure {
			exposed[name] = true
		}
		for _, name := range prior.Entry.Exposure {
			exposed[name] = true
		}
	}
	if len(previous) != 0 {
		return nil, errs.New(errs.KindInternal, "Entry reconciliation cannot remove an Entry")
	}
	identities, err := composerender.ComposeIdentitySnapshotFromProjection(candidate)
	if err != nil {
		return nil, err
	}
	selected := make(map[string]bool)
	for _, identity := range identities.Services {
		if !running[identity.ID] ||
			!exposed["all"] &&
				!exposed[identity.Name] {
			continue
		}
		for _, service := range artifact.Services {
			if service.GetServiceId() == identity.ID &&
				service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
				selected[identity.ID] = true
			}
		}
	}
	result := make([]string, 0, len(selected))
	for id := range selected {
		result = append(result, id)
	}
	sort.Strings(result)
	return result, nil
}
