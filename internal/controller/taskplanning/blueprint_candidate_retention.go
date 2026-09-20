package taskplanning

import (
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"sort"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// The immutable candidate projection already contains the captured runtime.
// Re-rendering selected candidates must not regenerate unselected ownership.
func retainBlueprintUnselectedRuntime(
	artifact *agentpb.ComposeArtifact,
	captured []byte,
	candidates []string,
) (*agentpb.ComposeArtifact, error) {
	prior := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(captured, prior); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint captured runtime artifact is invalid")
	}
	selected := make(map[string]bool, len(candidates))
	for _, serviceID := range candidates {
		selected[serviceID] = true
	}
	retained := make(map[string]bool)
	for _, service := range prior.Services {
		if service.OwnerComponentId == "" && !selected[service.ServiceId] &&
			service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED {
			retained[service.ServiceId] = true
		}
	}
	if len(retained) == 0 {
		return artifact, nil
	}
	serviceIDs := make([]string, 0, len(retained))
	for serviceID := range retained {
		serviceIDs = append(serviceIDs, serviceID)
	}
	sort.Strings(serviceIDs)
	return composerender.RetainBlueprintNativeRuntime(artifact, prior, serviceIDs)
}
