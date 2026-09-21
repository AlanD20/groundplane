package environmentprojection

import (
	"crypto/sha256"
	"encoding/hex"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"sort"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func SelectManagedComponentRuntimeSources(
	desired EnvironmentComposeProjection,
	applied EnvironmentComposeProjection,
	hasApplied bool,
) ([]ManagedComponentRuntimeSource, error) {
	if hasApplied && applied.RevisionID == desired.RevisionID {
		return seedManagedComponentRuntimeSources(applied)
	}
	if err := ValidateManagedComponentRuntimeSources(desired); err != nil {
		return nil, err
	}
	return append([]ManagedComponentRuntimeSource(nil), desired.ManagedComponentRuntimeSources...), nil
}

func seedManagedComponentRuntimeSources(
	projection EnvironmentComposeProjection,
) ([]ManagedComponentRuntimeSource, error) {
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		return nil, errs.New(errs.KindInternal, "applied managed Component artifact is corrupt")
	}
	result := make([]ManagedComponentRuntimeSource, 0, MaximumManagedComponentRuntimeSources)
	for _, component := range projection.Components {
		if !component.Desired.Enabled {
			continue
		}
		source, err := ManagedComponentRuntimeSource(projection, component, artifact)
		if err != nil {
			return nil, err
		}
		result = append(result, source)
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].ComponentKind < result[right].ComponentKind
	})
	return result, nil
}

func ManagedComponentRuntimeSource(
	projection EnvironmentComposeProjection,
	component componentrecord.Record,
	artifact *agentpb.ComposeArtifact,
) (ManagedComponentRuntimeSource, error) {
	if len(component.Runtime.GeneratedServices) != 1 {
		return ManagedComponentRuntimeSource{}, errs.New(
			errs.KindInternal,
			"managed Component source does not own one generated Service",
		)
	}
	serviceID := component.Runtime.GeneratedServices[0]
	var service *agentpb.ComposeService
	for _, candidate := range artifact.GetServices() {
		if candidate.GetServiceId() == serviceID && candidate.GetOwnerComponentId() == component.Desired.ID {
			if service != nil {
				return ManagedComponentRuntimeSource{}, errs.New(
					errs.KindInternal,
					"managed Component source Service is duplicated",
				)
			}
			service = candidate
		}
	}
	if service == nil || service.GetComposeName() == "" {
		return ManagedComponentRuntimeSource{}, errs.New(
			errs.KindInternal,
			"managed Component source Service is absent",
		)
	}
	digest := sha256.Sum256(projection.ComposeArtifact)
	return ManagedComponentRuntimeSource{
		ComponentKind:  component.Desired.Kind,
		ComponentID:    component.Desired.ID,
		ServiceID:      serviceID,
		ComposeName:    service.GetComposeName(),
		RevisionID:     projection.RevisionID,
		ArtifactID:     artifact.GetArtifactId(),
		ArtifactSHA256: hex.EncodeToString(digest[:]),
	}, nil
}

func ReplaceManagedComponentRuntimeSource(
	sources []ManagedComponentRuntimeSource,
	replacement ManagedComponentRuntimeSource,
) []ManagedComponentRuntimeSource {
	for index := range sources {
		if sources[index].ComponentID == replacement.ComponentID {
			sources[index] = replacement
			return sources
		}
	}
	return append(sources, replacement)
}
