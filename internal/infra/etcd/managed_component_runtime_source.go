package etcd

import (
	"crypto/sha256"
	"encoding/hex"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"sort"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ProjectManagedComponentRuntimeSources binds possible managed runtime to an
// immutable desired revision. Failed attempts therefore remain removable by a
// later Task without depending on the short-lived Component candidate record.
func ProjectManagedComponentRuntimeSources(
	preparation ComponentTaskPreparation,
	projection projectionrecord.EnvironmentComposeProjection,
) ([]projectionrecord.ManagedComponentRuntimeSource, error) {
	sources := append([]projectionrecord.ManagedComponentRuntimeSource(nil), projection.ManagedComponentRuntimeSources...)
	if !componentTaskPreparationIsZero(preparation) {
		if err := validateComponentTaskPreparation(preparation); err != nil {
			return nil, err
		}
		sources = append(sources[:0], preparation.managedRuntimeSources...)
	}

	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		return nil, errs.New(errs.KindInternal, "Blueprint managed Component artifact is corrupt")
	}
	for _, candidate := range preparation.Intent.Candidates {
		if !candidate.Candidate.Desired.Enabled {
			continue
		}
		if len(candidate.Candidate.Runtime.GeneratedServices) != 1 {
			return nil, errs.New(errs.KindInternal, "enabled Component candidate runtime identity is incomplete")
		}
		source, err := managedComponentRuntimeSource(
			projection,
			candidate.Candidate,
			artifact,
		)
		if err != nil {
			return nil, err
		}
		sources = replaceManagedComponentRuntimeSource(sources, source)
	}
	sort.Slice(sources, func(left, right int) bool {
		return sources[left].ComponentKind < sources[right].ComponentKind
	})
	projected := projection
	projected.ManagedComponentRuntimeSources = sources
	if err := projectionrecord.ValidateManagedComponentRuntimeSources(projected); err != nil {
		return nil, err
	}
	return append([]projectionrecord.ManagedComponentRuntimeSource(nil), sources...), nil
}

func selectManagedComponentRuntimeSources(
	desired projectionrecord.EnvironmentComposeProjection,
	applied projectionrecord.EnvironmentComposeProjection,
	hasApplied bool,
) ([]projectionrecord.ManagedComponentRuntimeSource, error) {
	if hasApplied && applied.RevisionID == desired.RevisionID {
		return seedManagedComponentRuntimeSources(applied)
	}
	if err := projectionrecord.ValidateManagedComponentRuntimeSources(desired); err != nil {
		return nil, err
	}
	return append([]projectionrecord.ManagedComponentRuntimeSource(nil), desired.ManagedComponentRuntimeSources...), nil
}

func seedManagedComponentRuntimeSources(
	projection projectionrecord.EnvironmentComposeProjection,
) ([]projectionrecord.ManagedComponentRuntimeSource, error) {
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		return nil, errs.New(errs.KindInternal, "applied managed Component artifact is corrupt")
	}
	result := make([]projectionrecord.ManagedComponentRuntimeSource, 0, projectionrecord.MaximumManagedComponentRuntimeSources)
	for _, component := range projection.Components {
		if !component.Desired.Enabled {
			continue
		}
		source, err := managedComponentRuntimeSource(projection, component, artifact)
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

func managedComponentRuntimeSource(
	projection projectionrecord.EnvironmentComposeProjection,
	component componentrecord.Record,
	artifact *agentpb.ComposeArtifact,
) (projectionrecord.ManagedComponentRuntimeSource, error) {
	if len(component.Runtime.GeneratedServices) != 1 {
		return projectionrecord.ManagedComponentRuntimeSource{}, errs.New(
			errs.KindInternal,
			"managed Component source does not own one generated Service",
		)
	}
	serviceID := component.Runtime.GeneratedServices[0]
	var service *agentpb.ComposeService
	for _, candidate := range artifact.GetServices() {
		if candidate.GetServiceId() == serviceID && candidate.GetOwnerComponentId() == component.Desired.ID {
			if service != nil {
				return projectionrecord.ManagedComponentRuntimeSource{}, errs.New(
					errs.KindInternal,
					"managed Component source Service is duplicated",
				)
			}
			service = candidate
		}
	}
	if service == nil || service.GetComposeName() == "" {
		return projectionrecord.ManagedComponentRuntimeSource{}, errs.New(
			errs.KindInternal,
			"managed Component source Service is absent",
		)
	}
	digest := sha256.Sum256(projection.ComposeArtifact)
	return projectionrecord.ManagedComponentRuntimeSource{
		ComponentKind:  component.Desired.Kind,
		ComponentID:    component.Desired.ID,
		ServiceID:      serviceID,
		ComposeName:    service.GetComposeName(),
		RevisionID:     projection.RevisionID,
		ArtifactID:     artifact.GetArtifactId(),
		ArtifactSHA256: hex.EncodeToString(digest[:]),
	}, nil
}

func replaceManagedComponentRuntimeSource(
	sources []projectionrecord.ManagedComponentRuntimeSource,
	replacement projectionrecord.ManagedComponentRuntimeSource,
) []projectionrecord.ManagedComponentRuntimeSource {
	for index := range sources {
		if sources[index].ComponentID == replacement.ComponentID {
			sources[index] = replacement
			return sources
		}
	}
	return append(sources, replacement)
}
