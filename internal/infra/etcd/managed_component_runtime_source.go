package etcd

import (
	"crypto/sha256"
	"encoding/hex"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const maximumManagedComponentRuntimeSources = 2

// ProjectManagedComponentRuntimeSources binds possible managed runtime to an
// immutable desired revision. Failed attempts therefore remain removable by a
// later Task without depending on the short-lived Component candidate record.
func ProjectManagedComponentRuntimeSources(
	preparation ComponentTaskPreparation,
	projection EnvironmentComposeProjection,
) ([]ManagedComponentRuntimeSource, error) {
	sources := append([]ManagedComponentRuntimeSource(nil), projection.ManagedComponentRuntimeSources...)
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
	if err := validateManagedComponentRuntimeSources(projected); err != nil {
		return nil, err
	}
	return append([]ManagedComponentRuntimeSource(nil), sources...), nil
}

func selectManagedComponentRuntimeSources(
	desired EnvironmentComposeProjection,
	applied EnvironmentComposeProjection,
	hasApplied bool,
) ([]ManagedComponentRuntimeSource, error) {
	if hasApplied && applied.RevisionID == desired.RevisionID {
		return seedManagedComponentRuntimeSources(applied)
	}
	if err := validateManagedComponentRuntimeSources(desired); err != nil {
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
	result := make([]ManagedComponentRuntimeSource, 0, maximumManagedComponentRuntimeSources)
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

func replaceManagedComponentRuntimeSource(
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

func validateManagedComponentRuntimeSources(projection EnvironmentComposeProjection) error {
	if len(projection.ManagedComponentRuntimeSources) > maximumManagedComponentRuntimeSources {
		return errs.New(errs.KindValidationFailed, "managed Component runtime source count is invalid")
	}
	components := make(map[core.ComponentKind]componentrecord.Record, len(projection.Components))
	for _, component := range projection.Components {
		components[component.Desired.Kind] = component
	}
	previousKind := core.ComponentKind("")
	for _, source := range projection.ManagedComponentRuntimeSources {
		component, known := components[source.ComponentKind]
		digest, digestErr := hex.DecodeString(source.ArtifactSHA256)
		if !known || source.ComponentKind <= previousKind || component.Desired.ID != source.ComponentID ||
			ids.Validate(ids.KindComponent, source.ComponentID) != nil ||
			ids.Validate(ids.KindService, source.ServiceID) != nil || source.ComposeName == "" ||
			ids.Validate(ids.KindTask, source.RevisionID) != nil ||
			ids.Validate(ids.KindConfig, source.ArtifactID) != nil || digestErr != nil || len(digest) != sha256.Size {
			return errs.New(errs.KindValidationFailed, "managed Component runtime source is invalid or unsorted")
		}
		if component.Desired.Enabled && (len(component.Runtime.GeneratedServices) != 1 ||
			component.Runtime.GeneratedServices[0] != source.ServiceID) {
			return errs.New(errs.KindValidationFailed, "enabled Component runtime source identity changed")
		}
		previousKind = source.ComponentKind
	}
	return nil
}
