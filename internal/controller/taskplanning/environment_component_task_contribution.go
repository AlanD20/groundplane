package taskplanning

import (
	"crypto/sha256"
	"encoding/hex"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type EnvironmentManagedConfigApplyInput struct {
	RevisionID       string
	RenderGeneration uint64
	Components       []componentrecord.Record
	ComponentCatalog []componentrender.EnvironmentComponentRegistration
	Materializations []materializationrecord.Record
	Artifact         *agentpb.ComposeArtifact
}

type EnvironmentManagedConfigApply struct {
	action             *agentpb.ComponentApply
	prerequisiteStepID string
}

func ResolveEnvironmentManagedConfigApply(
	input EnvironmentManagedConfigApplyInput,
) (EnvironmentManagedConfigApply, bool, error) {
	if ids.Validate(ids.KindTask, input.RevisionID) != nil || input.RenderGeneration == 0 || input.Artifact == nil ||
		ids.Validate(ids.KindConfig, input.Artifact.GetArtifactId()) != nil ||
		input.Artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		ids.Validate(ids.KindEnvironment, input.Artifact.GetOwnerId()) != nil {
		return EnvironmentManagedConfigApply{}, false, errs.New(
			errs.KindInternal,
			"Blueprint managed-config apply input is invalid",
		)
	}

	if err := componentrender.ValidateEnvironmentComponentCatalog(input.ComponentCatalog); err != nil {
		return EnvironmentManagedConfigApply{}, false, err
	}
	candidate, registration, hasCandidate, err := resolveEnvironmentManagedConfigCandidate(
		input.Components,
		input.ComponentCatalog,
	)
	if err != nil {
		return EnvironmentManagedConfigApply{}, false, err
	}
	materializationIndex, hasMaterialization, err := resolveEnvironmentManagedConfigMaterialization(
		input.Materializations,
		candidate.Desired.ID,
		registration.ManagedConfiguration,
		input.ComponentCatalog,
	)
	if err != nil {
		return EnvironmentManagedConfigApply{}, false, err
	}
	if !hasCandidate {
		return EnvironmentManagedConfigApply{}, false, nil
	}
	if input.Artifact.GetOwnerId() != candidate.Desired.OwnerID {
		return EnvironmentManagedConfigApply{}, false, errs.New(
			errs.KindInternal,
			"Blueprint managed-config candidate and artifact owners differ",
		)
	}
	serviceID := candidate.Runtime.GeneratedServices[0]
	if err := requireEnvironmentManagedConfigArtifactService(input.Artifact, serviceID); err != nil {
		return EnvironmentManagedConfigApply{}, false, err
	}
	if !hasMaterialization {
		return EnvironmentManagedConfigApply{}, false, nil
	}
	digest, err := validateEnvironmentManagedConfigMaterialization(
		input.Materializations[materializationIndex],
		input.RevisionID,
		candidate,
		registration.ManagedConfiguration,
	)
	if err != nil {
		return EnvironmentManagedConfigApply{}, false, err
	}
	reference := input.Materializations[materializationIndex]
	action, err := componentrender.BuildEnvironmentComponentAction(
		input.ComponentCatalog,
		candidate.Desired.Kind,
		candidate.Desired.ID,
		registration.ManagedConfiguration.ActionID,
		reference.MaterializationID,
		digest,
		input.RenderGeneration,
	)
	if err != nil {
		return EnvironmentManagedConfigApply{}, false, err
	}
	return EnvironmentManagedConfigApply{
		action: action, prerequisiteStepID: reference.StepID,
	}, true, nil
}

func resolveEnvironmentManagedConfigCandidate(
	components []componentrecord.Record,
	catalog []componentrender.EnvironmentComponentRegistration,
) (componentrecord.Record, componentrender.EnvironmentComponentRegistration, bool, error) {
	var candidate componentrecord.Record
	var selected componentrender.EnvironmentComponentRegistration
	found := false
	seenKinds := make(map[core.ComponentKind]bool)
	seenIDs := make(map[string]bool)
	ownerID := ""
	for _, component := range components {
		registration, registered := environmentComponentRegistration(catalog, component.Desired.Kind)
		if !registered || registration.ManagedConfiguration == nil {
			continue
		}
		if seenKinds[component.Desired.Kind] || seenIDs[component.Desired.ID] {
			return componentrecord.Record{}, componentrender.EnvironmentComponentRegistration{}, false, errs.New(
				errs.KindInternal,
				"Blueprint managed-config provider repeats its kind or identity",
			)
		}
		if ids.Validate(ids.KindComponent, component.Desired.ID) != nil ||
			component.Desired.Owner != core.ComponentOwnerEnvironment ||
			ids.Validate(ids.KindEnvironment, component.Desired.OwnerID) != nil ||
			(ownerID != "" && component.Desired.OwnerID != ownerID) {
			return componentrecord.Record{}, componentrender.EnvironmentComponentRegistration{}, false, errs.New(
				errs.KindInternal,
				"Blueprint managed-config candidate identity is invalid",
			)
		}
		seenKinds[component.Desired.Kind], seenIDs[component.Desired.ID] = true, true
		ownerID = component.Desired.OwnerID
		if component.Desired.Enabled {
			if len(component.Runtime.GeneratedServices) != 1 ||
				ids.Validate(ids.KindService, component.Runtime.GeneratedServices[0]) != nil {
				return componentrecord.Record{}, componentrender.EnvironmentComponentRegistration{}, false, errs.New(
					errs.KindInternal,
					"enabled Blueprint managed-config candidate Service identity is invalid",
				)
			}
		} else if len(component.Runtime.GeneratedServices) != 0 {
			return componentrecord.Record{}, componentrender.EnvironmentComponentRegistration{}, false, errs.New(
				errs.KindInternal,
				"disabled Blueprint managed-config candidate has a generated Service",
			)
		}
		if !component.Desired.Enabled {
			continue
		}
		if found {
			return componentrecord.Record{}, componentrender.EnvironmentComponentRegistration{}, false, errs.New(
				errs.KindValidationFailed,
				"Environment can enable only one proxy; disable the current proxy before selecting another",
			)
		}
		candidate = component
		selected = registration
		found = true
	}
	return candidate, selected, found, nil
}

func environmentComponentRegistration(
	catalog []componentrender.EnvironmentComponentRegistration,
	kind core.ComponentKind,
) (componentrender.EnvironmentComponentRegistration, bool) {
	for _, registration := range catalog {
		if registration.Kind == kind {
			return registration, true
		}
	}
	return componentrender.EnvironmentComponentRegistration{}, false
}

func resolveEnvironmentManagedConfigMaterialization(
	materializations []materializationrecord.Record,
	componentID string,
	managed *componentrender.EnvironmentManagedConfigurationRegistration,
	catalog []componentrender.EnvironmentComponentRegistration,
) (int, bool, error) {
	selected := -1
	for index := range materializations {
		reference := materializations[index]
		componentFile := reference.Source.ComponentFile
		if componentFile == nil || !claimsManagedConfiguration(componentFile.Path, catalog) {
			continue
		}
		if managed == nil || componentFile.ComponentID != componentID || componentFile.Path != managed.SourcePath {
			return -1, false, errs.New(
				errs.KindInternal,
				"Blueprint managed configuration belongs to a disabled or unselected proxy",
			)
		}
		if selected >= 0 {
			return -1, false, errs.New(
				errs.KindInternal,
				"Blueprint managed configuration materialization is duplicated",
			)
		}
		selected = index
	}
	return selected, selected >= 0, nil
}

func claimsManagedConfiguration(
	referencePath string,
	catalog []componentrender.EnvironmentComponentRegistration,
) bool {
	for _, registration := range catalog {
		if registration.ManagedConfiguration != nil &&
			referencePath == registration.ManagedConfiguration.SourcePath {
			return true
		}
	}
	return false
}

func requireEnvironmentManagedConfigArtifactService(
	artifact *agentpb.ComposeArtifact,
	serviceID string,
) error {
	matches := 0
	for _, service := range artifact.GetServices() {
		if service == nil {
			return errs.New(errs.KindInternal, "Blueprint managed-config artifact Service is invalid")
		}
		if service.GetServiceId() == serviceID {
			matches++
		}
	}
	if matches != 1 {
		return errs.New(
			errs.KindInternal,
			"Blueprint managed-config generated Service is not uniquely present in the artifact",
		)
	}
	return nil
}

func validateEnvironmentManagedConfigMaterialization(
	reference materializationrecord.Record,
	revisionID string,
	candidate componentrecord.Record,
	managed *componentrender.EnvironmentManagedConfigurationRegistration,
) ([sha256.Size]byte, error) {
	componentFile := reference.Source.ComponentFile
	if managed == nil || ids.Validate(ids.KindStep, reference.StepID) != nil ||
		ids.Validate(ids.KindConfig, reference.MaterializationID) != nil ||
		reference.EnvironmentID != candidate.Desired.OwnerID ||
		reference.Destination != managed.SourcePath ||
		reference.ServiceID != "" || reference.ServiceName != "" ||
		reference.OutputKind != materializationrecord.OutputPlainFile ||
		reference.UID != 0 || reference.GID != 0 ||
		reference.Mode != uint32(entrymaterialization.ModeReadOnly) ||
		reference.Source.Kind != materializationrecord.SourceComponentFile ||
		componentFile == nil || reference.Source.BlueprintFile != nil ||
		reference.Source.EntryValue != nil || reference.Source.GeneratedEnvironment != nil ||
		componentFile.RevisionID != revisionID ||
		componentFile.ComponentID != candidate.Desired.ID ||
		componentFile.Path != managed.SourcePath {
		return [sha256.Size]byte{}, errs.New(
			errs.KindInternal,
			"Blueprint managed configuration materialization identity is invalid",
		)
	}
	decoded, err := hex.DecodeString(reference.SHA256)
	if err != nil || len(decoded) != sha256.Size {
		return [sha256.Size]byte{}, errs.New(
			errs.KindInternal,
			"Blueprint managed configuration materialization digest is invalid",
		)
	}
	var digest [sha256.Size]byte
	copy(digest[:], decoded)
	return digest, nil
}

func (apply EnvironmentManagedConfigApply) ExecutionStep(
	stepID string,
	timeoutSeconds uint32,
) (*agentpb.ExecutionStep, error) {
	if ids.Validate(ids.KindStep, stepID) != nil || ids.Validate(ids.KindStep, apply.prerequisiteStepID) != nil ||
		timeoutSeconds == 0 || apply.action == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint managed-config apply step is invalid")
	}
	return &agentpb.ExecutionStep{
		StepId: stepID, TimeoutSeconds: timeoutSeconds, PrerequisiteStepId: apply.prerequisiteStepID,
		Payload: &agentpb.ExecutionStep_ComponentApply{
			ComponentApply: proto.Clone(apply.action).(*agentpb.ComponentApply),
		},
	}, nil
}
