package taskplanning

import (
	"crypto/sha256"
	"encoding/hex"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

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

// EnvironmentComponentTaskContributionInput supplies the immutable Blueprint
// evidence needed to optionally contribute the managed-config action step.
type EnvironmentComponentTaskContributionInput struct {
	Apply          EnvironmentManagedConfigApplyInput
	AllocateStep   func() string
	TimeoutSeconds uint32
}

// BuildEnvironmentComponentTaskContribution resolves the managed-config candidate and
// returns the complete optional task contribution. The caller only appends
// the returned generic steps and records; managed-config-specific branching stays in
// this controller capability module.
func BuildEnvironmentComponentTaskContribution(
	input EnvironmentComponentTaskContributionInput,
) ([]*agentpb.ExecutionStep, []taskjournal.TaskStepRecord, error) {
	if input.AllocateStep == nil || input.TimeoutSeconds == 0 {
		return nil, nil, errs.New(errs.KindInternal, "Blueprint managed-config task contribution input is invalid")
	}
	apply, found, err := ResolveEnvironmentManagedConfigApply(input.Apply)
	if err != nil || !found {
		return nil, nil, err
	}
	stepID := input.AllocateStep()
	step, err := apply.ExecutionStep(stepID, input.TimeoutSeconds)
	if err != nil {
		return nil, nil, err
	}
	return []*agentpb.ExecutionStep{
			step,
		}, []taskjournal.TaskStepRecord{
			{Kind: taskjournal.TaskStepOperation, ID: stepID},
		}, nil
}

// AppendEnvironmentComponentTaskContribution preserves the deploy invariant
// that runtime reconciliation completes before managed configuration activates.
func AppendEnvironmentComponentTaskContribution(
	steps []*agentpb.ExecutionStep,
	records []taskjournal.TaskStepRecord,
	contribution []*agentpb.ExecutionStep,
	contributionRecords []taskjournal.TaskStepRecord,
) ([]*agentpb.ExecutionStep, []taskjournal.TaskStepRecord, error) {
	if len(contribution) != len(contributionRecords) {
		return nil, nil, errs.New(errs.KindInternal, "Component task contribution is inconsistent")
	}
	if len(contribution) == 0 {
		return steps, records, nil
	}
	if len(steps) == 0 || steps[len(steps)-1].GetComposeApply() == nil {
		return nil, nil, errs.New(
			errs.KindInternal,
			"managed configuration must follow Compose apply",
		)
	}
	return append(steps, contribution...), append(records, contributionRecords...), nil
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
		if hasMaterialization {
			return EnvironmentManagedConfigApply{}, false, errs.New(
				errs.KindInternal,
				"Blueprint managed configuration materialization has no candidate Component",
			)
		}
		return EnvironmentManagedConfigApply{}, false, nil
	}
	if input.Artifact.GetOwnerId() != candidate.Desired.OwnerID {
		return EnvironmentManagedConfigApply{}, false, errs.New(
			errs.KindInternal,
			"Blueprint managed-config candidate and artifact owners differ",
		)
	}
	if !candidate.Desired.Enabled {
		if hasMaterialization {
			return EnvironmentManagedConfigApply{}, false, errs.New(
				errs.KindInternal,
				"disabled Blueprint managed-config candidate has a managed configuration materialization",
			)
		}
		return EnvironmentManagedConfigApply{}, false, nil
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
	for _, component := range components {
		registration, registered := environmentComponentRegistration(catalog, component.Desired.Kind)
		if !registered || registration.ManagedConfiguration == nil {
			continue
		}
		if found {
			return componentrecord.Record{}, componentrender.EnvironmentComponentRegistration{}, false, errs.New(
				errs.KindInternal,
				"Blueprint managed-config provider is duplicated",
			)
		}
		if ids.Validate(ids.KindComponent, component.Desired.ID) != nil ||
			component.Desired.Owner != core.ComponentOwnerEnvironment ||
			ids.Validate(ids.KindEnvironment, component.Desired.OwnerID) != nil {
			return componentrecord.Record{}, componentrender.EnvironmentComponentRegistration{}, false, errs.New(
				errs.KindInternal,
				"Blueprint managed-config candidate identity is invalid",
			)
		}
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
		if componentFile == nil || !claimsManagedConfiguration(
			componentFile.ComponentID,
			componentFile.Path,
			componentID,
			managed,
			catalog,
		) {
			continue
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
	referenceComponentID string,
	referencePath string,
	componentID string,
	managed *componentrender.EnvironmentManagedConfigurationRegistration,
	catalog []componentrender.EnvironmentComponentRegistration,
) bool {
	if managed != nil {
		return referenceComponentID == componentID && referencePath == managed.SourcePath
	}
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
		componentFile.Path != managed.SourcePath || componentFile.RouteTaskID != "" {
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
