package taskplanning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type BlueprintManagedComponentTeardown struct {
	Artifacts []*agentpb.ComposeArtifact
	Steps     []*agentpb.ExecutionStep
	Procedure *agentpb.ManagedComponentProcedure
}

// BlueprintManagedComponentTeardown resolves each conservative per-Component
// runtime source from its immutable Blueprint revision. Every removal stays a
// one-Service Compose rm; no source grants project, dependency, orphan, volume,
// network, or native-Service authority.
func (resolver *TaskPlanResolver) BlueprintManagedComponentTeardown(
	ctx context.Context,
	task etcd.TaskRecord,
	projection projectionrecord.EnvironmentComposeProjection,
	candidate *agentpb.ComposeArtifact,
	prerequisite string,
	releaseForward bool,
) (BlueprintManagedComponentTeardown, error) {
	desiredRevisionID := task.Params[blueprints.EnvironmentDesiredRevisionParam]
	if resolver == nil || resolver.blueprints == nil || ctx == nil || candidate == nil ||
		projection.EnvironmentID != task.Target || ids.Validate(ids.KindTask, desiredRevisionID) != nil ||
		projection.RevisionID != desiredRevisionID ||
		candidate.GetArtifactId() == "" {
		return BlueprintManagedComponentTeardown{}, errs.New(
			errs.KindInternal,
			"Blueprint managed Component teardown input is invalid",
		)
	}
	if err := etcd.ValidateManagedComponentTeardownSources(task.ManagedComponentTeardownSources); err != nil {
		return BlueprintManagedComponentTeardown{}, err
	}
	result := BlueprintManagedComponentTeardown{Artifacts: []*agentpb.ComposeArtifact{candidate}}
	if len(task.ManagedComponentTeardownSources) == 0 {
		return result, nil
	}
	planTime, err := ids.Timestamp(ids.KindPlan, task.PlanID)
	if err != nil {
		return BlueprintManagedComponentTeardown{}, err
	}
	result.Procedure = &agentpb.ManagedComponentProcedure{
		Services: make([]*agentpb.ManagedComponentService, 0, len(task.ManagedComponentTeardownSources)),
	}
	artifacts := map[string]*agentpb.ComposeArtifact{candidate.GetArtifactId(): candidate}
	for _, source := range task.ManagedComponentTeardownSources {
		sourceProjection := projection
		if source.RevisionID != projection.RevisionID {
			loaded, found, loadErr := resolver.blueprints.GetEnvironmentComposeProjectionRevision(
				ctx,
				projection.EnvironmentID,
				source.RevisionID,
			)
			if loadErr != nil {
				return BlueprintManagedComponentTeardown{}, loadErr
			}
			if !found {
				return BlueprintManagedComponentTeardown{}, errs.New(
					errs.KindStateConflict,
					"managed Component runtime source revision is unavailable",
				)
			}
			sourceProjection = loaded.Record
		}
		if sourceProjection.EnvironmentID != projection.EnvironmentID {
			return BlueprintManagedComponentTeardown{}, errs.New(
				errs.KindStateConflict,
				"managed Component runtime source Environment ownership changed",
			)
		}
		sourceArtifact, loadErr := decodeManagedComponentSourceArtifact(sourceProjection, source)
		if loadErr != nil {
			return BlueprintManagedComponentTeardown{}, loadErr
		}
		artifact, exists := artifacts[source.ArtifactID]
		if !exists {
			artifact = sourceArtifact
			artifacts[source.ArtifactID] = artifact
			result.Artifacts = append(result.Artifacts, artifact)
		} else if sourceErr := validateManagedComponentServiceIdentity(artifact, source); sourceErr != nil {
			return BlueprintManagedComponentTeardown{}, sourceErr
		}
		removeStepID := ids.DeriveAt(
			ids.KindStep,
			planTime,
			task.PlanID,
			"blueprint-managed-remove:"+source.ComponentID,
		)
		step := &agentpb.ExecutionStep{
			StepId:             removeStepID,
			PrerequisiteStepId: prerequisite,
			TimeoutSeconds:     uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
				ArtifactId: source.ArtifactID,
				ServiceIds: []string{source.ServiceID},
			}},
		}
		if releaseForward {
			step.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
		}
		result.Steps = append(result.Steps, step)
		result.Procedure.Services = append(result.Procedure.Services, &agentpb.ManagedComponentService{
			ComponentKind:        string(source.ComponentKind),
			ComponentId:          source.ComponentID,
			ServiceId:            source.ServiceID,
			ComposeServiceName:   source.ComposeName,
			SourceRevisionId:     source.RevisionID,
			SourceArtifactId:     source.ArtifactID,
			SourceArtifactSha256: mustDecodeManagedComponentDigest(source.ArtifactSHA256),
			RemoveStepId:         removeStepID,
		})
		prerequisite = removeStepID
	}
	return result, nil
}

func decodeManagedComponentSourceArtifact(
	projection projectionrecord.EnvironmentComposeProjection,
	source projectionrecord.ManagedComponentRuntimeSource,
) (*agentpb.ComposeArtifact, error) {
	if projection.RevisionID != source.RevisionID {
		return nil, errs.New(errs.KindStateConflict, "managed Component source revision identity changed")
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		return nil, errs.New(errs.KindInternal, "managed Component source artifact is corrupt")
	}
	if artifact.GetOwnerId() != projection.EnvironmentID {
		return nil, errs.New(errs.KindStateConflict, "managed Component source artifact ownership changed")
	}
	if err := validateManagedComponentSourceArtifact(artifact, source); err != nil {
		return nil, err
	}
	return artifact, nil
}

func validateManagedComponentSourceArtifact(
	artifact *agentpb.ComposeArtifact,
	source projectionrecord.ManagedComponentRuntimeSource,
) error {
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	wantDigest, digestErr := hex.DecodeString(source.ArtifactSHA256)
	actualDigest := sha256.Sum256(encoded)
	if err != nil || digestErr != nil || !bytes.Equal(actualDigest[:], wantDigest) ||
		artifact.GetArtifactId() != source.ArtifactID {
		return errs.New(errs.KindStateConflict, "managed Component source artifact changed")
	}
	return validateManagedComponentServiceIdentity(artifact, source)
}

func validateManagedComponentServiceIdentity(
	artifact *agentpb.ComposeArtifact,
	source projectionrecord.ManagedComponentRuntimeSource,
) error {
	matched := 0
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() == source.ServiceID && service.GetOwnerComponentId() == source.ComponentID &&
			service.GetComposeName() == source.ComposeName {
			matched++
		}
	}
	if matched != 1 {
		return errs.New(errs.KindStateConflict, "managed Component source Service changed")
	}
	return nil
}

func mustDecodeManagedComponentDigest(value string) []byte {
	digest, _ := hex.DecodeString(value)
	return digest
}
