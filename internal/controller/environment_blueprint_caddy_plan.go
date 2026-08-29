package controller

import (
	"crypto/sha256"
	"encoding/hex"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type EnvironmentCaddyApplyInput struct {
	RevisionID       string
	RenderGeneration uint64
	Components       []etcd.ComponentRecord
	ComponentCatalog []EnvironmentComponentRegistration
	Materializations []etcd.TaskMaterializationRecord
	Artifact         *agentpb.ComposeArtifact
}

type EnvironmentCaddyApply struct {
	action *agentpb.ComponentApply
}

func ResolveEnvironmentCaddyApply(
	input EnvironmentCaddyApplyInput,
) (EnvironmentCaddyApply, bool, error) {
	if ids.Validate(ids.KindTask, input.RevisionID) != nil || input.RenderGeneration == 0 || input.Artifact == nil ||
		ids.Validate(ids.KindConfig, input.Artifact.GetArtifactId()) != nil ||
		input.Artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		ids.Validate(ids.KindEnvironment, input.Artifact.GetOwnerId()) != nil {
		return EnvironmentCaddyApply{}, false, errs.New(
			errs.KindInternal,
			"Blueprint Caddy apply input is invalid",
		)
	}

	caddy, hasCaddy, err := resolveEnvironmentCaddyCandidate(input.Components)
	if err != nil {
		return EnvironmentCaddyApply{}, false, err
	}
	materializationIndex, hasMaterialization, err := resolveEnvironmentCaddyMaterialization(
		input.Materializations,
	)
	if err != nil {
		return EnvironmentCaddyApply{}, false, err
	}
	if !hasCaddy {
		if hasMaterialization {
			return EnvironmentCaddyApply{}, false, errs.New(
				errs.KindInternal,
				"Blueprint Caddyfile materialization has no candidate Component",
			)
		}
		return EnvironmentCaddyApply{}, false, nil
	}
	if input.Artifact.GetOwnerId() != caddy.Desired.OwnerID {
		return EnvironmentCaddyApply{}, false, errs.New(
			errs.KindInternal,
			"Blueprint Caddy candidate and artifact owners differ",
		)
	}
	if !caddy.Desired.Enabled {
		if hasMaterialization {
			return EnvironmentCaddyApply{}, false, errs.New(
				errs.KindInternal,
				"disabled Blueprint Caddy candidate has a Caddyfile materialization",
			)
		}
		return EnvironmentCaddyApply{}, false, nil
	}

	serviceID := caddy.Runtime.GeneratedServices[0]
	if err := requireEnvironmentCaddyArtifactService(input.Artifact, serviceID); err != nil {
		return EnvironmentCaddyApply{}, false, err
	}
	if !hasMaterialization {
		return EnvironmentCaddyApply{}, false, nil
	}
	digest, err := validateEnvironmentCaddyMaterialization(
		input.Materializations[materializationIndex],
		input.RevisionID,
		caddy,
	)
	if err != nil {
		return EnvironmentCaddyApply{}, false, err
	}
	reference := input.Materializations[materializationIndex]
	action, err := BuildEnvironmentComponentAction(
		input.ComponentCatalog,
		core.ComponentKindIngressCaddy,
		caddy.Desired.ID,
		componentsdk.ActionID("activate-config"),
		reference.MaterializationID,
		digest,
		input.RenderGeneration,
	)
	if err != nil {
		return EnvironmentCaddyApply{}, false, err
	}
	return EnvironmentCaddyApply{action: action}, true, nil
}

func resolveEnvironmentCaddyCandidate(
	components []etcd.ComponentRecord,
) (etcd.ComponentRecord, bool, error) {
	var caddy etcd.ComponentRecord
	found := false
	for _, component := range components {
		if component.Desired.Kind != core.ComponentKindIngressCaddy {
			continue
		}
		if found {
			return etcd.ComponentRecord{}, false, errs.New(
				errs.KindInternal,
				"Blueprint Caddy candidate is duplicated",
			)
		}
		if ids.Validate(ids.KindComponent, component.Desired.ID) != nil ||
			component.Desired.Owner != core.ComponentOwnerEnvironment ||
			ids.Validate(ids.KindEnvironment, component.Desired.OwnerID) != nil {
			return etcd.ComponentRecord{}, false, errs.New(
				errs.KindInternal,
				"Blueprint Caddy candidate identity is invalid",
			)
		}
		if component.Desired.Enabled {
			if len(component.Runtime.GeneratedServices) != 1 ||
				ids.Validate(ids.KindService, component.Runtime.GeneratedServices[0]) != nil {
				return etcd.ComponentRecord{}, false, errs.New(
					errs.KindInternal,
					"enabled Blueprint Caddy candidate Service identity is invalid",
				)
			}
		} else if len(component.Runtime.GeneratedServices) != 0 {
			return etcd.ComponentRecord{}, false, errs.New(
				errs.KindInternal,
				"disabled Blueprint Caddy candidate has a generated Service",
			)
		}
		caddy = component
		found = true
	}
	return caddy, found, nil
}

func resolveEnvironmentCaddyMaterialization(
	materializations []etcd.TaskMaterializationRecord,
) (int, bool, error) {
	selected := -1
	for index := range materializations {
		reference := materializations[index]
		componentFile := reference.Source.ComponentFile
		claimsCaddyfile := reference.Destination == RouteRemovalCaddyfilePath ||
			componentFile != nil && componentFile.Path == RouteRemovalCaddyfilePath
		if !claimsCaddyfile {
			continue
		}
		if selected >= 0 {
			return -1, false, errs.New(
				errs.KindInternal,
				"Blueprint Caddyfile materialization is duplicated",
			)
		}
		selected = index
	}
	return selected, selected >= 0, nil
}

func requireEnvironmentCaddyArtifactService(
	artifact *agentpb.ComposeArtifact,
	serviceID string,
) error {
	matches := 0
	for _, service := range artifact.GetServices() {
		if service == nil {
			return errs.New(errs.KindInternal, "Blueprint Caddy artifact Service is invalid")
		}
		if service.GetServiceId() == serviceID {
			matches++
		}
	}
	if matches != 1 {
		return errs.New(
			errs.KindInternal,
			"Blueprint Caddy generated Service is not uniquely present in the artifact",
		)
	}
	return nil
}

func validateEnvironmentCaddyMaterialization(
	reference etcd.TaskMaterializationRecord,
	revisionID string,
	caddy etcd.ComponentRecord,
) ([sha256.Size]byte, error) {
	componentFile := reference.Source.ComponentFile
	if ids.Validate(ids.KindStep, reference.StepID) != nil ||
		ids.Validate(ids.KindConfig, reference.MaterializationID) != nil ||
		reference.EnvironmentID != caddy.Desired.OwnerID ||
		reference.Destination != RouteRemovalCaddyfilePath ||
		reference.ServiceID != "" || reference.ServiceName != "" ||
		reference.OutputKind != etcd.TaskMaterializationOutputPlainFile ||
		reference.UID != 0 || reference.GID != 0 ||
		reference.Mode != uint32(entrymaterialization.ModeReadOnly) ||
		reference.Source.Kind != etcd.TaskMaterializationSourceComponentFile ||
		componentFile == nil || reference.Source.BlueprintFile != nil ||
		reference.Source.EntryValue != nil || reference.Source.GeneratedEnvironment != nil ||
		componentFile.RevisionID != revisionID ||
		componentFile.ComponentID != caddy.Desired.ID ||
		componentFile.Path != RouteRemovalCaddyfilePath || componentFile.RouteRemovalTaskID != "" {
		return [sha256.Size]byte{}, errs.New(
			errs.KindInternal,
			"Blueprint Caddyfile materialization identity is invalid",
		)
	}
	decoded, err := hex.DecodeString(reference.SHA256)
	if err != nil || len(decoded) != sha256.Size {
		return [sha256.Size]byte{}, errs.New(
			errs.KindInternal,
			"Blueprint Caddyfile materialization digest is invalid",
		)
	}
	var digest [sha256.Size]byte
	copy(digest[:], decoded)
	return digest, nil
}

func (apply EnvironmentCaddyApply) ExecutionStep(
	stepID string,
	timeoutSeconds uint32,
	prerequisiteStepID string,
) (*agentpb.ExecutionStep, error) {
	if ids.Validate(ids.KindStep, stepID) != nil || ids.Validate(ids.KindStep, prerequisiteStepID) != nil ||
		timeoutSeconds == 0 || apply.action == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Caddy apply step is invalid")
	}
	return &agentpb.ExecutionStep{
		StepId: stepID, TimeoutSeconds: timeoutSeconds, PrerequisiteStepId: prerequisiteStepID,
		Payload: &agentpb.ExecutionStep_ComponentApply{
			ComponentApply: proto.Clone(apply.action).(*agentpb.ComponentApply),
		},
	}, nil
}
