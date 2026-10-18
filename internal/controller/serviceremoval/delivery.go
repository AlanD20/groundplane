// Package serviceremoval coordinates captured runtime removal and subsequent
// Service definition settlement. It does not rebuild runtime from desired state.
package serviceremoval

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BuildHostInput uses the same bounded member-removal machine contract as
// Destroy runtime. The Service-removal owner additionally requires definition
// settlement; a host receipt alone cannot complete the visible action.
func BuildHostInput(input serviceremovalwork.Input) (*agentpb.ServiceLifecycleWorkInput, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	if input.Runtime == nil {
		return nil, errs.New(errs.KindStateConflict, "undeployed Service removal has no host work")
	}
	ref, err := input.Reference()
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(ref.SHA256)
	if err != nil {
		return nil, errs.New(errs.KindInternal, "Service removal capture digest is invalid")
	}
	runtime := input.Runtime.Runtime
	artifacts, err := servicelifecycle.SplitRuntimeArtifacts(input.TaskID, input.CreatedAt, input.ServiceID,
		[][]byte{runtime.CurrentArtifact, runtime.RetainedPriorArtifact})
	if err != nil {
		return nil, err
	}
	result := &agentpb.ServiceLifecycleWorkInput{
		CapturedSource: &agentpb.WorkInputReference{InputId: ref.ID, Sha256: digest},
		EnvironmentId:  input.Scope.EnvironmentID, ServiceId: input.ServiceID,
		Action: agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_DESTROY,
	}
	for _, artifact := range artifacts {
		if artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT || artifact.OwnerId != input.Scope.EnvironmentID {
			return nil, errs.New(errs.KindStateConflict, "Service removal member belongs to another Environment")
		}
		step := ids.DeriveAt(ids.KindStep, input.CreatedAt, input.WorkItemID, artifact.Services[0].ComposeName)
		ownership, err := servicelifecycle.RuntimeSource(artifact, step)
		if err != nil {
			return nil, err
		}
		result.Members = append(result.Members, &agentpb.ServiceLifecycleMember{Artifact: artifact, Ownership: ownership})
	}
	if err := executionplan.ValidateServiceLifecycleWorkInput(result); err != nil {
		return nil, err
	}
	return result, nil
}
