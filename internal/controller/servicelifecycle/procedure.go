package servicelifecycle

import (
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Procedure is the complete ordered operation selected from captured runtime.
// Each member has a stable checkpoint identity; hooks are separately granted,
// never silently replayed when a container operation resumes.
type Procedure struct {
	Action    workexecution.Action
	ServiceID string
	HostID    string
	Members   []RuntimeMember
	Hook      *LifecycleHook
}

type RuntimeMember struct {
	Artifact *agentpb.ComposeArtifact
	Source   *agentpb.ServiceLifecycleSource
}

type LifecycleHook struct {
	StageID    string
	Event      backinghook.Event
	Definition backinghook.Definition
}

// BuildProcedure does not consult desired state, Task history, or the host.
// Repeated construction from one capture produces the same member/stage IDs.
func BuildProcedure(input servicelifecyclework.Input) (Procedure, error) {
	if err := input.Validate(); err != nil {
		return Procedure{}, err
	}
	procedure := Procedure{Action: input.Action, ServiceID: input.ServiceID, HostID: input.HostID}
	if input.Runtime == nil {
		return procedure, nil
	}
	runtime := input.Runtime
	var encoded [][]byte
	if runtime.NativeBacking != nil {
		encoded = [][]byte{runtime.NativeBacking.ComposeArtifact}
	} else {
		encoded = [][]byte{runtime.AcknowledgedRuntime.Runtime.CurrentArtifact, runtime.AcknowledgedRuntime.Runtime.RetainedPriorArtifact}
	}
	artifacts, err := SplitRuntimeArtifacts(input.TaskID, input.CreatedAt, input.ServiceID, encoded)
	if err != nil {
		return Procedure{}, err
	}
	for _, artifact := range artifacts {
		if artifact.GetOwnerId() != input.Scope.EnvironmentID ||
			artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
			artifact.GetAuthorizedVolumeDir() != runtime.AuthorizedVolumeDir {
			return Procedure{}, errs.New(errs.KindStateConflict, "Service lifecycle member belongs to a different Environment")
		}
		stageID := ids.DeriveAt(ids.KindStep, input.CreatedAt, input.WorkItemID, artifact.Services[0].ComposeName)
		source, err := RuntimeSource(artifact, stageID)
		if err != nil {
			return Procedure{}, err
		}
		procedure.Members = append(procedure.Members, RuntimeMember{Artifact: artifact, Source: source})
	}
	if runtime.HookConfiguration != nil {
		configuration := backinghook.CloneConfiguration(runtime.HookConfiguration)
		definition, event := configuration.BeforeStop, backinghook.BeforeStop
		if input.Action == workexecution.ActionStart {
			definition, event = configuration.AfterStart, backinghook.AfterStart
		}
		if definition == nil {
			return Procedure{}, errs.New(errs.KindStateConflict, "Service lifecycle hook is missing from its capture")
		}
		procedure.Hook = &LifecycleHook{StageID: ids.DeriveAt(ids.KindStep, input.CreatedAt, input.WorkItemID, "hook", string(event)), Event: event, Definition: *definition}
	}
	return procedure, nil
}

// StageIDs expresses the real execution order. BeforeStop runs before any host
// stop/removal; AfterStart runs only after all captured members have started.
func (procedure Procedure) StageIDs() []string {
	stages := make([]string, 0, len(procedure.Members)+1)
	if procedure.Hook != nil && procedure.Hook.Event == backinghook.BeforeStop {
		stages = append(stages, procedure.Hook.StageID)
	}
	for _, member := range procedure.Members {
		stages = append(stages, member.Source.StepId)
	}
	if procedure.Hook != nil && procedure.Hook.Event == backinghook.AfterStart {
		stages = append(stages, procedure.Hook.StageID)
	}
	return stages
}
