package componentwork

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// AppliedRuntime builds the independently retained runtime record from the
// captured candidate and its final acknowledged host proof. It does not mutate
// desired state or grant queue completion; publication must join both owners.
func AppliedRuntime(view workqueue.ResumeView, input *agentpb.ComponentWorkInput, capture Capture,
) (componentplanning.Runtime, error) {
	attempt, checkpoint := view.Attempt.Attempt, view.Checkpoint.Record
	if view.Work.Item.Item.Input == nil || executionplan.ValidateComponentWorkInput(input) != nil ||
		capture.matches(view.Work, input) != nil || attempt.Validate() != nil ||
		attempt.Direction != workexecution.Forward || attempt.Input != *view.Work.Item.Item.Input ||
		attempt.WorkItemID != view.Work.Item.Item.ID || attempt.TaskID != view.Work.Task.Task.ID ||
		checkpoint == nil || checkpoint.AttemptID != attempt.ID || checkpoint.Evidence.Validate() != nil {
		return componentplanning.Runtime{}, conflict("Component applied runtime requires its captured completion")
	}
	record := capture.Candidate
	record.Runtime.Healthy = record.Desired.Enabled
	record.Runtime.GeneratedServices = nil
	var artifact []byte
	if record.Desired.Enabled {
		for _, service := range input.Candidate.Services {
			if service.GetOwnerComponentId() != input.ComponentId {
				return componentplanning.Runtime{}, conflict("Component candidate includes another owner's workload")
			}
			record.Runtime.GeneratedServices = append(record.Runtime.GeneratedServices, service.ServiceId)
		}
		var err error
		artifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(input.Candidate)
		if err != nil {
			return componentplanning.Runtime{}, err
		}
	} else {
		record.Runtime.PinnedIPv4 = ""
	}
	runtime := componentplanning.Runtime{EnvironmentID: input.EnvironmentId, Component: record,
		RevisionID: attempt.TaskID, RenderGeneration: input.RenderGeneration, Artifact: artifact,
		Zones: capture.Zones, ProxyPreflight: capture.Proxy,
		Execution: blueprintunits.AppliedExecution{TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID,
			AttemptID: attempt.ID, Executor: attempt.Executor, Input: attempt.Input, Result: checkpoint.Evidence}}
	if err := componentplanning.ValidateRuntime(runtime); err != nil {
		clear(artifact)
		return componentplanning.Runtime{}, err
	}
	return runtime, nil
}
