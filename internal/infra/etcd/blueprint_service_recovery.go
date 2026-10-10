package etcd

import (
	"bytes"
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *TaskRepository) authoredServiceRecoveryProcedureAtRevision(
	ctx context.Context, task TaskRecord, revision int64,
) (*agentpb.CandidateReleaseProcedure, environmentchanges.ServiceRemovalIntent, []keyvalue.Condition, error) {
	if !authoredBlueprintServiceRemovalTask(task) || len(task.Steps) < 3 {
		return nil, environmentchanges.ServiceRemovalIntent{}, nil, taskassignments.CorruptTaskAssignment()
	}
	keys := []string{
		environmentchanges.ServiceRemovalIntentKey(task.ID),
		projectionrecord.EnvironmentComposeProjectionStorageKey(task.Owner.EnvironmentID),
		servicerecord.ServiceRuntimeKey(task.Target),
		serviceruntimerecord.Key(task.Target),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, environmentchanges.ServiceRemovalIntent{}, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[3] == nil {
		return nil, environmentchanges.ServiceRemovalIntent{}, nil, taskassignments.CorruptTaskAssignment()
	}
	defer keyvalue.ClearValues(read.Values)
	intent, err := environmentchanges.DecodeServiceRemovalIntent(read.Values[0].Value)
	if err != nil || intent.Authored == nil || intent.Status != taskjournal.TaskStatusPending ||
		validateServiceRemovalTaskOwner(task, intent) != nil ||
		read.Values[1].ModRevision != intent.CurrentProjectionRevision ||
		!keyvalue.ConditionMatchesRead(
			keyvalue.Condition{Key: keys[2], ModRevision: intent.RuntimeRevision}, read.Values[2],
		) || read.Values[3].ModRevision != intent.AcknowledgedRuntimeRevision {
		return nil, environmentchanges.ServiceRemovalIntent{}, nil, taskassignments.CorruptTaskAssignment()
	}
	applied, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[1].Value)
	if err != nil || !environmentchanges.SameServiceRemovalProjection(applied, intent.CurrentProjection) {
		return nil, environmentchanges.ServiceRemovalIntent{}, nil, taskassignments.CorruptTaskAssignment()
	}
	wantRuntime, err := releases.EncodeReleaseRecord("service-acknowledged-runtime", *intent.AcknowledgedRuntime)
	if err != nil || !bytes.Equal(wantRuntime, read.Values[3].Value) {
		clear(wantRuntime)
		return nil, environmentchanges.ServiceRemovalIntent{}, nil, taskassignments.CorruptTaskAssignment()
	}
	clear(wantRuntime)
	recovery, err := environmentchanges.ResolveAuthoredServiceRemovalRecovery(intent)
	if err != nil {
		return nil, environmentchanges.ServiceRemovalIntent{}, nil, taskassignments.CorruptTaskAssignment()
	}
	forwardCount := len(task.Steps) - 2
	physicalMembers := 0
	for _, artifact := range recovery.PriorArtifacts {
		physicalMembers += len(artifact.GetServices())
	}
	if forwardCount != physicalMembers {
		return nil, environmentchanges.ServiceRemovalIntent{}, nil, taskassignments.CorruptTaskAssignment()
	}
	forwardIDs := make([]string, forwardCount)
	for index := range forwardIDs {
		forwardIDs[index] = task.Steps[index].ID
	}
	procedure, err := executionplan.BuildCandidateReleaseProcedure(executionplan.CandidateReleaseProcedureInput{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
		Members: []executionplan.CandidateReleaseMemberInput{{
			ServiceID: intent.ServiceID, CandidateReleaseID: recovery.CandidateReleaseID,
			CandidateArtifactID: recovery.CandidateArtifactID, ForwardStepIDs: forwardIDs,
			ServingPredecessor: &executionplan.ServingPredecessorInput{
				ProbeStepID: task.Steps[forwardCount].ID, CompensateStepID: task.Steps[forwardCount+1].ID,
				PriorArtifactID: recovery.PriorArtifactID, PriorReleaseID: recovery.PriorReleaseID,
				PriorTarget: recovery.PriorTarget, RetainedPriorArtifactID: recovery.RetainedPriorArtifactID,
			},
			CandidateAbsence: &executionplan.CandidateAbsenceInput{
				ComposeProjectName: recovery.ComposeProjectName,
				Services: []executionplan.CandidateServiceIdentity{{
					ServiceID: intent.ServiceID, ReleaseID: recovery.CandidateReleaseID,
				}},
				ProbeStepID: task.Steps[forwardCount].ID, CompensateStepID: task.Steps[forwardCount+1].ID,
			},
		}},
	})
	if err != nil {
		return nil, environmentchanges.ServiceRemovalIntent{}, nil, taskassignments.CorruptTaskAssignment()
	}
	conditions := make([]keyvalue.Condition, len(keys))
	for index, key := range keys {
		conditions[index] = keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])}
	}
	return procedure, environmentchanges.CloneServiceRemovalIntent(intent), conditions, nil
}

func (repository *TaskRepository) prepareAuthoredServiceRestorationAuthority(
	ctx context.Context, task TaskRecord, revision int64,
) (taskassignments.ReleaseRestorationAuthority, string, []keyvalue.Condition, error) {
	procedure, intent, conditions, err := repository.authoredServiceRecoveryProcedureAtRevision(ctx, task, revision)
	if err != nil {
		return taskassignments.ReleaseRestorationAuthority{}, "", nil, err
	}
	member := procedure.GetMembers()[0]
	authority := taskassignments.ReleaseRestorationAuthority{
		Schema: 1, TaskID: task.ID, OperationID: task.OperationID, PlanHash: task.PlanHash,
		EnvironmentID: task.Owner.EnvironmentID, CandidateArtifactID: member.GetCandidateArtifactId(),
		Candidates: []taskassignments.ReleaseRestorationCandidate{{
			ServiceID: member.GetServiceId(), ReleaseID: member.GetCandidateReleaseId(),
			Target: taskassignments.ReleaseRestorationServingPredecessor,
		}},
		NativePredecessors: []taskassignments.ReleaseNativePredecessorAuthority{{
			ServiceID:             task.Target,
			CurrentArtifact:       slices.Clone(intent.AcknowledgedRuntime.Runtime.CurrentArtifact),
			RetainedPriorArtifact: slices.Clone(intent.AcknowledgedRuntime.Runtime.RetainedPriorArtifact),
		}},
	}
	if err := validateNativeRestorationDescriptor(authority, procedure); err != nil {
		return taskassignments.ReleaseRestorationAuthority{}, "", nil, err
	}
	digest, err := taskassignments.ReleaseRestorationAuthoritySHA256(authority)
	if err != nil {
		return taskassignments.ReleaseRestorationAuthority{}, "", nil, errs.Wrap(errs.KindInternal, err)
	}
	return authority, digest, conditions, nil
}
