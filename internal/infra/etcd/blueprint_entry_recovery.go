package etcd

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *TaskRepository) authoredEntryRecoveryProcedureAtRevision(
	ctx context.Context, task TaskRecord, revision int64,
) (*agentpb.CandidateReleaseProcedure, []keyvalue.Condition, error) {
	if !authoredBlueprintEntryRemovalTask(task) || task.Configuration == nil || len(task.Materializations) == 0 {
		return nil, nil, taskassignments.CorruptTaskAssignment()
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{environmentchanges.EntryRemovalIntentKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, nil, taskassignments.CorruptTaskAssignment()
	}
	intent, err := environmentchanges.DecodeEntryRemovalIntent(read.Values[0].Value)
	if err != nil || intent.Authored == nil || intent.Status != taskjournal.TaskStatusPending ||
		validateEntryRemovalTaskOwner(task, intent) != nil {
		return nil, nil, taskassignments.CorruptTaskAssignment()
	}
	configuration := &agentpb.ConfigurationRestoration{}
	if task.Configuration.Prior != nil {
		configuration.PriorSnapshotId = task.Configuration.Prior.ID
		configuration.PriorSnapshotSha256, err = hex.DecodeString(task.Configuration.Prior.SHA256)
		if err != nil || len(configuration.PriorSnapshotSha256) != 32 {
			return nil, nil, taskassignments.CorruptTaskAssignment()
		}
	}
	for _, file := range task.Materializations {
		probeID := ids.DeriveAt(ids.KindStep, task.CreatedAt, task.PlanID, "configuration-probe/"+file.StepID)
		compensateID := ids.DeriveAt(ids.KindStep, task.CreatedAt, task.PlanID, "configuration-restore/"+file.StepID)
		configuration.Files = append(configuration.Files, &agentpb.ConfigurationFileRestoration{
			ForwardStepId: file.StepID, ProbeStepId: probeID, CompensateStepId: compensateID,
		})
	}
	procedure := &agentpb.CandidateReleaseProcedure{ConfigurationRestoration: configuration}
	steps := make(map[string]struct{}, len(task.Steps))
	for _, step := range task.Steps {
		steps[step.ID] = struct{}{}
	}
	if validateTaskConfigurationProcedure(task, procedure, steps) != nil {
		return nil, nil, taskassignments.CorruptTaskAssignment()
	}
	return procedure, []keyvalue.Condition{{
		Key: environmentchanges.EntryRemovalIntentKey(task.ID), ModRevision: read.Values[0].ModRevision,
	}}, nil
}

func (repository *TaskRepository) prepareAuthoredEntryRestorationAuthority(
	ctx context.Context, task TaskRecord, revision int64,
) (taskassignments.ReleaseRestorationAuthority, string, []keyvalue.Condition, error) {
	_, conditions, err := repository.authoredEntryRecoveryProcedureAtRevision(ctx, task, revision)
	if err != nil {
		return taskassignments.ReleaseRestorationAuthority{}, "", nil, err
	}
	authority := taskassignments.ReleaseRestorationAuthority{
		Schema: 1, TaskID: task.ID, OperationID: task.OperationID,
		PlanHash: task.PlanHash, EnvironmentID: task.Owner.EnvironmentID,
	}
	digest, err := taskassignments.ReleaseRestorationAuthoritySHA256(authority)
	if err != nil {
		return taskassignments.ReleaseRestorationAuthority{}, "", nil, errs.Wrap(errs.KindInternal, err)
	}
	return authority, digest, conditions, nil
}

func (repository *TaskRepository) releaseRecoveryProcedureAtRevision(
	ctx context.Context, task TaskRecord, revision int64,
) (*agentpb.CandidateReleaseProcedure, []keyvalue.Condition, error) {
	if authoredBlueprintEntryRemovalTask(task) {
		return repository.authoredEntryRecoveryProcedureAtRevision(ctx, task, revision)
	}
	if authoredBlueprintServiceRemovalTask(task) {
		procedure, _, conditions, err := repository.authoredServiceRecoveryProcedureAtRevision(ctx, task, revision)
		return procedure, conditions, err
	}
	_, procedure, err := repository.candidateReleaseDescriptorAtRevision(ctx, task, revision)
	return procedure, nil, err
}
