package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) validateRunnerTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) error {
	applies, err := taskOwnsRunner(task)
	if err != nil || !applies {
		return err
	}
	if task.Status != terminalStatus || !runnerTerminal(terminalStatus) {
		return errs.New(errs.KindInternal, "runner terminal task has invalid durable input")
	}
	if task.Type == taskjournal.TaskCreate {
		return repository.validateRunnerCreationAcknowledgementReplay(ctx, task, terminalStatus, revision)
	}
	return repository.validateRunnerRemovalAcknowledgementReplay(ctx, task, terminalStatus, revision)
}

func (repository *TaskRepository) validateRunnerCreationAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) error {
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			runnerrecord.RunnerKey(task.Target),
			runnerrecord.RunnerLifecycleKey(task.Target),
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetRunner), task.Target),
		},
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if stored == nil || len(stored.Values) != 3 || stored.Values[0] == nil || stored.Values[1] == nil ||
		stored.Values[2] != nil {
		return errs.New(errs.KindStateConflict, "runner creation replay evidence is incomplete")
	}
	record, err := runnerrecord.DecodeRunnerAggregate(stored.Values[0], stored.Values[1])
	if err != nil || record.Desired.ID != task.Target || record.CreateTaskID != task.ID {
		return errs.New(errs.KindStateConflict, "runner creation replay retained corrupt target state")
	}
	wantState := runnerrecord.RunnerProvisioningFailed
	if terminalStatus == taskjournal.TaskStatusCompleted {
		wantState = runnerrecord.RunnerProvisioningReady
	}
	if record.ProvisioningState != wantState {
		return errs.New(errs.KindStateConflict, "runner creation replay target state changed")
	}
	_, err = (composeRunnerRepository(repository.store)).ReadRunnerAllocationEvidence(ctx, record, revision)
	return err
}

func (repository *TaskRepository) validateRunnerRemovalAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) error {
	evidence, err := decodeRunnerRemovalTaskEvidence(task)
	if err != nil {
		return err
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			runnerrecord.RunnerKey(task.Target),
			runnerrecord.RunnerLifecycleKey(task.Target),
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetRunner), task.Target),
			runnerrecord.RunnerRemovalIntentKey(task.Target),
			runnerrecord.RunnerObservationKey(task.Target),
			runnerrecord.RunnerOwnerKey(evidence.ownerKind, evidence.ownerID, task.Target),
			runnerrecord.RunnerTenantQuotaKey(evidence.tenantID),
			runnerrecord.RunnerHostSlotKey(evidence.hostSlot),
			runnerrecord.SystemPoolRegistryKey,
			runnerrecord.RunnerRuntimeOwnershipKey(task.Target),
		},
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if stored == nil || len(stored.Values) != 10 {
		return errs.New(errs.KindInternal, "runner removal replay evidence is incomplete")
	}
	if stored.Values[2] != nil || stored.Values[3] != nil {
		return errs.New(errs.KindStateConflict, "runner removal replay found a live cleanup fence")
	}
	if stored.Values[6] == nil || stored.Values[8] == nil {
		return errs.New(errs.KindStateConflict, "runner removal replay lost allocation registries")
	}
	quota, err := runnerrecord.DecodeRunnerTenantQuota(stored.Values[6].Value)
	if err != nil || quota.Validate() != nil {
		return runnerrecord.CorruptRunnerTenantQuota()
	}
	system, err := runnerrecord.DecodeSystemPoolRegistry(stored.Values[8].Value)
	if err != nil || system.Reservations == nil {
		return runnerrecord.CorruptSystemPoolRegistry()
	}
	if terminalStatus == taskjournal.TaskStatusCompleted {
		if stored.Values[0] != nil || stored.Values[1] != nil || stored.Values[4] != nil || stored.Values[5] != nil ||
			stored.Values[7] != nil || stored.Values[9] != nil {
			return errs.New(errs.KindStateConflict, "completed runner removal retained target state")
		}
		index := runnerrecord.SortSearchRunnerID(quota.RunnerIDs, task.Target)
		if (index < len(quota.RunnerIDs) && quota.RunnerIDs[index] == task.Target) ||
			system.Reservations[runnerallocation.RunnerReservationOwner(task.Target)] != "" {
			return errs.New(errs.KindStateConflict, "completed runner removal retained allocation ownership")
		}
		return nil
	}
	if stored.Values[0] == nil || stored.Values[1] == nil || stored.Values[5] == nil || stored.Values[7] == nil {
		return errs.New(errs.KindStateConflict, "failed runner removal lost target state")
	}
	record, err := runnerrecord.DecodeRunnerAggregate(stored.Values[0], stored.Values[1])
	if err != nil || record.Desired.ID != task.Target || !evidence.matchesRecord(record) {
		return errs.New(errs.KindStateConflict, "failed runner removal retained corrupt target state")
	}
	slug, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{runnerrecord.RunnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug)},
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if slug == nil || len(slug.Values) != 1 {
		return errs.New(errs.KindInternal, "runner removal replay slug evidence is incomplete")
	}
	return runnerrecord.RunnerAllocationEvidenceOwns(record, runnerrecord.RunnerAllocationEvidence{
		Owner:  stored.Values[5],
		Slug:   slug.Values[0],
		Quota:  stored.Values[6],
		Host:   stored.Values[7],
		System: stored.Values[8],
	})
}
