package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"

	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateDNSCheck publishes one platform work item. Captured runtime, latest
// selection and single active-check ownership commit with the idempotency key.
func (repository *TaskRepository) CreateDNSCheck(ctx context.Context, source DNSCheckSource,
	input dnschecks.Record, task workexecution.Task, marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if source.readRevision <= 0 || input.Validate() != nil || input.Phase != dnschecks.Ready ||
		task.Validate() != nil || task.Scope != (workexecution.Scope{}) || task.Action != workexecution.ActionCheck ||
		task.Actor != workexecution.OperatorActor || task.ExpectedItems != 1 || task.PublicationComplete || task.Progress.Total() != 0 ||
		input.Owner.TaskID != task.ID || input.Owner.PlanID != task.Intent.ID || input.Source.ComponentID != task.TargetID ||
		!input.CreatedAt.Equal(task.CreatedAt) || input.Source != source.configuration ||
		marker.Locator.ScopeKind != idempotency.IdempotencyScopePlatform || marker.Locator.ScopeID != "-" ||
		marker.TaskID != task.ID || marker.Kind != idempotency.IdempotencyMarkerTask || marker.State != idempotency.IdempotencyMarkerPending ||
		!marker.CreatedAt.Equal(task.CreatedAt) || !marker.UpdatedAt.Equal(task.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"DNS check publication differs from its captured input",
		)
	}
	digest, err := dnschecks.PlanHash(input)
	if err != nil || digest != task.Intent.SHA256 || idempotency.ValidateIdempotencyMarker(marker) != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "DNS check input or request replay differs from publication")
	}
	request, err := input.Input()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	expected, err := source.Request(request.Action, request.Queries)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	rebuilt, err := dnschecks.NewRecord(input.Owner, source.configuration, expected, input.CreatedAt)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if rebuilt.ConfigurationSHA256 != input.ConfigurationSHA256 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"DNS check runtime differs from its repository capture",
		)
	}
	conditions, mutations, err := dnschecks.PreparePublication(input)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(mutations)
	item := workexecution.Item{ID: input.Owner.WorkItemID, TaskID: task.ID, Operation: workexecution.DNSCheck,
		TargetID: task.TargetID, Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims: []workexecution.Claim{{HostID: source.configuration.HostID, ScopeID: task.TargetID}}, Input: &task.Intent,
		Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline}
	work, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(work.Mutations)
	conditions = append(conditions, source.conditions...)
	conditions = append(conditions, work.Conditions...)
	mutations = append(mutations, work.Mutations...)
	plan, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask, conditions, mutations,
		func(_ int64, values []*keyvalue.KeyValue) error {
			if len(values) != len(conditions) {
				return errs.New(errs.KindInternal, "DNS check publication evidence is incomplete")
			}
			for index, condition := range conditions {
				if keyvalue.RevisionOf(values[index]) != condition.ModRevision {
					return errs.New(
						errs.KindStateConflict,
						"CoreDNS, its Agent or another check changed before acceptance; refresh and submit a new check",
					)
				}
			}
			return nil
		})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replay, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return replay.Apply(ctx, marker, plan)
}
