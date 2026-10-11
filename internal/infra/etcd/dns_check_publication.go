package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateDNSCheck uses native platform Task publication. Captured runtime, latest
// selection and single active-check ownership commit with the idempotency key.
func (repository *TaskRepository) CreateDNSCheck(ctx context.Context, source DNSCheckSource,
	input dnschecks.Record, task TaskRecord, marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if source.readRevision <= 0 || input.Validate() != nil || input.Phase != dnschecks.Ready ||
		!dnsCheckMatchesTask(input, task) || task.Status != taskjournal.TaskStatusPending ||
		!input.CreatedAt.Equal(task.CreatedAt) || input.Source != source.configuration ||
		marker.Locator.ScopeKind != idempotency.IdempotencyScopePlatform || marker.Locator.ScopeID != "-" ||
		marker.Locator.Key != task.IdempotencyKey {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"DNS check publication differs from its captured input",
		)
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
	conditions = append(slices.Clone(source.conditions), conditions...)
	return repository.createTask(ctx, task, marker, conditions, mutations,
		func(_ int64, values []*keyvalue.KeyValue) error {
			// createTask prepends its four native Task absence comparisons.
			if len(values) != len(conditions)+4 {
				return errs.New(errs.KindInternal, "DNS check publication evidence is incomplete")
			}
			for _, value := range values[:4] {
				if value != nil {
					return errs.New(errs.KindStateConflict, "DNS check operation has already been accepted")
				}
			}
			for index, condition := range conditions {
				if keyvalue.RevisionOf(values[index+4]) != condition.ModRevision {
					return errs.New(
						errs.KindStateConflict,
						"CoreDNS, its Agent or another check changed before acceptance; refresh and submit a new check",
					)
				}
			}
			return nil
		})
}
