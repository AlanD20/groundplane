package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// resourceTaskChange joins one capability's captured input and cleanup ownership
// to native Task terminal/Retry publication. It is not a second journal commit.
type resourceTaskChange struct {
	applies    bool
	conditions []keyvalue.Condition
	mutations  []keyvalue.Mutation
	values     [][]byte
}

func clearResourceTaskChange(change resourceTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}

func (repository *TaskRepository) prepareResourceTaskAcknowledgement(ctx context.Context, task TaskRecord,
	status taskjournal.TaskStatus, revision int64, at time.Time,
) (resourceTaskChange, error) {
	if edgeCheckTask(task) {
		return repository.prepareEdgeCheckTerminal(ctx, task, status, revision, at)
	}
	if runnerCheckTask(task) {
		return repository.prepareRunnerCheckTerminal(ctx, task, status, revision, at)
	}
	if dnsCheckTask(task) {
		return repository.prepareDNSCheckTerminal(ctx, task, status, revision, at)
	}
	if backingCheckTask(task) {
		return repository.prepareBackingCheckTerminal(ctx, task, status, revision, at)
	}
	return repository.prepareConnectorTaskAcknowledgement(ctx, task, status, revision, at)
}

func (repository *TaskRepository) prepareResourceTaskRetry(
	ctx context.Context,
	source, retry TaskRecord,
	revision int64,
) (resourceTaskChange, error) {
	if edgeCheckTask(source) {
		return repository.prepareEdgeCheckRetry(ctx, source, retry, revision)
	}
	if runnerCheckTask(source) {
		return resourceTaskChange{}, errs.New(
			errs.KindStateConflict,
			"Runner diagnostics cannot replay a previous check; submit a new check against the current registered runtime",
		)
	}
	if dnsCheckTask(source) {
		return resourceTaskChange{}, errs.New(
			errs.KindStateConflict,
			"DNS checks cannot replay a previous query; submit a new check against the current CoreDNS configuration",
		)
	}
	if backingCheckTask(source) {
		return repository.prepareBackingCheckRetry(ctx, source, retry, revision)
	}
	return repository.prepareConnectorTaskRetry(ctx, source, retry, revision)
}

func (repository *TaskRepository) validateResourceTaskAcknowledgementReplay(ctx context.Context, task TaskRecord,
	status taskjournal.TaskStatus, revision int64,
) error {
	if edgeCheckTask(task) {
		_, _, err := repository.loadEdgeCheckAt(ctx, task, revision)
		return err
	}
	if runnerCheckTask(task) {
		_, _, err := repository.loadRunnerCheckAt(ctx, task, revision)
		return err
	}
	if dnsCheckTask(task) {
		_, _, err := repository.loadDNSCheckAt(ctx, task, revision)
		return err
	}
	if backingCheckTask(task) {
		record, _, err := repository.loadBackingCheckAt(ctx, task, revision)
		defer record.Clear()
		return err
	}
	return repository.validateConnectorTaskAcknowledgementReplay(ctx, task, status, revision)
}
