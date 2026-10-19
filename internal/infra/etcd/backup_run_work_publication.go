package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// PublishWork consumes the exact resource snapshot once. It joins native work
// admission with the prepared resource transaction; no legacy Task is emitted.
func (publication *PreparedBackupRunPublication) PublishWork(ctx context.Context,
	task workexecution.Task, item workexecution.Item, input *agentpb.BackupWorkInput,
	marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if publication == nil || publication.state == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Backup publication is not prepared")
	}
	state := publication.state
	state.mu.Lock()
	if state.consumed || state.repository == nil {
		state.mu.Unlock()
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"Backup publication was already consumed",
		)
	}
	state.consumed = true
	repository, plan := state.repository, state.plan
	scope, steps, artifacts := state.scope, state.authority, state.artifacts
	state.repository = nil
	state.plan = backupRunPublicationPlan{}
	state.scope, state.authority, state.artifacts = nil, nil, nil
	state.mu.Unlock()
	defer plan.clear()
	if input == nil || scope == nil || input.ProjectId != scope.ProjectId ||
		input.EnvironmentId != scope.EnvironmentId ||
		!proto.Equal(input.Project, scope.Project) ||
		!proto.Equal(input.Environment, scope.Environment) ||
		len(input.Services) != len(scope.Services) ||
		len(input.Steps) != len(steps) ||
		len(input.Artifacts) != len(artifacts) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Backup captured resource scope changed",
		)
	}
	for index, fact := range scope.Services {
		if !proto.Equal(fact, input.Services[index]) {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindValidationFailed,
				"Backup captured Service fact changed",
			)
		}
	}
	for index, step := range steps {
		if !proto.Equal(step, input.Steps[index]) {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindValidationFailed,
				"Backup captured source authority changed",
			)
		}
	}
	for index, artifact := range artifacts {
		if !proto.Equal(artifact, input.Artifacts[index]) {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindValidationFailed,
				"Backup captured artifact changed",
			)
		}
	}
	if item.Placement.HostID != plan.hostID || plan.hostID == "" {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"Backup target host changed after capture",
		)
	}
	mutation, err := plan.workPublication(task, item, input, marker, plan.initiation)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	receipts, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return receipts.Apply(ctx, marker, mutation)
}
