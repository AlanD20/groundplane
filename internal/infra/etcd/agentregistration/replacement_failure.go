package agentregistration

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RecordReplacementFailure retains the first cause on the exact candidate
// generation before rollback preparation can fail or the Controller can exit.
// It changes no lifecycle authority, credential, configuration or Ready evidence.
func (repository *Repository) RecordReplacementFailure(ctx context.Context,
	current etcdstore.Versioned[localagents.LocalAgentRecord], failure *taskprogress.Failure,
) (etcdstore.Versioned[localagents.LocalAgentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[localagents.LocalAgentRecord]{}, err
	}
	if failure == nil || failure.Validate() != nil || failure.ExitCode != 0 ||
		current.Revision <= 0 || current.Record.Generation < 2 {
		return etcdstore.Versioned[localagents.LocalAgentRecord]{}, errs.New(errs.KindValidationFailed,
			"agent replacement failure observation is invalid")
	}
	evidence, err := repository.readSingleton(ctx)
	if err != nil {
		return etcdstore.Versioned[localagents.LocalAgentRecord]{}, err
	}
	record := evidence.record
	if record.ID != current.Record.ID || record.Generation != current.Record.Generation ||
		record.Image != current.Record.Image || record.Phase != current.Record.Phase ||
		evidence.primaryRevision != current.Revision ||
		record.Phase != localagents.LocalAgentPhaseUpdating && record.Phase != localagents.LocalAgentPhaseReady {
		return etcdstore.Versioned[localagents.LocalAgentRecord]{}, errs.New(errs.KindStateConflict,
			"agent candidate changed before its failure was recorded")
	}
	if record.ReplacementFailure != nil {
		return etcdstore.Versioned[localagents.LocalAgentRecord]{Record: record,
			Revision: evidence.primaryRevision, ReadRevision: evidence.readRevision}, nil
	}
	record.ReplacementFailure = taskprogress.CloneFailure(failure)
	value, err := localagents.EncodeLocalAgentPrimary(record)
	if err != nil {
		return etcdstore.Versioned[localagents.LocalAgentRecord]{}, err
	}
	defer clear(value)
	key := localagents.LocalAgentPrimaryKey(record.ID)
	result, err := repository.store.Transact(ctx, []etcdstore.Condition{
		{Key: localagents.LocalAgentSingletonKey, ModRevision: evidence.singleton.ModRevision},
		{Key: key, ModRevision: evidence.primaryRevision},
	}, []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: value}})
	if err != nil {
		return etcdstore.Versioned[localagents.LocalAgentRecord]{}, err
	}
	etcdstore.ClearValues(result.FailureReads)
	if !result.Succeeded {
		return etcdstore.Versioned[localagents.LocalAgentRecord]{}, errs.New(errs.KindStateConflict,
			"agent candidate changed while recording its failure")
	}
	return etcdstore.Versioned[localagents.LocalAgentRecord]{Record: record,
		Revision: result.Revision, ReadRevision: result.Revision}, nil
}
