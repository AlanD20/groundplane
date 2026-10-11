package runtime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
)

func (adapter *localAgentRepositoryAdapter) RecordReplacementFailure(ctx context.Context,
	current localagent.StoredRecord, failure *taskprogress.Failure,
) (localagent.StoredRecord, error) {
	record, err := localAgentRecordToDurable(current.Record)
	if err != nil {
		return localagent.StoredRecord{}, err
	}
	stored, err := adapter.repository.RecordReplacementFailure(
		ctx,
		etcdstore.Versioned[localagents.LocalAgentRecord]{
			Record:       record,
			Revision:     current.Revision,
			ReadRevision: current.Revision,
		},
		failure,
	)
	if err != nil {
		return localagent.StoredRecord{}, err
	}
	return localAgentRecordFromDurable(stored)
}
