package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// captureDatabaseGuards checks the captured Attach and backing ownership at the
// dump-start grant. Later result/cleanup reports must remain admissible after
// desired state changes; these guards therefore do not apply to settlement.
func (repository *InputRepository) captureDatabaseGuards(ctx context.Context,
	source backupruntime.BackupRunSourceAttemptRecord, revision int64,
) ([]keyvalue.Condition, error) {
	snapshot, err := backupplanning.SelectBackupDatabasePublicationSnapshot(source)
	if err != nil {
		return nil, err
	}
	keys := []string{attachments.AttachKey(source.TargetID), attachments.AttachFactsKey(source.TargetID),
		hierarchy.ProjectKey(snapshot.BackingProjectID), hierarchy.EnvironmentKey(snapshot.BackingEnvironmentID),
		blueprints.EnvironmentBlueprintHeadKey(snapshot.BackingEnvironmentID)}
	read, err := repository.readSettlementKeys(ctx, keys, revision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if err := backupplanning.ValidateBackupDatabasePublicationEvidence(read.Values, source, snapshot); err != nil {
		return nil, err
	}
	guards := make([]keyvalue.Condition, len(keys))
	for index, key := range keys {
		guards[index] = keyvalue.Condition{Key: key, ModRevision: read.Values[index].ModRevision}
	}
	return guards, nil
}
