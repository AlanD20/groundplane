package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) finalizeBlueprintReleaseTaskBatch(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	terminalStatus taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	agentID string,
	terminalAt time.Time,
	readRevision int64,
) (bool, error) {
	publicationID := task.Params[TaskReleasePublicationParam]
	baseKeys := []string{
		releases.ReleasePublicationKey(publicationID), releases.ReleaseManifestStagingKey(publicationID),
		hierarchyrecord.EnvironmentMutationEpochKey(task.Owner.EnvironmentID),
	}
	base, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: baseKeys, Revision: readRevision})
	if err != nil {
		return false, err
	}
	if base == nil || base.ReadRevision != readRevision || len(base.Values) != len(baseKeys) ||
		base.Values[0] == nil || base.Values[1] == nil || base.Values[2] == nil {
		return false, releases.CorruptReleaseRecord()
	}
	marker, err := releases.DecodeReleaseRecord[releases.ReleasePublicationMarker](base.Values[0].Value, "release-publication")
	if err != nil {
		return false, releases.CorruptReleaseRecord()
	}
	manifest, err := releases.DecodeReleaseRecord[releases.ReleaseStagedManifest](base.Values[1].Value, "release-staged-manifest")
	if err != nil || validateBlueprintCandidateManifest(task, marker, manifest) != nil {
		return false, releases.CorruptReleaseRecord()
	}
	rootRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{environmentBlueprintRootKey(
			task.Owner.EnvironmentID, task.Params[blueprints.EnvironmentDesiredRevisionParam],
		)},
		Revision: readRevision,
	})
	if err != nil || rootRead == nil || len(rootRead.Values) != 1 || rootRead.Values[0] == nil {
		if err != nil {
			return false, err
		}
		return false, releases.CorruptReleaseRecord()
	}
	seal, err := decodeEnvironmentBlueprintSeal(rootRead.Values[0].Value)
	if err != nil {
		return false, err
	}
	attempts, _, err := repository.blueprintCandidateAttempts(ctx, task, seal, readRevision)
	if err != nil {
		return false, err
	}
	terminalKeys := make([]string, len(manifest.Members))
	for index, member := range manifest.Members {
		terminalKeys[index] = releases.ReleaseTerminalKey(member.ReleaseID)
	}
	terminals, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: terminalKeys, Revision: readRevision})
	if err != nil {
		return false, err
	}
	if terminals == nil || terminals.ReadRevision != readRevision || len(terminals.Values) != len(terminalKeys) {
		return false, releases.CorruptReleaseRecord()
	}
	pending := make([]int, 0, len(manifest.Members))
	for index, value := range terminals.Values {
		if value == nil {
			pending = append(pending, index)
		}
	}
	if len(pending) == 0 {
		return false, nil
	}
	detailKeys := make([]string, 0, len(pending)*5)
	for _, index := range pending {
		member := manifest.Members[index]
		detailKeys = append(detailKeys,
			releases.ReleaseIntentStagingKey(publicationID, member.ReleaseID),
			releases.ReleaseCheckpointStagingKey(publicationID, member.ReleaseID),
			releases.ReleaseProjectionKey(member.ServiceID),
			releases.ReleaseTerminalKey(member.ReleaseID),
			releases.ReleaseRetentionKey(member.ReleaseID),
		)
	}
	details, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: detailKeys, Revision: readRevision})
	if err != nil {
		return false, err
	}
	if details == nil || details.ReadRevision != readRevision || len(details.Values) != len(detailKeys) {
		return false, releases.CorruptReleaseRecord()
	}
	conditions := []etcdstore.Condition{
		{Key: base.Values[0].Key, ModRevision: base.Values[0].ModRevision},
		{Key: base.Values[1].Key, ModRevision: base.Values[1].ModRevision},
		{Key: base.Values[2].Key, ModRevision: base.Values[2].ModRevision},
	}
	mutations := make([]etcdstore.Mutation, 0, len(pending)*4)
	defer clearMutations(mutations)
	for offset, index := range pending {
		keys, values := detailKeys[offset*5:offset*5+5], details.Values[offset*5:offset*5+5]
		if values[0] == nil || values[1] == nil || values[3] != nil || values[4] != nil {
			return false, releases.CorruptReleaseRecord()
		}
		member := manifest.Members[index]
		intent, decodeErr := releases.DecodeReleaseRecord[domain.Intent](values[0].Value, "release-intent")
		intentDigest, _ := domain.Digest(intent)
		if decodeErr != nil || domain.ValidateIntent(intent) != nil || intentDigest != member.IntentDigest ||
			intent.OperationKind != domain.OperationBlueprintApply || intent.ID != member.ReleaseID ||
			intent.ServiceID != member.ServiceID || intent.EnvironmentID != task.Owner.EnvironmentID ||
			intent.OperationID != task.OperationID {
			return false, releases.CorruptReleaseRecord()
		}
		checkpoint, decodeErr := releases.DecodeReleaseRecord[domain.Checkpoint](values[1].Value, "release-checkpoint")
		checkpointDigest, _ := domain.Digest(checkpoint)
		if decodeErr != nil || domain.ValidateCheckpoint(checkpoint) != nil ||
			checkpointDigest != member.CheckpointDigest ||
			checkpoint.ReleaseID != intent.ID ||
			checkpoint.State != domain.StatePending {
			return false, releases.CorruptReleaseRecord()
		}
		projection, decodeErr := decodeReleaseProjection(values[2], task.Owner.EnvironmentID, member.ServiceID)
		if decodeErr != nil {
			return false, decodeErr
		}
		successful := terminalStatus == taskjournal.TaskStatusCompleted && !result.ReconciliationRequired
		state := releaseOperationTerminalState(terminalStatus)
		if result.ReconciliationRequired {
			state = domain.StateRecoveryRequired
		}
		checkpoint.State = state
		checkpoint.Evidence = nil
		checkpoint.UpdatedAt = terminalAt
		if err := domain.ValidateCheckpoint(checkpoint); err != nil {
			return false, err
		}
		if successful {
			if projection.EnvironmentID == "" {
				projection.EnvironmentID, projection.ServiceID = task.Owner.EnvironmentID, member.ServiceID
			}
			projection.ServingReleaseID = intent.ID
			projection.CurrentSuccessfulReleaseID = intent.ID
			projection.ServingSlot = intent.Slot
			projection.ActiveOperationID = ""
			projection.Revision++
		}
		retention := releaseRollbackMaterial(intent, state, terminalAt)
		terminal := releaseTerminalSummary(
			intent,
			state,
			projection.ServingReleaseID,
			checkpoint.Evidence,
			attempts,
			retention.Digest,
			terminalAt,
		)
		checkpointValue, encodeErr := releases.EncodeReleaseRecord("release-checkpoint", checkpoint)
		if encodeErr != nil {
			return false, encodeErr
		}
		terminalValue, encodeErr := releases.EncodeReleaseRecord("release-terminal-summary", terminal)
		if encodeErr != nil {
			clear(checkpointValue)
			return false, encodeErr
		}
		retentionValue, encodeErr := releases.EncodeReleaseRecord("release-retention", retention)
		if encodeErr != nil {
			clear(checkpointValue)
			clear(terminalValue)
			return false, encodeErr
		}
		conditions = append(conditions,
			etcdstore.Condition{Key: values[0].Key, ModRevision: values[0].ModRevision},
			etcdstore.Condition{Key: values[1].Key, ModRevision: values[1].ModRevision},
			etcdstore.Condition{Key: keys[2], ModRevision: keyValueRevision(values[2])},
			etcdstore.Condition{Key: keys[3]}, etcdstore.Condition{Key: keys[4]},
		)
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: values[1].Key, Value: checkpointValue},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[3], Value: terminalValue},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[4], Value: retentionValue},
		)
		if successful {
			projectionValue, encodeErr := releases.EncodeReleaseRecord("service-release-projection", projection)
			if encodeErr != nil {
				return false, encodeErr
			}
			mutations = append(
				mutations,
				etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[2], Value: projectionValue},
			)
			runtimeValue, err := blueprintAcknowledgedRuntime(marker, task, assignment, member, result, terminalAt)
			if err != nil {
				return false, err
			}
			// This full replacement consumes no prior runtime receipt. The owning
			// Blueprint terminal envelope fences the held Environment writer and
			// epoch; another runtime writer cannot publish under that ownership.
			mutations = append(
				mutations,
				etcdstore.Mutation{Type: etcdstore.MutationPut, Key: serviceruntimerecord.Key(member.ServiceID), Value: runtimeValue},
			)
		}
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return false, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(errs.KindStateConflict, "Blueprint release terminal evidence changed")
	}
	return true, nil
}
