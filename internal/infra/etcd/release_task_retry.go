package etcd

import (
	"context"
	"encoding/json"
	"slices"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type releaseTaskRetryChange struct {
	applies    bool
	conditions []Condition
	mutations  []Mutation
}

func (change *releaseTaskRetryChange) clear() {
	if change == nil {
		return
	}
	clearMutations(change.mutations)
	change.conditions = nil
	change.mutations = nil
}

func (repository *TaskRepository) prepareReleaseTaskRetry(ctx context.Context, source, retry TaskRecord, revision int64) (releaseTaskRetryChange, error) {
	publicationID := source.Params[TaskReleasePublicationParam]
	if publicationID == "" {
		return releaseTaskRetryChange{}, nil
	}
	if validatePublicationID(publicationID) != nil || source.Executor != TaskExecutorAgent ||
		(source.Type != TaskDeploy && source.Type != TaskRollback) || retry.Type != source.Type ||
		retry.OperationID != source.OperationID || retry.Params[TaskReleasePublicationParam] != publicationID ||
		source.Result == nil || !source.Result.ReconciliationRequired {
		return releaseTaskRetryChange{}, errs.New(errs.KindTaskNotRetryable, "release Task does not own a recoverable ledger attempt")
	}
	baseKeys := []string{
		releasePublicationKey(publicationID), releaseManifestStagingKey(publicationID),
		releaseOperationKey(source.OperationID), releaseFenceSetKey(source.Owner.EnvironmentID),
		environmentMutationEpochKey(source.Owner.EnvironmentID),
	}
	base, err := repository.store.GetMany(ctx, GetManyRequest{Keys: baseKeys, Revision: revision})
	if err != nil {
		return releaseTaskRetryChange{}, err
	}
	if base == nil || base.ReadRevision != revision || len(base.Values) != len(baseKeys) {
		return releaseTaskRetryChange{}, corruptReleaseRecord()
	}
	for _, value := range base.Values {
		if value == nil {
			return releaseTaskRetryChange{}, corruptReleaseRecord()
		}
	}
	marker, err := decodeReleaseRecord[ReleasePublicationMarker](base.Values[0].Value, "release-publication")
	if err != nil || marker.PublicationID != publicationID || marker.OperationID != source.OperationID {
		return releaseTaskRetryChange{}, corruptReleaseRecord()
	}
	manifest, err := decodeReleaseRecord[ReleaseStagedManifest](base.Values[1].Value, "release-staged-manifest")
	if err != nil || manifest.PublicationID != publicationID || manifest.OperationID != source.OperationID || len(manifest.Members) == 0 {
		return releaseTaskRetryChange{}, corruptReleaseRecord()
	}
	head, err := decodeReleaseRecord[ReleaseOperationHead](base.Values[2].Value, "release-operation")
	if err != nil || head.OperationID != source.OperationID || head.PublicationID != publicationID ||
		head.EnvironmentID != source.Owner.EnvironmentID || head.LatestTaskID != source.ID ||
		head.State != domain.StateRecoveryRequired || len(head.Members) != len(manifest.Members) ||
		head.RecoveryOutcome == "" || head.FailedMemberOrdinal == 0 {
		return releaseTaskRetryChange{}, errs.New(errs.KindTaskNotRetryable, "release operation is not awaiting recovery")
	}
	fence, err := decodeReleaseRecord[ReleaseFenceSet](base.Values[3].Value, "release-fence-set")
	if err != nil || fence.OperationID != source.OperationID || fence.AttemptTaskID != source.ID ||
		fence.EnvironmentID != source.Owner.EnvironmentID || len(fence.Members) != len(manifest.Members) {
		return releaseTaskRetryChange{}, corruptReleaseRecord()
	}
	memberKeys := make([]string, 0, len(manifest.Members)*2)
	for _, member := range manifest.Members {
		memberKeys = append(memberKeys, releaseIntentStagingKey(publicationID, member.ReleaseID), releaseRenderInputStagingKey(publicationID, member.ReleaseID))
	}
	members, err := repository.store.GetMany(ctx, GetManyRequest{Keys: memberKeys, Revision: revision})
	if err != nil {
		return releaseTaskRetryChange{}, err
	}
	if members == nil || members.ReadRevision != revision || len(members.Values) != len(memberKeys) {
		return releaseTaskRetryChange{}, corruptReleaseRecord()
	}
	conditions := make([]Condition, 0, len(baseKeys)+len(memberKeys))
	for index, value := range base.Values {
		conditions = append(conditions, Condition{Key: baseKeys[index], ModRevision: value.ModRevision})
	}
	for index, reference := range manifest.Members {
		intentValue, renderValue := members.Values[index*2], members.Values[index*2+1]
		if intentValue == nil || renderValue == nil {
			return releaseTaskRetryChange{}, corruptReleaseRecord()
		}
		intent, err := decodeReleaseRecord[domain.Intent](intentValue.Value, "release-intent")
		if err != nil || domain.ValidateIntent(intent) != nil || intent.ID != reference.ReleaseID ||
			intent.ServiceID != reference.ServiceID || intent.OperationID != source.OperationID ||
			len(head.Attempts) == 0 || intent.OriginatingTaskID != head.Attempts[0].TaskID {
			return releaseTaskRetryChange{}, corruptReleaseRecord()
		}
		raw, err := decodeReleaseRecord[json.RawMessage](renderValue.Value, "release-render-input")
		if err != nil {
			return releaseTaskRetryChange{}, err
		}
		render, err := decodeReleaseRenderInput(raw)
		digest, digestErr := domain.Digest(raw)
		if err != nil || digestErr != nil || render.ReleaseID != intent.ID || render.ServiceID != intent.ServiceID ||
			render.PlanID != source.PlanID || digest != intent.RenderInputDigest || digest != reference.RenderDigest {
			return releaseTaskRetryChange{}, corruptReleaseRecord()
		}
		conditions = append(conditions,
			Condition{Key: memberKeys[index*2], ModRevision: intentValue.ModRevision},
			Condition{Key: memberKeys[index*2+1], ModRevision: renderValue.ModRevision},
		)
	}
	head.State = domain.StateRecovering
	head.LatestTaskID = retry.ID
	head.Attempts = append(slices.Clone(head.Attempts), domain.Attempt{ID: retry.ID, TaskID: retry.ID, RetryOf: source.ID, StartedAt: retry.CreatedAt})
	head.UpdatedAt = retry.CreatedAt
	if head.Progress != nil {
		progress := *head.Progress
		progress.AttemptID = retry.ID
		progress.UpdatedAt = retry.CreatedAt
		head.Progress = &progress
	}
	fence.Generation++
	fence.AttemptTaskID = retry.ID
	headValue, err := encodeReleaseRecord("release-operation", head)
	if err != nil {
		return releaseTaskRetryChange{}, err
	}
	fenceValue, err := encodeReleaseRecord("release-fence-set", fence)
	if err != nil {
		clear(headValue)
		return releaseTaskRetryChange{}, err
	}
	epochValue := slices.Clone(base.Values[4].Value)
	return releaseTaskRetryChange{
		applies: true, conditions: conditions,
		mutations: []Mutation{
			{Type: MutationPut, Key: baseKeys[2], Value: headValue},
			{Type: MutationPut, Key: baseKeys[3], Value: fenceValue},
			{Type: MutationPut, Key: baseKeys[4], Value: epochValue},
		},
	}, nil
}
