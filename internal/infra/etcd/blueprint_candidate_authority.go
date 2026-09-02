package etcd

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const blueprintCandidateAttemptAuthorityPrefix = "/v1/runtime/blueprint-candidate-attempts/"

type blueprintCandidateAttemptAuthorityRecord struct {
	Schema                  int              `json:"schema"`
	TaskID                  string           `json:"task_id"`
	RetryOf                 string           `json:"retry_of,omitempty"`
	OperationID             string           `json:"operation_id"`
	PublicationID           string           `json:"publication_id"`
	EnvironmentID           string           `json:"environment_id"`
	BaselineAppliedRevision int64            `json:"baseline_applied_revision"`
	Attempts                []domain.Attempt `json:"attempts"`
}

func blueprintCandidateAttemptAuthorityKey(taskID string) string {
	return blueprintCandidateAttemptAuthorityPrefix + taskID
}

func blueprintCandidateManifestDigest(manifest ReleaseStagedManifest) (string, error) {
	return domain.Digest(struct {
		PublicationID string                   `json:"publication_id"`
		OperationID   string                   `json:"operation_id"`
		Members       []ReleaseStagedMemberRef `json:"members"`
	}{manifest.PublicationID, manifest.OperationID, manifest.Members})
}

func validateBlueprintCandidateManifest(
	task TaskRecord,
	marker ReleasePublicationMarker,
	manifest ReleaseStagedManifest,
) error {
	publicationID := task.Params[TaskReleasePublicationParam]
	if validatePublicationID(publicationID) != nil || marker.PublicationID != publicationID ||
		marker.OperationID != task.OperationID || manifest.PublicationID != publicationID ||
		manifest.OperationID != task.OperationID || marker.ManifestDigest != manifest.Digest ||
		marker.PublishedAt.IsZero() || marker.PublishedAt.Location() != time.UTC ||
		manifest.CreatedAt.IsZero() || manifest.CreatedAt.Location() != time.UTC ||
		len(manifest.Members) == 0 || len(manifest.Members) > maximumReleasePublicationMembers {
		return corruptReleaseRecord()
	}
	digest, err := blueprintCandidateManifestDigest(manifest)
	if err != nil || digest != manifest.Digest {
		return corruptReleaseRecord()
	}
	releases := make(map[string]struct{}, len(manifest.Members))
	services := make(map[string]struct{}, len(manifest.Members))
	for _, member := range manifest.Members {
		if ids.Validate(ids.KindDeployment, member.ReleaseID) != nil ||
			ids.Validate(ids.KindService, member.ServiceID) != nil ||
			!validSHA256(member.IntentDigest) || !validSHA256(member.RenderDigest) ||
			!validSHA256(member.CheckpointDigest) {
			return corruptReleaseRecord()
		}
		if _, duplicate := releases[member.ReleaseID]; duplicate {
			return corruptReleaseRecord()
		}
		if _, duplicate := services[member.ServiceID]; duplicate {
			return corruptReleaseRecord()
		}
		releases[member.ReleaseID] = struct{}{}
		services[member.ServiceID] = struct{}{}
	}
	return nil
}

func validateBlueprintCandidateCompensation(
	intent domain.Intent,
	render ReleaseRenderInput,
	result TaskResultRecord,
) error {
	proxy, hasProxy := releaseProxyEvidence(result, intent.ServiceID)
	recreate, hasRecreate := releaseRecreateEvidence(result, intent.ServiceID)
	priorReleaseID := intent.PriorServingReleaseID
	if priorReleaseID == "" {
		priorReleaseID = "baseline"
	}
	switch intent.Strategy {
	case domain.StrategyBlueGreen:
		if !hasProxy || hasRecreate || !proxy.Compensated ||
			proxy.ReleaseID != priorReleaseID || proxy.Target != string(render.PriorTarget) ||
			proxy.ProxyGeneration != render.PriorProxyGeneration ||
			proxy.ConfigSHA256 != render.PriorProxyDigest {
			return errs.New(errs.KindReleaseRecoveryRequired, "Blueprint proxy predecessor restoration is unproven")
		}
	case domain.StrategyRecreate:
		if hasProxy || !hasRecreate || !recreate.Compensated ||
			recreate.ReleaseID != priorReleaseID || recreate.Target != string(render.PriorTarget) ||
			recreate.ArtifactID != render.PriorArtifactID {
			return errs.New(errs.KindReleaseRecoveryRequired, "Blueprint recreate predecessor restoration is unproven")
		}
	default:
		return corruptReleaseRecord()
	}
	return nil
}

func validateBlueprintCandidateAttempts(
	record blueprintCandidateAttemptAuthorityRecord,
	task TaskRecord,
	seal EnvironmentBlueprintSeal,
) error {
	if record.Schema != 1 || record.TaskID != task.ID || record.RetryOf != task.RetryOf ||
		record.OperationID != task.OperationID ||
		record.PublicationID != task.Params[TaskReleasePublicationParam] ||
		record.EnvironmentID != task.Owner.EnvironmentID ||
		record.BaselineAppliedRevision != seal.BaselineHeadRevision ||
		len(record.Attempts) < 2 || len(record.Attempts) > 33 {
		return corruptReleaseRecord()
	}
	for index, attempt := range record.Attempts {
		if ids.Validate(ids.KindTask, attempt.ID) != nil || attempt.TaskID != attempt.ID ||
			attempt.StartedAt.IsZero() || attempt.StartedAt.Location() != time.UTC ||
			(index == 0 && attempt.RetryOf != "") ||
			(index > 0 && attempt.RetryOf != record.Attempts[index-1].TaskID) {
			return corruptReleaseRecord()
		}
	}
	last := record.Attempts[len(record.Attempts)-1]
	if last.TaskID != task.ID || last.RetryOf != task.RetryOf {
		return corruptReleaseRecord()
	}
	return nil
}

func (repository *TaskRepository) blueprintCandidateAttempts(
	ctx context.Context,
	task TaskRecord,
	seal EnvironmentBlueprintSeal,
	revision int64,
) ([]domain.Attempt, []Condition, error) {
	if task.RetryOf == "" {
		startedAt := task.CreatedAt
		if task.StartedAt != nil {
			startedAt = *task.StartedAt
		}
		return []domain.Attempt{{ID: task.ID, TaskID: task.ID, StartedAt: startedAt}}, nil, nil
	}
	key := blueprintCandidateAttemptAuthorityKey(task.ID)
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, nil, corruptReleaseRecord()
	}
	record, err := decodeEnvelope[blueprintCandidateAttemptAuthorityRecord](
		read.Values[0].Value, "blueprint-candidate-attempt-authority",
	)
	if err != nil || validateBlueprintCandidateAttempts(record, task, seal) != nil {
		return nil, nil, corruptReleaseRecord()
	}
	return slices.Clone(record.Attempts),
		[]Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}, nil
}

func blueprintAttemptIDs(attempts []domain.Attempt) []string {
	result := make([]string, len(attempts))
	for index, attempt := range attempts {
		result[index] = attempt.TaskID
	}
	return result
}
