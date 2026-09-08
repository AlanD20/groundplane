package etcd

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func blueprintExecutedArtifact(evidence BlueprintReleasePublicationEvidence) ([]byte, error) {
	if err := executionplan.CandidateReleaseDescriptorMatchesPlan(evidence.CandidateReleaseDescriptor, evidence.Plan); err != nil {
		return nil, err
	}
	if evidence.Plan.GetTargetId() != evidence.EnvironmentID ||
		evidence.Plan.GetRenderGeneration() != uint64(evidence.Task.RenderGeneration) {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint executed artifact scope differs from Task")
	}
	for _, artifact := range evidence.Plan.GetArtifacts() {
		if artifact.GetArtifactId() != evidence.Task.Params[TaskComposeArtifactParam] {
			continue
		}
		if artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
			artifact.GetOwnerId() != evidence.EnvironmentID {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint executed artifact owner differs from Task")
		}
		value, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		return value, nil
	}
	return nil, errs.New(errs.KindValidationFailed, "Blueprint executed artifact is absent from prepared plan")
}

func (repository *TaskRepository) blueprintAcknowledgedArtifact(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) ([]byte, Condition, error) {
	key := releasePublicationKey(task.Params[TaskReleasePublicationParam])
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, Condition{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, Condition{}, corruptReleaseRecord()
	}
	marker, err := decodeReleaseRecord[ReleasePublicationMarker](read.Values[0].Value, "release-publication")
	if err != nil || marker.OperationID != task.OperationID ||
		marker.PublicationID != task.Params[TaskReleasePublicationParam] ||
		marker.CandidateReleaseDescriptor.PlanID != task.PlanID ||
		hex.EncodeToString(marker.CandidateReleaseDescriptor.PlanHash) != task.PlanHash {
		return nil, Condition{}, corruptReleaseRecord()
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(marker.ExecutedComposeArtifact, artifact); err != nil ||
		artifact.GetArtifactId() != task.Params[TaskComposeArtifactParam] || artifact.GetOwnerId() != task.Owner.EnvironmentID ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT {
		return nil, Condition{}, corruptReleaseRecord()
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil || !bytes.Equal(canonical, marker.ExecutedComposeArtifact) {
		return nil, Condition{}, corruptReleaseRecord()
	}
	return canonical, Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}
