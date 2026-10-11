package etcd

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type ReleaseNetworkPreparationSource struct {
	Parent     TaskRecord
	ChildID    string
	Artifact   *agentpb.ComposeArtifact
	Conditions []keyvalue.Condition
}

// The prerequisite uses the original published Compose artifact. A newer desired
// head, mutable image tag or later release attempt cannot change its bridge input.
func (ledger *ReleaseLedger) GetReleaseNetworkPreparationSource(
	ctx context.Context,
	parentID string,
) (ReleaseNetworkPreparationSource, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return ReleaseNetworkPreparationSource{}, err
	}
	if ids.Validate(ids.KindTask, parentID) != nil {
		return ReleaseNetworkPreparationSource{}, errs.New(
			errs.KindValidationFailed,
			"release network parent Task id is invalid",
		)
	}
	parent, err := ledger.tasks.GetTask(ctx, parentID)
	if err != nil {
		return ReleaseNetworkPreparationSource{}, err
	}
	task := parent.Record
	publicationID := task.Params[releaserender.TaskReleasePublicationParam]
	if task.Executor != taskjournal.TaskExecutorAgent ||
		(task.Type != taskjournal.TaskDeploy && task.Type != taskjournal.TaskRollback) ||
		task.Owner.EnvironmentID == "" || releases.ValidatePublicationID(publicationID) != nil {
		return ReleaseNetworkPreparationSource{}, errs.New(
			errs.KindStateConflict,
			"release network parent is not a native release Task",
		)
	}
	key := releases.ReleasePublicationKey(publicationID)
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: parent.ReadRevision})
	if err != nil {
		return ReleaseNetworkPreparationSource{}, err
	}
	if read == nil || read.ReadRevision != parent.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return ReleaseNetworkPreparationSource{}, errs.New(
			errs.KindStateConflict,
			"release network captured publication is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	publication, err := releases.DecodeReleaseRecord[releases.ReleasePublicationMarker](
		read.Values[0].Value,
		"release-publication",
	)
	if err != nil {
		return ReleaseNetworkPreparationSource{}, err
	}
	if publication.PublicationID != publicationID || publication.OperationID != task.OperationID ||
		publication.NetworkPreparationTaskID != task.Params[taskjournal.TaskReleaseNetworkChildParam] ||
		publication.CandidateReleaseDescriptor.PlanID != task.PlanID ||
		hex.EncodeToString(publication.CandidateReleaseDescriptor.PlanHash) != task.PlanHash ||
		len(publication.ExecutedComposeArtifact) == 0 {
		return ReleaseNetworkPreparationSource{}, errs.New(
			errs.KindStateConflict,
			"release network captured publication differs from its Task",
		)
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(publication.ExecutedComposeArtifact, artifact); err != nil {
		return ReleaseNetworkPreparationSource{}, errs.Wrap(errs.KindInternal, err)
	}
	if artifact.GetArtifactId() != task.Params[taskjournal.TaskComposeArtifactParam] ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != task.Owner.EnvironmentID {
		return ReleaseNetworkPreparationSource{}, errs.New(
			errs.KindStateConflict,
			"release network captured artifact owner changed",
		)
	}
	return ReleaseNetworkPreparationSource{
		Parent:   task,
		ChildID:  publication.NetworkPreparationTaskID,
		Artifact: artifact,
		Conditions: []keyvalue.Condition{
			{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: parent.Revision},
			{Key: key, ModRevision: read.Values[0].ModRevision},
		},
	}, nil
}
