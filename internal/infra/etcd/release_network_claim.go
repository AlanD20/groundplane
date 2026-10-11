package etcd

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type releaseNetworkDependency struct {
	parent      keyvalue.Versioned[TaskRecord]
	child       keyvalue.Versioned[TaskRecord]
	upstreams   []string
	preparation executionplan.ReleaseNetworkPreparation
	conditions  []keyvalue.Condition
}

// Reads only immutable release input and its native dependency at one MVCC view.
// No newer Environment head or image tag can redefine the queued release.
func (repository *TaskRepository) readReleaseNetworkDependency(ctx context.Context,
	task TaskRecord, revision int64,
) (releaseNetworkDependency, bool, error) {
	parent := task
	conditions := []keyvalue.Condition{}
	parentRevision := int64(0)
	if taskjournal.IsReleaseNetworkChild(task.Params) {
		key := taskjournal.TaskStorageKey(task.Params[taskjournal.TaskReleaseNetworkParentParam])
		read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
		if err != nil {
			return releaseNetworkDependency{}, false, err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
			return releaseNetworkDependency{}, false, errs.New(
				errs.KindStateConflict,
				"release network parent is unavailable",
			)
		}
		defer keyvalue.ClearValues(read.Values)
		parent, err = DecodeTaskRecord(read.Values[0].Value)
		if err != nil {
			return releaseNetworkDependency{}, false, err
		}
		parentRevision = read.Values[0].ModRevision
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: parentRevision})
	}
	publicationID := parent.Params[releaserender.TaskReleasePublicationParam]
	if publicationID == "" {
		return releaseNetworkDependency{}, false, nil
	}
	if parent.Executor != taskjournal.TaskExecutorAgent ||
		(parent.Type != taskjournal.TaskDeploy && parent.Type != taskjournal.TaskRollback) {
		return releaseNetworkDependency{}, false, nil // Authored Service units own their separate prerequisite path.
	}
	if parent.RetryOf != "" {
		// Native release Retry executes its captured recovery procedure, not another
		// forward deployment. It must not re-enter a forward DNS prerequisite.
		return releaseNetworkDependency{}, false, nil
	}
	keys := []string{
		releases.ReleasePublicationKey(publicationID),
		releases.ReleaseFenceSetKey(parent.Owner.EnvironmentID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return releaseNetworkDependency{}, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) || read.Values[0] == nil {
		return releaseNetworkDependency{}, false, errs.New(
			errs.KindStateConflict,
			"release network publication is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	marker, err := releases.DecodeReleaseRecord[releases.ReleasePublicationMarker](
		read.Values[0].Value,
		"release-publication",
	)
	if err != nil {
		return releaseNetworkDependency{}, false, err
	}
	if marker.NetworkPreparationTaskID == "" {
		if taskjournal.IsReleaseNetworkChild(task.Params) ||
			parent.Params[taskjournal.TaskReleaseNetworkChildParam] != "" {
			return releaseNetworkDependency{}, false, errs.New(
				errs.KindStateConflict,
				"release does not own this network prerequisite",
			)
		}
		return releaseNetworkDependency{}, false, nil
	}
	if marker.PublicationID != publicationID || marker.OperationID != parent.OperationID ||
		parent.Params[taskjournal.TaskReleaseNetworkChildParam] != marker.NetworkPreparationTaskID ||
		marker.CandidateReleaseDescriptor.PlanID != parent.PlanID ||
		hex.EncodeToString(marker.CandidateReleaseDescriptor.PlanHash) != parent.PlanHash ||
		ids.Validate(ids.KindTask, marker.NetworkPreparationTaskID) != nil {
		return releaseNetworkDependency{}, false, releases.CorruptReleaseRecord()
	}
	conditions = append(conditions, keyvalue.Condition{Key: keys[0], ModRevision: read.Values[0].ModRevision})
	if read.Values[1] != nil {
		fence, err := releases.DecodeReleaseRecord[releases.ReleaseFenceSet](read.Values[1].Value, "release-fence-set")
		if err != nil || !taskjournal.IsTerminalTaskStatus(parent.Status) &&
			(fence.OperationID != parent.OperationID || fence.AttemptTaskID != parent.ID) {
			return releaseNetworkDependency{}, false, errs.New(
				errs.KindStateConflict,
				"release network operation no longer owns its Environment fence",
			)
		}
		conditions = append(conditions, keyvalue.Condition{Key: keys[1], ModRevision: read.Values[1].ModRevision})
	} else {
		conditions = append(conditions, keyvalue.Condition{Key: keys[1]})
		if !taskjournal.IsTerminalTaskStatus(parent.Status) {
			return releaseNetworkDependency{}, false, releases.CorruptReleaseRecord()
		}
	}
	childKey := taskjournal.TaskStorageKey(marker.NetworkPreparationTaskID)
	childRead, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{childKey}, Revision: revision},
	)
	if err != nil {
		return releaseNetworkDependency{}, false, err
	}
	if childRead == nil || childRead.ReadRevision != revision || len(childRead.Values) != 1 ||
		childRead.Values[0] == nil {
		return releaseNetworkDependency{}, false, errs.New(
			errs.KindStateConflict,
			"release network prerequisite is unavailable",
		)
	}
	defer keyvalue.ClearValues(childRead.Values)
	child, err := DecodeTaskRecord(childRead.Values[0].Value)
	if err != nil {
		return releaseNetworkDependency{}, false, err
	}
	if child.ID != marker.NetworkPreparationTaskID || child.Owner != parent.Owner ||
		child.Target != parent.Owner.EnvironmentID || child.Params[taskjournal.TaskReleaseNetworkParentParam] != parent.ID ||
		child.RenderGeneration != parent.RenderGeneration || !taskjournal.IsReleaseNetworkChild(child.Params) ||
		taskjournal.IsReleaseNetworkChild(task.Params) && child.ID != task.ID {
		return releaseNetworkDependency{}, false, errs.New(
			errs.KindStateConflict,
			"release network prerequisite ownership changed",
		)
	}
	conditions = append(conditions, keyvalue.Condition{Key: childKey, ModRevision: childRead.Values[0].ModRevision})
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(marker.ExecutedComposeArtifact, artifact); err != nil {
		return releaseNetworkDependency{}, false, errs.Wrap(errs.KindInternal, err)
	}
	if artifact.GetArtifactId() != parent.Params[taskjournal.TaskComposeArtifactParam] ||
		artifact.GetOwnerId() != parent.Owner.EnvironmentID {
		return releaseNetworkDependency{}, false, releases.CorruptReleaseRecord()
	}
	planTime, err := ids.Timestamp(ids.KindPlan, child.PlanID)
	if err != nil {
		return releaseNetworkDependency{}, false, err
	}
	preparation, err := executionplan.CaptureReleaseNetworkPreparation(artifact,
		ids.DeriveAt(ids.KindConfig, planTime, child.PlanID, "release-network-preparation"))
	if err != nil || preparation.Artifact == nil {
		if err == nil {
			err = releases.CorruptReleaseRecord()
		}
		return releaseNetworkDependency{}, false, err
	}
	return releaseNetworkDependency{
		parent: keyvalue.Versioned[TaskRecord]{Record: parent, Revision: parentRevision, ReadRevision: revision},
		child: keyvalue.Versioned[TaskRecord]{
			Record:       child,
			Revision:     childRead.Values[0].ModRevision,
			ReadRevision: revision,
		},
		upstreams: preparation.Upstreams, preparation: preparation, conditions: conditions,
	}, true, nil
}

func (repository *TaskRepository) releaseNetworkClaimConditions(ctx context.Context, task TaskRecord,
	revision int64, at time.Time,
) ([]keyvalue.Condition, bool, error) {
	dependency, applies, err := repository.readReleaseNetworkDependency(ctx, task, revision)
	if err != nil || !applies {
		return nil, !applies, err
	}
	if !TaskForwardDeadline(task, at).After(at) {
		return dependency.conditions, false, nil
	}
	if taskjournal.IsReleaseNetworkChild(task.Params) {
		return dependency.conditions, dependency.parent.Record.Status == taskjournal.TaskStatusPending, nil
	}
	if dependency.child.Record.Status != taskjournal.TaskStatusCompleted || dependency.child.Record.Result == nil ||
		dependency.child.Record.Result.ReconciliationRequired {
		return dependency.conditions, false, nil
	}
	ack, ready, err := readResolverAcknowledgement(ctx, repository.store, revision)
	if kind, _ := errs.KindOf(err); kind == errs.KindValidationFailed {
		return dependency.conditions, false, nil
	}
	if err != nil || !ready {
		return dependency.conditions, false, err
	}
	if !releaseNetworkResolverReady(ack, ready, dependency.upstreams) {
		return dependency.conditions, false, nil
	}
	return append(dependency.conditions, ack.conditions...), true, nil
}
