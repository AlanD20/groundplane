package etcd

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// CaptureCandidateLogSource selects the latest published, unfinished Release at
// the caller's snapshot. Captured labels permit read-only container selection;
// they are never treated as an acknowledged serving runtime or health result.
func (ledger *ReleaseLedger) CaptureCandidateLogSource(
	ctx context.Context,
	environmentID, serviceID string,
	revision int64,
) (*agentpb.ServiceObservationTarget, error) {
	if ctx == nil || ledger == nil || ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindService, serviceID) != nil ||
		revision <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "Candidate log source requires a fixed Service snapshot")
	}
	prefix := releases.ReleaseServiceIndexScope(environmentID, serviceID)
	page, err := ledger.store.Range(
		ctx,
		keyvalue.RangeRequest{Prefix: prefix, Limit: 1, Revision: revision, Descending: true},
	)
	if err != nil {
		return nil, err
	}
	if page == nil || page.ReadRevision != revision || len(page.Values) > 1 {
		return nil, releases.CorruptReleaseRecord()
	}
	defer func() {
		for i := range page.Values {
			clear(page.Values[i].Value)
		}
	}()
	if len(page.Values) == 0 {
		return nil, nil
	}
	indexed := page.Values[0]
	releaseID := strings.TrimPrefix(indexed.Key, prefix)
	if ids.Validate(ids.KindDeployment, releaseID) != nil {
		return nil, releases.CorruptReleaseRecord()
	}
	publicationID, err := releasequeries.DecodeReleaseIndex(indexed.Value, serviceID)
	if err != nil {
		return nil, err
	}
	selected, err := ledger.ReadViewAt(ctx, publicationID, releaseID, revision)
	if err != nil {
		return nil, err
	}
	if selected.Intent.EnvironmentID != environmentID || selected.Intent.ServiceID != serviceID {
		return nil, releases.CorruptReleaseRecord()
	}
	if selected.Terminal != nil {
		return nil, nil
	}
	markerRead, err := ledger.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{releases.ReleasePublicationKey(publicationID)}, Revision: revision},
	)
	if err != nil {
		return nil, err
	}
	if markerRead == nil || markerRead.ReadRevision != revision || len(markerRead.Values) != 1 ||
		markerRead.Values[0] == nil {
		return nil, releases.CorruptReleaseRecord()
	}
	defer keyvalue.ClearValues(markerRead.Values)
	marker, err := releases.DecodeWorkPublication(markerRead.Values[0].Value)
	if err != nil {
		return nil, err
	}
	if marker.EnvironmentID != environmentID || marker.ServiceID != serviceID ||
		marker.TaskID != selected.Intent.OriginatingTaskID ||
		marker.OperationID != selected.Intent.OperationID {
		return nil, releases.CorruptReleaseRecord()
	}
	queue, err := workqueue.NewRepository(ledger.store)
	if err != nil {
		return nil, err
	}
	work, err := queue.ReadWork(ctx, marker.TaskID, marker.WorkItemID, revision)
	if err != nil {
		return nil, err
	}
	if work.Task.Task.Scope.EnvironmentID != environmentID || work.Item.Item.Operation != workexecution.Release ||
		work.Item.Item.TargetID != serviceID ||
		work.Item.Item.Input == nil ||
		*work.Item.Item.Input != marker.Input {
		return nil, releases.CorruptReleaseRecord()
	}
	if work.Task.Task.FinishedAt != nil || work.Task.Task.CollectionStartedAt != nil {
		return nil, nil
	}
	sources, err := ledger.LoadReleaseWorkInput(ctx, work)
	if err != nil {
		return nil, err
	}
	defer clear(sources.Rollout.Record.CandidateArtifact)
	if sources.Intent.ID != releaseID {
		return nil, releases.CorruptReleaseRecord()
	}
	artifact, err := releaserender.OpenReleaseCandidateArtifact(sources.Rollout.Record)
	if err != nil {
		return nil, err
	}
	return serviceruntimerecord.CandidateWorkloadObservation(
		environmentID,
		serviceID,
		releaseID,
		sources.Rollout.Record.Render.PlanID,
		uint64(sources.Rollout.Record.Render.Projection.RenderGeneration),
		artifact,
	)
}
