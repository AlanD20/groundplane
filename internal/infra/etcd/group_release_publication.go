package etcd

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublishGroupReleaseWithWork accepts one native Task with captured Group
// membership, exact source revisions and Environment exclusion. Large leaves
// are inert until this atomic acceptance; graph dispatch stays closed until
// operation-owned inputs and all member descriptors have been published.
func (ledger *ReleaseLedger) PublishGroupReleaseWithWork(ctx context.Context, capture GroupReleaseCapture,
	addresses networkreservations.ProxyAddresses, marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if ledger == nil || ledger.store == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Group Release storage is not configured")
	}
	task, err := capture.Validate()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != task.Scope.EnvironmentID ||
		!time.Now().Before(task.Deadline) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"Group admission scope or deadline changed",
		)
	}
	seal, err := ledger.stageGroupRelease(ctx, capture)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	// Read the seal and header together. The header is also the orphan-cleanup
	// fence: accepted drafts cannot be removed by the absent-Task collector.
	headerKey := groupReleaseDraftHeaderKey(task.ID)
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{headerKey, seal.Key}})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if read == nil || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil ||
		read.Values[0].Key != headerKey || read.Values[1].Key != seal.Key || read.Values[0].ModRevision <= 0 ||
		read.Values[1].ModRevision != seal.ModRevision || !bytes.Equal(read.Values[0].Value, read.Values[1].Value) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"Group admission has no matching sealed draft",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	draft, err := recordcodec.Decode[groupReleaseDraft](read.Values[0].Value, "group-release-draft")
	if err != nil || draft.TaskID != task.ID || draft.Intent != task.Intent ||
		!draft.CreatedAt.Equal(task.CreatedAt) || !draft.Deadline.Equal(task.Deadline) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"Group admission seal differs from its captured Task",
		)
	}
	firstID := capture.Root.Policy.Members[0].ForwardID
	firstIndex := slices.IndexFunc(capture.Root.Graph, func(item workexecution.Item) bool { return item.ID == firstID })
	if firstIndex < 0 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"Group admission lost its initial member",
		)
	}
	change, err := workqueue.PrepareStagedGraph(
		task,
		capture.Root.Graph,
		[]workexecution.Item{capture.Root.Graph[firstIndex]},
		marker,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	root, err := releasegroups.PrepareExecutionCapture(task, capture.Root)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ZeroMutationBytes(root.Mutations)
	host, err := hostbindings.ReadLocal(ctx, ledger.store, capture.Root.SourceReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if host.Binding.HostID != capture.Root.Placement.HostID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"Group target host changed during acceptance",
		)
	}
	var sources []keyvalue.Condition
	for _, member := range capture.Members {
		guards, err := ledger.directReleaseSourceConditions(ctx, task.Scope, member.Preparation.Source,
			member.Preparation.Intent.ServiceID, read.ReadRevision)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		sources = append(sources, guards...)
	}
	conditions, err := resourcePublicationConditions(change.Conditions, root.Conditions, sources, host.Conditions,
		addresses.Conditions(), []keyvalue.Condition{seal, {Key: headerKey, ModRevision: read.Values[0].ModRevision}})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	mutations := append(slices.Clone(change.Mutations), root.Mutations...)
	mutations = append(mutations, addresses.Mutations()...)
	plan, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask,
		conditions, mutations, resourceWorkPublicationConflict(conditions))
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replay, err := NewIdempotencyRepository(ledger.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return replay.Apply(ctx, marker, plan)
}
