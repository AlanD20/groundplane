package etcd

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareGroupReleaseCapture stages bounded inert Group inputs after Task
// prerequisites succeed and returns source/manifest/reservation comparisons for
// atomic promotion. Dispatch remains closed until every native input is ready.
func (ledger *ReleaseLedger) PrepareGroupReleaseCapture(
	ctx context.Context,
	accepted workqueue.TaskSnapshot,
	capture GroupReleaseCapture,
	addresses networkreservations.ProxyAddresses,
) (workqueue.Change, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return workqueue.Change{}, err
	}
	if ledger == nil || ledger.store == nil {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Group Release storage is not configured")
	}
	task, err := capture.Validate()
	if err != nil {
		return workqueue.Change{}, err
	}
	if accepted.Revision <= 0 || accepted.Task.ID != task.ID || !accepted.Task.PendingPreparation() ||
		accepted.Task.Scope != task.Scope ||
		!time.Now().Before(task.Deadline) {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Group admission scope or deadline changed",
		)
	}
	seal, err := ledger.stageGroupRelease(ctx, accepted, capture)
	if err != nil {
		return workqueue.Change{}, err
	}
	// Read the seal and header together. The header is also the orphan-cleanup
	// fence: accepted drafts cannot be removed by the absent-Task collector.
	headerKey := groupReleaseDraftHeaderKey(task.ID)
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{headerKey, seal.Key}})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil ||
		read.Values[0].Key != headerKey || read.Values[1].Key != seal.Key || read.Values[0].ModRevision <= 0 ||
		read.Values[1].ModRevision != seal.ModRevision || !bytes.Equal(read.Values[0].Value, read.Values[1].Value) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Group admission has no matching sealed draft",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	draft, err := recordcodec.Decode[groupReleaseDraft](read.Values[0].Value, "group-release-draft")
	if err != nil || draft.TaskID != task.ID || draft.Intent != task.Intent ||
		!draft.CreatedAt.Equal(task.CreatedAt) || !draft.Deadline.Equal(task.Deadline) {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Group admission seal differs from its captured Task",
		)
	}
	change := workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: workqueue.TaskKey(task.ID), ModRevision: accepted.Revision}},
	}
	root, err := releasegroups.PrepareExecutionCapture(task, capture.Root)
	if err != nil {
		return workqueue.Change{}, err
	}
	host, err := hostbindings.ReadLocal(ctx, ledger.store, capture.Root.SourceReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if host.Binding.HostID != capture.Root.Placement.HostID {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Group target host changed during acceptance",
		)
	}
	var sources []keyvalue.Condition
	for _, member := range capture.Members {
		guards, err := ledger.directReleaseSourceConditions(ctx, task.Scope, member.Preparation.Source,
			member.Preparation.Intent.ServiceID, read.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		sources = append(sources, guards...)
	}
	conditions, err := resourcePublicationConditions(change.Conditions, root.Conditions, sources, host.Conditions,
		addresses.Conditions(), []keyvalue.Condition{seal, {Key: headerKey, ModRevision: read.Values[0].ModRevision}})
	if err != nil {
		return workqueue.Change{}, err
	}
	mutations := append(slices.Clone(change.Mutations), root.Mutations...)
	mutations = append(mutations, addresses.Mutations()...)
	return workqueue.Change{Conditions: conditions, Mutations: mutations}, nil
}
