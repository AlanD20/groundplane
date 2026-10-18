package etcd

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverwork"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type resolverAcknowledgement struct {
	component     components.Record
	state         resolverwork.State
	conditions    []keyvalue.Condition
	pendingTaskID string
}

type resolverAcknowledgementPurpose uint8

const (
	resolverServingAcknowledgement resolverAcknowledgementPurpose = iota
	resolverWithdrawalBeforeEffects
	resolverWithdrawalFinalization
)

func readResolverAcknowledgement(ctx context.Context, store hierarchyStore, revision int64) (resolverAcknowledgement, bool, error) {
	return readResolverStateAcknowledgement(ctx, store, revision, resolverServingAcknowledgement)
}

// Serving and withdrawal decisions consume independent applied resource state.
// Captured pending work is read only to exclude effects not yet proven settled;
// no completed Task, diagnostic event or old render-input record is consulted.
func readResolverStateAcknowledgement(ctx context.Context, store hierarchyStore, revision int64, purpose resolverAcknowledgementPurpose) (resolverAcknowledgement, bool, error) {
	var ack resolverAcknowledgement
	indexKey := platformcomponents.PlatformComponentKindKey(core.ComponentKindCoreDNS)
	index, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{indexKey}, Revision: revision})
	if err != nil {
		return ack, false, err
	}
	if index == nil || index.ReadRevision != revision || len(index.Values) != 1 {
		return ack, false, errs.New(errs.KindInternal, "DNS resolver index read is incomplete")
	}
	defer keyvalue.ClearValues(index.Values)
	ack.conditions = []keyvalue.Condition{{Key: indexKey, ModRevision: keyvalue.RevisionOf(index.Values[0])}}
	if index.Values[0] == nil {
		if purpose != resolverServingAcknowledgement {
			return ack, true, nil
		}
		return ack, false, errs.New(errs.KindValidationFailed, "Environment DNS requires an enabled platform CoreDNS resolver")
	}
	componentID := string(index.Values[0].Value)
	if ids.Validate(ids.KindComponent, componentID) != nil {
		return ack, false, errs.New(errs.KindInternal, "DNS resolver index identity is corrupt")
	}
	keys := []string{components.RecordKey(componentID), platformcomponents.PlatformComponentBootstrapKey(componentID)}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return ack, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) || read.Values[0] == nil {
		return ack, false, errs.New(errs.KindInternal, "DNS resolver resource is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	component, err := components.DecodeRecord(read.Values[0].Value)
	if err != nil {
		return ack, false, err
	}
	if component.Desired.ID != componentID || component.Desired.Kind != core.ComponentKindCoreDNS ||
		component.Desired.Owner != core.ComponentOwnerPlatform || component.Desired.OwnerID != "" {
		return ack, false, errs.New(errs.KindInternal, "DNS resolver resource identity changed")
	}
	ack.component = component
	for i, key := range keys {
		ack.conditions = append(ack.conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[i])})
	}
	ack.state, err = resolverwork.ReadLocalState(ctx, store, componentID, revision)
	if err != nil {
		return ack, false, err
	}
	ack.conditions = append(ack.conditions, ack.state.Conditions...)
	if active := ack.state.ActiveItem; active != nil && active.Operation == workexecution.Resolver {
		ack.pendingTaskID = active.TaskID
	} else if ack.state.Pending != nil {
		ack.pendingTaskID = ack.state.Pending.Input.TaskID
	}
	if purpose == resolverServingAcknowledgement && !component.Desired.Enabled {
		return ack, false, errs.New(errs.KindValidationFailed, "Environment DNS requires an enabled platform CoreDNS resolver")
	}
	if ack.pendingTaskID != "" && purpose != resolverWithdrawalFinalization {
		return ack, false, nil
	}
	applied := ack.state.Applied
	if applied == nil {
		absent := purpose != resolverServingAcknowledgement && ack.state.IntentRevision == 0 &&
			!component.Desired.Enabled && !component.Runtime.Healthy && len(component.Runtime.GeneratedServices) == 0 &&
			read.Values[1] != nil && string(read.Values[1].Value) == componentID
		return ack, absent, nil
	}
	if purpose == resolverServingAcknowledgement && !applied.Desired.Enabled {
		return ack, false, nil
	}
	return ack, true, nil
}

// A resource-owned request retains the original forward outcome after history
// expiry. Only a failure for these exact current DNS inputs can fail a dependent
// action; an older authored head or removal fence is never reused as its failure.
func failedResolverConfigurationRequest(ctx context.Context, store hierarchyStore, revision int64, ack resolverAcknowledgement) (string, []keyvalue.Condition, error) {
	request := ack.state.Request
	if request == nil || request.Outcome == "" || request.Outcome == workexecution.Succeeded {
		return "", nil, nil
	}
	desired, err := resolverwork.DesiredDigest(ack.component.Desired)
	if err != nil {
		return "", nil, err
	}
	if desired != request.DesiredSHA256 {
		return "", nil, nil
	}
	global, conditions, err := dnsrecords.NewReader(store).ResolveEnvironmentDNS(ctx, revision)
	if err != nil {
		return "", nil, err
	}
	removal, matches, err := matchResolverRemovalFences(ctx, store, request.References, revision)
	if err != nil || !matches {
		return "", nil, err
	}
	generation, matches, err := dnsrecords.NewReader(store).MatchResolverDNSGeneration(ctx, request.References, revision)
	if err != nil || !matches {
		return "", nil, err
	}
	digest, err := platformcomponents.ResolverConfigurationFingerprint(request.PrivateListener, global.Captures, request.ZoneListeners)
	if err != nil {
		return "", nil, err
	}
	if digest != request.DNSFingerprint {
		return "", nil, nil
	}
	conditions = append(conditions, removal...)
	conditions = appendHostResolutionCondition(conditions, generation)
	conditions = append(conditions, ack.conditions...)
	return request.TaskID, conditions, nil
}

func matchResolverRemovalFences(ctx context.Context, store hierarchyStore,
	captured []keyvalue.Condition, revision int64,
) ([]keyvalue.Condition, bool, error) {
	var matched []keyvalue.Condition
	for _, condition := range captured {
		if !strings.HasPrefix(condition.Key, deletions.TombstoneKey(string(deletions.DeletionTargetZone), "")) &&
			!strings.HasPrefix(condition.Key, deletions.TombstoneKey(string(deletions.DeletionTargetService), "")) {
			continue
		}
		read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{condition.Key}, Revision: revision})
		if err != nil {
			return nil, false, err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
			return nil, false, errs.New(errs.KindInternal, "DNS removal failure source read is incomplete")
		}
		if condition.ModRevision != keyvalue.RevisionOf(read.Values[0]) {
			return nil, false, nil
		}
		matched = append(matched, condition)
	}
	return matched, true, nil
}
