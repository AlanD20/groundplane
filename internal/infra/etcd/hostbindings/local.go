package hostbindings

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const localHostKey = "/v1/hosts/local"

type localHost struct {
	ID string `json:"id"`
}

// ReadLocal selects the enrolled MVP host for a newly admitted operator action.
// The returned explicit binding and comparisons must be captured at publication;
// an assigned attempt never resolves this singleton again.
func ReadLocal(ctx context.Context, store Reader, revision int64) (Snapshot, error) {
	anchor, err := ReadLocalIdentity(ctx, store, revision)
	if err != nil {
		return Snapshot{}, err
	}
	if anchor.Revision == 0 {
		return Snapshot{}, errs.New(errs.KindStateConflict, "enroll an Agent before creating host work")
	}
	owners, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{HostKey(anchor.Record)}, Revision: anchor.ReadRevision})
	if err != nil {
		return Snapshot{}, err
	}
	if owners == nil || owners.ReadRevision != anchor.ReadRevision || len(owners.Values) != 1 {
		return Snapshot{}, errs.New(errs.KindInternal, "local host binding read is incomplete")
	}
	defer keyvalue.ClearValues(owners.Values)
	owner := owners.Values[0]
	if owner == nil {
		return Snapshot{}, errs.New(errs.KindStateConflict, "local host has no enrolled Agent")
	}
	if owner.Key != HostKey(anchor.Record) || owner.ModRevision <= 0 || owner.ModRevision > anchor.ReadRevision {
		return Snapshot{}, errs.New(errs.KindInternal, "local host binding revision is invalid")
	}
	binding, err := Decode(owner.Value)
	if err != nil || binding.HostID != anchor.Record {
		return Snapshot{}, errs.New(errs.KindInternal, "local host binding identity is inconsistent")
	}
	snapshot, err := Read(ctx, store, binding.AgentID, anchor.ReadRevision)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot.Conditions = append(snapshot.Conditions, keyvalue.Condition{Key: localHostKey, ModRevision: anchor.Revision})
	return snapshot, nil
}

// PrepareLocalEnrollment allocates the MVP installation's host identity only
// during enrollment. The anchor outlives Agent removal/replacement. Execution
// reads use explicit bindings, never this bootstrap singleton as a fallback.
func PrepareLocalEnrollment(ctx context.Context, store Reader, agentID string) (workqueue.Change, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return workqueue.Change{}, err
	}
	if store == nil || ids.Validate(ids.KindAgent, agentID) != nil {
		return workqueue.Change{}, errs.New(errs.KindValidationFailed, "local host enrollment requires an Agent")
	}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{localHostKey}})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 {
		return workqueue.Change{}, errs.New(errs.KindInternal, "local host identity read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	var host localHost
	condition := keyvalue.Condition{Key: localHostKey}
	if value := read.Values[0]; value != nil {
		if value.Key != localHostKey || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
			return workqueue.Change{}, errs.New(errs.KindInternal, "local host identity revision is invalid")
		}
		host, err = recordcodec.Decode[localHost](value.Value, "local-host")
		if err != nil || ids.Validate(ids.KindHost, host.ID) != nil {
			return workqueue.Change{}, errs.New(errs.KindInternal, "local host identity is corrupt")
		}
		condition.ModRevision = value.ModRevision
	} else {
		host.ID = ids.New(ids.KindHost)
	}
	change, err := PrepareCreation(Binding{HostID: host.ID, AgentID: agentID})
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, condition)
	if condition.ModRevision == 0 {
		value, err := recordcodec.Encode("local-host", host)
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, err
		}
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: localHostKey, Value: value})
	}
	return change, nil
}
