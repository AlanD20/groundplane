package scriptsourceevidence

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func serviceScriptSource(serviceID string) sourceref.SourceIdentity {
	return sourceref.SourceIdentity{Kind: sourceref.SourceService, ServiceID: serviceID}
}

func serviceScriptAbsenceConditions(serviceID string) []etcdstore.Condition {
	source := serviceScriptSource(serviceID)
	return []etcdstore.Condition{
		{Key: ScriptSourceCountKey(source)},
		{Key: sourceref.ForwardReferencePrefix + ScriptSourceSuffix(source) + "/", Prefix: true},
	}
}

func ClassifyServiceScriptReferences(serviceID string, values []*etcdstore.KeyValue) error {
	if len(values) != 2 || (values[0] == nil) != (values[1] == nil) {
		return errs.New(errs.KindInternal, "Service Script reference absence proof is inconsistent")
	}
	if values[0] == nil {
		return nil
	}
	source := serviceScriptSource(serviceID)
	count, err := DecodeScriptSourceCount(values[0].Value)
	if err != nil || values[0].Key != ScriptSourceCountKey(source) || count.Source != source ||
		count.ReferencedExecutionCount == 0 {
		return errs.New(errs.KindInternal, "Service Script reference count is corrupt")
	}
	reference, err := recordcodec.Decode[sourceref.Reference](values[1].Value, "script-source-reference")
	if err != nil || reference.Source != source || ScriptSourceForwardReferenceKey(reference) != values[1].Key {
		return errs.New(errs.KindInternal, "Service Script source membership is corrupt")
	}
	return errs.New(errs.KindResourceInUse, "Service is referenced by a Script")
}

// Both reads share one revision, and both absence conditions remain in the
// final removal transaction so a reservation cannot race the read proof.
type serviceAbsenceStore interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
}

func PrepareServiceScriptAbsence(
	ctx context.Context,
	store serviceAbsenceStore,
	serviceID string,
	revision int64,
) ([]etcdstore.Condition, error) {
	conditions := serviceScriptAbsenceConditions(serviceID)
	count, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{conditions[0].Key}, Revision: revision})
	if err != nil {
		return nil, err
	}
	if count == nil || len(count.Values) != 1 || count.ReadRevision <= 0 ||
		(revision > 0 && count.ReadRevision != revision) {
		return nil, errs.New(errs.KindInternal, "Service Script count evidence is incomplete")
	}
	members, err := store.Range(ctx, etcdstore.RangeRequest{Prefix: conditions[1].Key, Limit: 1, Revision: count.ReadRevision})
	if err != nil {
		return nil, err
	}
	if members == nil || members.ReadRevision != count.ReadRevision || len(members.Values) > 1 ||
		(members.More && len(members.Values) == 0) {
		return nil, errs.New(errs.KindInternal, "Service Script membership evidence is incomplete")
	}
	var first *etcdstore.KeyValue
	if len(members.Values) == 1 {
		first = &members.Values[0]
	}
	if err := ClassifyServiceScriptReferences(serviceID, []*etcdstore.KeyValue{count.Values[0], first}); err != nil {
		return nil, err
	}
	return conditions, nil
}
