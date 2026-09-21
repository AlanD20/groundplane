package secretmutations

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"

	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func secretScriptSource(secretID string) sourceref.SourceIdentity {
	return sourceref.SourceIdentity{
		Kind:              sourceref.SourceSecretValue,
		SecretID:          secretID,
		ValueGenerationID: secretID,
	}
}

func SecretScriptAbsenceConditions(secretID string) []etcdstore.Condition {
	source := secretScriptSource(secretID)
	return []etcdstore.Condition{
		{Key: scriptsourceevidence.ScriptSourceCountKey(source)},
		{Key: sourceref.ForwardReferencePrefix + scriptsourceevidence.ScriptSourceSuffix(source) + "/", Prefix: true},
		{Key: tasksecretpinrecord.SecretPrefix(secretID), Prefix: true},
	}
}

func ClassifySecretScriptReferences(secretID string, values []*etcdstore.KeyValue) error {
	if len(values) != 3 || (values[0] == nil) != (values[1] == nil) {
		return errs.New(errs.KindInternal, "Secret Script reference absence proof is inconsistent")
	}
	if values[0] != nil {
		source := secretScriptSource(secretID)
		count, err := scriptsourceevidence.DecodeScriptSourceCount(values[0].Value)
		if err != nil || values[0].Key != scriptsourceevidence.ScriptSourceCountKey(source) || count.Source != source ||
			count.ReferencedExecutionCount == 0 {
			return errs.New(errs.KindInternal, "Secret Script reference count is corrupt")
		}
		reference, err := recordcodec.Decode[sourceref.Reference](
			values[1].Value,
			"script-source-reference",
		)
		if err != nil || reference.Source != source ||
			scriptsourceevidence.ScriptSourceForwardReferenceKey(reference) != values[1].Key {
			return errs.New(errs.KindInternal, "Secret Script source membership is corrupt")
		}
	}
	if values[2] != nil {
		pin, err := tasksecretpinrecord.Decode(values[2].Value)
		if err != nil || pin.SecretID != secretID ||
			values[2].Key != tasksecretpinrecord.Key(pin.SecretID, pin.OperationID) {
			return tasksecretpinrecord.Corrupt()
		}
		return errs.New(errs.KindResourceInUse, "Secret is pinned by a recoverable Task")
	}
	if values[0] != nil {
		return errs.New(errs.KindResourceInUse, "Secret is referenced by a Script")
	}
	return nil
}

// All reads share one revision, and every absence condition remains in the
// final removal transaction so a Script reservation or recovery pin cannot
// race the read proof.
func PrepareSecretScriptAbsence(
	ctx context.Context,
	store readStore,
	secretID string,
) ([]etcdstore.Condition, error) {
	conditions := SecretScriptAbsenceConditions(secretID)
	count, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{conditions[0].Key}})
	if err != nil {
		return nil, err
	}
	if count == nil || len(count.Values) != 1 || count.ReadRevision <= 0 {
		return nil, errs.New(errs.KindInternal, "Secret Script count evidence is incomplete")
	}
	members, err := store.Range(
		ctx,
		etcdstore.RangeRequest{Prefix: conditions[1].Key, Limit: 1, Revision: count.ReadRevision},
	)
	if err != nil {
		return nil, err
	}
	if members == nil || members.ReadRevision != count.ReadRevision || len(members.Values) > 1 ||
		(members.More && len(members.Values) == 0) {
		return nil, errs.New(errs.KindInternal, "Secret Script membership evidence is incomplete")
	}
	var first *etcdstore.KeyValue
	if len(members.Values) == 1 {
		first = &members.Values[0]
	}
	pins, err := store.Range(
		ctx,
		etcdstore.RangeRequest{Prefix: conditions[2].Key, Limit: 1, Revision: count.ReadRevision},
	)
	if err != nil {
		return nil, err
	}
	if pins == nil || pins.ReadRevision != count.ReadRevision || len(pins.Values) > 1 ||
		(pins.More && len(pins.Values) == 0) {
		return nil, errs.New(errs.KindInternal, "Secret recovery pin evidence is incomplete")
	}
	var firstPin *etcdstore.KeyValue
	if len(pins.Values) == 1 {
		firstPin = &pins.Values[0]
	}
	if err := ClassifySecretScriptReferences(secretID, []*etcdstore.KeyValue{count.Values[0], first, firstPin}); err != nil {
		return nil, err
	}
	return conditions, nil
}
