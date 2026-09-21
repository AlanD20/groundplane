package scripts

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ReadActiveScriptSet(
	ctx context.Context,
	store activeReadStore,
	environmentID string,
	revision int64,
) (etcdstore.Versioned[SetGenerationRecord], error) {
	read, err := store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{ScriptSetActiveKey(environmentID)}, Revision: revision},
	)
	if err != nil {
		return etcdstore.Versioned[SetGenerationRecord]{}, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		return etcdstore.Versioned[SetGenerationRecord]{}, errs.New(
			errs.KindInternal,
			"Environment active Script-set generation is missing",
		)
	}
	record, err := DecodeScriptSetGeneration(read.Values[0].Value)
	if err != nil || record.EnvironmentID != environmentID {
		return etcdstore.Versioned[SetGenerationRecord]{}, recordcodec.CorruptRecord()
	}
	return etcdstore.Versioned[SetGenerationRecord]{
		Record: record, Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision,
	}, nil
}

type activeScriptStorage struct {
	Script             etcdstore.Versioned[Record]
	Active             etcdstore.Versioned[SetGenerationRecord]
	Locator            etcdstore.Versioned[LocatorRecord]
	EnvironmentLocator etcdstore.Versioned[string]
}

func ReadActiveScriptStorage(
	ctx context.Context,
	store activeReadStore,
	scriptID string,
	revision int64,
) (activeScriptStorage, error) {
	locatorRead, err := store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{ScriptLocatorKey(scriptID)}, Revision: revision},
	)
	if err != nil {
		return activeScriptStorage{}, err
	}
	if locatorRead == nil || len(locatorRead.Values) != 1 || locatorRead.Values[0] == nil {
		return activeScriptStorage{}, errs.New(errs.KindScriptNotFound, "Script was not found")
	}
	locator, err := DecodeScriptLocator(locatorRead.Values[0].Value)
	if err != nil || locator.ScriptID != scriptID {
		return activeScriptStorage{}, recordcodec.CorruptRecord()
	}
	environmentLocatorRead, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			ScriptEnvironmentLocatorKey(locator.EnvironmentID, scriptID),
		}, Revision: locatorRead.ReadRevision,
	})
	if err != nil {
		return activeScriptStorage{}, err
	}
	if environmentLocatorRead == nil || len(environmentLocatorRead.Values) != 1 ||
		environmentLocatorRead.Values[0] == nil || string(environmentLocatorRead.Values[0].Value) != scriptID {
		return activeScriptStorage{}, errs.New(errs.KindInternal, "Script Environment locator is missing or corrupt")
	}
	active, err := ReadActiveScriptSet(ctx, store, locator.EnvironmentID, locatorRead.ReadRevision)
	if err != nil {
		return activeScriptStorage{}, err
	}
	primary, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{ScriptSetScriptKey(locator.EnvironmentID, active.Record.GenerationID, scriptID)},
		Revision: active.ReadRevision,
	})
	if err != nil {
		return activeScriptStorage{}, err
	}
	if primary == nil || len(primary.Values) != 1 || primary.Values[0] == nil {
		return activeScriptStorage{}, errs.New(errs.KindScriptNotFound, "Script was not found")
	}
	record, err := DecodeRecord(primary.Values[0].Value)
	if err != nil || record.Desired.ID != scriptID || record.EnvironmentID != locator.EnvironmentID ||
		record.ScriptSetGeneration != active.Record.GenerationID {
		return activeScriptStorage{}, recordcodec.CorruptRecord()
	}
	return activeScriptStorage{
		Script: etcdstore.Versioned[Record]{
			Record:       record,
			Revision:     primary.Values[0].ModRevision,
			ReadRevision: active.ReadRevision,
		},
		Active: active,
		Locator: etcdstore.Versioned[LocatorRecord]{
			Record:       locator,
			Revision:     locatorRead.Values[0].ModRevision,
			ReadRevision: active.ReadRevision,
		},
		EnvironmentLocator: etcdstore.Versioned[string]{
			Record:       scriptID,
			Revision:     environmentLocatorRead.Values[0].ModRevision,
			ReadRevision: active.ReadRevision,
		},
	}, nil
}
