package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func readActiveScriptSet(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	revision int64,
) (Versioned[scriptrecord.SetGenerationRecord], error) {
	read, err := store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{scriptrecord.ScriptSetActiveKey(environmentID)}, Revision: revision},
	)
	if err != nil {
		return Versioned[scriptrecord.SetGenerationRecord]{}, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		return Versioned[scriptrecord.SetGenerationRecord]{}, errs.New(
			errs.KindInternal,
			"Environment active Script-set generation is missing",
		)
	}
	record, err := scriptrecord.DecodeScriptSetGeneration(read.Values[0].Value)
	if err != nil || record.EnvironmentID != environmentID {
		return Versioned[scriptrecord.SetGenerationRecord]{}, recordcodec.CorruptRecord()
	}
	return Versioned[scriptrecord.SetGenerationRecord]{
		Record: record, Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision,
	}, nil
}

type activeScriptStorage struct {
	Script             Versioned[scriptrecord.Record]
	Active             Versioned[scriptrecord.SetGenerationRecord]
	Locator            Versioned[scriptrecord.LocatorRecord]
	EnvironmentLocator Versioned[string]
}

func readActiveScriptStorage(
	ctx context.Context,
	store hierarchyStore,
	scriptID string,
	revision int64,
) (activeScriptStorage, error) {
	locatorRead, err := store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{scriptrecord.ScriptLocatorKey(scriptID)}, Revision: revision},
	)
	if err != nil {
		return activeScriptStorage{}, err
	}
	if locatorRead == nil || len(locatorRead.Values) != 1 || locatorRead.Values[0] == nil {
		return activeScriptStorage{}, errs.New(errs.KindScriptNotFound, "Script was not found")
	}
	locator, err := scriptrecord.DecodeScriptLocator(locatorRead.Values[0].Value)
	if err != nil || locator.ScriptID != scriptID {
		return activeScriptStorage{}, recordcodec.CorruptRecord()
	}
	environmentLocatorRead, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			scriptrecord.ScriptEnvironmentLocatorKey(locator.EnvironmentID, scriptID),
		}, Revision: locatorRead.ReadRevision,
	})
	if err != nil {
		return activeScriptStorage{}, err
	}
	if environmentLocatorRead == nil || len(environmentLocatorRead.Values) != 1 ||
		environmentLocatorRead.Values[0] == nil || string(environmentLocatorRead.Values[0].Value) != scriptID {
		return activeScriptStorage{}, errs.New(errs.KindInternal, "Script Environment locator is missing or corrupt")
	}
	active, err := readActiveScriptSet(ctx, store, locator.EnvironmentID, locatorRead.ReadRevision)
	if err != nil {
		return activeScriptStorage{}, err
	}
	primary, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{scriptrecord.ScriptSetScriptKey(locator.EnvironmentID, active.Record.GenerationID, scriptID)},
		Revision: active.ReadRevision,
	})
	if err != nil {
		return activeScriptStorage{}, err
	}
	if primary == nil || len(primary.Values) != 1 || primary.Values[0] == nil {
		return activeScriptStorage{}, errs.New(errs.KindScriptNotFound, "Script was not found")
	}
	record, err := scriptrecord.DecodeRecord(primary.Values[0].Value)
	if err != nil || record.Desired.ID != scriptID || record.EnvironmentID != locator.EnvironmentID ||
		record.ScriptSetGeneration != active.Record.GenerationID {
		return activeScriptStorage{}, recordcodec.CorruptRecord()
	}
	return activeScriptStorage{
		Script: Versioned[scriptrecord.Record]{
			Record:       record,
			Revision:     primary.Values[0].ModRevision,
			ReadRevision: active.ReadRevision,
		},
		Active: active,
		Locator: Versioned[scriptrecord.LocatorRecord]{
			Record:       locator,
			Revision:     locatorRead.Values[0].ModRevision,
			ReadRevision: active.ReadRevision,
		},
		EnvironmentLocator: Versioned[string]{
			Record:       scriptID,
			Revision:     environmentLocatorRead.Values[0].ModRevision,
			ReadRevision: active.ReadRevision,
		},
	}, nil
}
