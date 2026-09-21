package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *ScriptRepository) GetScript(ctx context.Context, id string) (etcdstore.Versioned[scriptrecord.Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindScript, id); err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	locatorRead, err := repository.store.Get(ctx, scriptrecord.ScriptLocatorKey(id))
	if err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	if locatorRead == nil || locatorRead.Entry == nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, errs.New(errs.KindScriptNotFound, "Script was not found")
	}
	locator, err := scriptrecord.DecodeScriptLocator(locatorRead.Entry.Value)
	if err != nil || locator.ScriptID != id {
		return etcdstore.Versioned[scriptrecord.Record]{}, recordcodec.CorruptRecord()
	}
	active, err := scriptrecord.ReadActiveScriptSet(ctx, repository.store, locator.EnvironmentID, locatorRead.ReadRevision)
	if err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	primary, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			scriptrecord.ScriptSetScriptKey(locator.EnvironmentID, active.Record.GenerationID, id),
		}, Revision: active.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	if primary == nil || len(primary.Values) != 1 || primary.Values[0] == nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, errs.New(errs.KindScriptNotFound, "Script was not found")
	}
	record, err := scriptrecord.DecodeRecord(primary.Values[0].Value)
	if err != nil || record.Desired.ID != id || record.EnvironmentID != locator.EnvironmentID ||
		record.ScriptSetGeneration != active.Record.GenerationID {
		return etcdstore.Versioned[scriptrecord.Record]{}, recordcodec.CorruptRecord()
	}
	return repository.hydrateScriptBody(ctx, etcdstore.Versioned[scriptrecord.Record]{
		Record: record, Revision: primary.Values[0].ModRevision, ReadRevision: active.ReadRevision,
	})
}

func (repository *ScriptRepository) ListScripts(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[scriptrecord.Record], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[scriptrecord.Record]{}, err
	}
	revision := int64(0)
	if request.Cursor != "" {
		cursor, cursorErr := recordcodec.DecodeCursor(request.Cursor)
		if cursorErr != nil {
			return etcdstore.Page[scriptrecord.Record]{}, cursorErr
		}
		revision = cursor.Revision
	}
	active, err := scriptrecord.ReadActiveScriptSet(ctx, repository.store, environmentID, revision)
	if err != nil {
		return etcdstore.Page[scriptrecord.Record]{}, err
	}
	page, err := recordquery.ListIndexAtRevision(
		ctx, repository.store, "scripts", "environment", environmentID,
		scriptrecord.ScriptSetOwnerPrefix(environmentID, active.Record.GenerationID),
		func(id string) string {
			return scriptrecord.ScriptSetScriptKey(environmentID, active.Record.GenerationID, id)
		},
		ids.KindScript, request, scriptrecord.DecodeRecord,
		func(record scriptrecord.Record) string { return record.Desired.ID },
		func(record scriptrecord.Record) bool { return record.EnvironmentID == environmentID }, active.ReadRevision,
	)
	if err != nil {
		return etcdstore.Page[scriptrecord.Record]{}, err
	}
	for index := range page.Items {
		hydrated, hydrateErr := repository.hydrateScriptBody(ctx, page.Items[index])
		if hydrateErr != nil {
			return etcdstore.Page[scriptrecord.Record]{}, hydrateErr
		}
		page.Items[index] = hydrated
	}
	return page, nil
}

func (repository *ScriptRepository) hydrateScriptBody(
	ctx context.Context,
	record etcdstore.Versioned[scriptrecord.Record],
) (etcdstore.Versioned[scriptrecord.Record], error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{scriptrecord.ScriptSetBodyGenerationKey(
			record.Record.EnvironmentID, record.Record.ScriptSetGeneration,
			record.Record.Desired.ID, record.Record.ActiveGeneration,
		)},
		Revision: record.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, errs.New(errs.KindInternal, "Script body generation is missing")
	}
	defer clear(result.Values[0].Value)
	generation, err := scriptrecord.DecodeScriptBodyGeneration(result.Values[0].Value)
	if err != nil || generation.ScriptID != record.Record.Desired.ID ||
		generation.Generation != record.Record.ActiveGeneration {
		return etcdstore.Versioned[scriptrecord.Record]{}, errs.New(errs.KindInternal, "Script body generation is corrupt")
	}
	record.Record.Desired.Body = generation.Body
	if err := scriptrecord.ValidateRecord(record.Record); err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, errs.New(errs.KindInternal, "Script aggregate is corrupt")
	}
	return record, nil
}
