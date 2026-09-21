package scripts

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Reader) GetScript(ctx context.Context, id string) (etcdstore.Versioned[Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindScript, id); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	locatorRead, err := repository.store.Get(ctx, ScriptLocatorKey(id))
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if locatorRead == nil || locatorRead.Entry == nil {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindScriptNotFound, "Script was not found")
	}
	locator, err := DecodeScriptLocator(locatorRead.Entry.Value)
	if err != nil || locator.ScriptID != id {
		return etcdstore.Versioned[Record]{}, recordcodec.CorruptRecord()
	}
	active, err := ReadActiveScriptSet(ctx, repository.store, locator.EnvironmentID, locatorRead.ReadRevision)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	primary, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			ScriptSetScriptKey(locator.EnvironmentID, active.Record.GenerationID, id),
		}, Revision: active.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if primary == nil || len(primary.Values) != 1 || primary.Values[0] == nil {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindScriptNotFound, "Script was not found")
	}
	record, err := DecodeRecord(primary.Values[0].Value)
	if err != nil || record.Desired.ID != id || record.EnvironmentID != locator.EnvironmentID ||
		record.ScriptSetGeneration != active.Record.GenerationID {
		return etcdstore.Versioned[Record]{}, recordcodec.CorruptRecord()
	}
	return repository.HydrateScriptBody(ctx, etcdstore.Versioned[Record]{
		Record: record, Revision: primary.Values[0].ModRevision, ReadRevision: active.ReadRevision,
	})
}

func (repository *Reader) ListScripts(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[Record], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[Record]{}, err
	}
	revision := int64(0)
	if request.Cursor != "" {
		cursor, cursorErr := recordcodec.DecodeCursor(request.Cursor)
		if cursorErr != nil {
			return etcdstore.Page[Record]{}, cursorErr
		}
		revision = cursor.Revision
	}
	active, err := ReadActiveScriptSet(ctx, repository.store, environmentID, revision)
	if err != nil {
		return etcdstore.Page[Record]{}, err
	}
	page, err := recordquery.ListIndexAtRevision(
		ctx, repository.store, "scripts", "environment", environmentID,
		ScriptSetOwnerPrefix(environmentID, active.Record.GenerationID),
		func(id string) string {
			return ScriptSetScriptKey(environmentID, active.Record.GenerationID, id)
		},
		ids.KindScript, request, DecodeRecord,
		func(record Record) string { return record.Desired.ID },
		func(record Record) bool { return record.EnvironmentID == environmentID }, active.ReadRevision,
	)
	if err != nil {
		return etcdstore.Page[Record]{}, err
	}
	for index := range page.Items {
		hydrated, hydrateErr := repository.HydrateScriptBody(ctx, page.Items[index])
		if hydrateErr != nil {
			return etcdstore.Page[Record]{}, hydrateErr
		}
		page.Items[index] = hydrated
	}
	return page, nil
}

func (repository *Reader) HydrateScriptBody(
	ctx context.Context,
	record etcdstore.Versioned[Record],
) (etcdstore.Versioned[Record], error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{ScriptSetBodyGenerationKey(
			record.Record.EnvironmentID, record.Record.ScriptSetGeneration,
			record.Record.Desired.ID, record.Record.ActiveGeneration,
		)},
		Revision: record.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindInternal, "Script body generation is missing")
	}
	defer clear(result.Values[0].Value)
	generation, err := DecodeScriptBodyGeneration(result.Values[0].Value)
	if err != nil || generation.ScriptID != record.Record.Desired.ID ||
		generation.Generation != record.Record.ActiveGeneration {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindInternal, "Script body generation is corrupt")
	}
	record.Record.Desired.Body = generation.Body
	if err := ValidateRecord(record.Record); err != nil {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindInternal, "Script aggregate is corrupt")
	}
	return record, nil
}
