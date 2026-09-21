package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
	"sort"
	"strings"
)

func (repository *RunnerRepository) ResolveRunner(
	ctx context.Context,
	tenantID string,
	reference string,
) (etcdstore.Versioned[runnerrecord.RunnerRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindTenant, tenantID); err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, err
	}
	if ids.Validate(ids.KindRunner, reference) == nil {
		current, err := repository.GetRunner(ctx, reference)
		if err != nil {
			return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, err
		}
		if current.Record.Desired.TenantID != tenantID {
			return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, errs.New(errs.KindScopeUnauthorized, "Runner is outside the Tenant scope")
		}
		return current, nil
	}
	if reference == "" {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, errs.New(errs.KindRunnerNotFound, "Runner was not found")
	}
	index, err := repository.store.Get(ctx, runnerrecord.RunnerTenantSlugKey(tenantID, reference))
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, err
	}
	if index == nil || index.Entry == nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, errs.New(errs.KindRunnerNotFound, "Runner was not found")
	}
	id := string(index.Entry.Value)
	if ids.Validate(ids.KindRunner, id) != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, errs.New(errs.KindInternal, "runner slug index is corrupt")
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{runnerrecord.RunnerKey(id), runnerrecord.RunnerLifecycleKey(id)}, Revision: index.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, err
	}
	if result == nil || len(result.Values) != 2 || result.Values[0] == nil || result.Values[1] == nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, errs.New(errs.KindInternal, "runner slug index is corrupt")
	}
	record, err := decodeRunnerAggregate(result.Values[0], result.Values[1])
	if err != nil || record.Desired.ID != id || record.Desired.TenantID != tenantID ||
		record.Desired.Slug != reference {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, errs.New(errs.KindInternal, "runner slug index is corrupt")
	}
	return etcdstore.Versioned[runnerrecord.RunnerRecord]{
		Record:       record,
		Revision:     result.Values[0].ModRevision,
		ReadRevision: result.ReadRevision,
	}, nil
}

func (repository *RunnerRepository) GetRunner(ctx context.Context, id string) (etcdstore.Versioned[runnerrecord.RunnerRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindRunner, id); err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, err
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{runnerrecord.RunnerKey(id), runnerrecord.RunnerLifecycleKey(id)},
	})
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, err
	}
	if result == nil || len(result.Values) != 2 {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, errs.New(errs.KindInternal, "runner read is incomplete")
	}
	if result.Values[0] == nil && result.Values[1] == nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, errs.New(errs.KindRunnerNotFound, "Runner was not found")
	}
	if result.Values[0] == nil || result.Values[1] == nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, errs.New(errs.KindInternal, "runner desired/lifecycle pair is incomplete")
	}
	record, err := decodeRunnerAggregate(result.Values[0], result.Values[1])
	if err != nil || record.Desired.ID != id {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, errs.New(errs.KindInternal, "runner desired/lifecycle pair is corrupt")
	}
	return etcdstore.Versioned[runnerrecord.RunnerRecord]{
		Record: record, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func decodeRunnerAggregate(desiredValue *etcdstore.KeyValue, lifecycleValue *etcdstore.KeyValue) (runnerrecord.RunnerRecord, error) {
	if desiredValue == nil || lifecycleValue == nil || desiredValue.ModRevision <= 0 ||
		lifecycleValue.ModRevision <= 0 {
		return runnerrecord.RunnerRecord{}, errs.New(errs.KindInternal, "runner desired/lifecycle pair is incomplete")
	}
	desired, err := runnerrecord.DecodeRunnerDesiredRecord(desiredValue.Value)
	if err != nil {
		return runnerrecord.RunnerRecord{}, err
	}
	lifecycle, err := runnerrecord.DecodeRunnerLifecycleRecord(lifecycleValue.Value)
	if err != nil || lifecycle.RunnerID != desired.ID {
		return runnerrecord.RunnerRecord{}, errs.New(errs.KindInternal, "runner desired/lifecycle pair is corrupt")
	}
	return runnerrecord.RunnerRecord{
		Desired: desired, RunnerLifecycleRecord: lifecycle, LifecycleRevision: lifecycleValue.ModRevision,
	}, nil
}

func (repository *RunnerRepository) ListRunners(
	ctx context.Context,
	filter RunnerFilter,
	request etcdstore.PageRequest,
) (etcdstore.Page[runnerrecord.RunnerRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Page[runnerrecord.RunnerRecord]{}, err
	}
	if (filter.TenantID == "") == (filter.ProjectID == "") {
		return etcdstore.Page[runnerrecord.RunnerRecord]{}, errs.New(
			errs.KindValidationFailed,
			"runner list requires exactly one tenant or project filter",
		)
	}
	if filter.ProjectID != "" {
		if err := recordcodec.ValidateID(ids.KindProject, filter.ProjectID); err != nil {
			return etcdstore.Page[runnerrecord.RunnerRecord]{}, err
		}
		page, err := recordquery.ListIndex(
			ctx, repository.store, "runners", "project", filter.ProjectID,
			runnerrecord.RunnerOwnerPrefix(runnerrecord.RunnerOwnerProject, filter.ProjectID), runnerrecord.RunnerKey, ids.KindRunner, request,
			runnerrecord.DecodeRunnerDesiredAggregate,
			func(record runnerrecord.RunnerRecord) string { return record.Desired.ID },
			func(record runnerrecord.RunnerRecord) bool {
				return record.Desired.OwnerKind == runnerrecord.RunnerOwnerProject && record.Desired.OwnerID == filter.ProjectID
			},
		)
		if err != nil {
			return etcdstore.Page[runnerrecord.RunnerRecord]{}, err
		}
		return repository.hydrateRunnerPage(ctx, page)
	}
	return repository.listTenantRunners(ctx, filter.TenantID, request)
}

func (repository *RunnerRepository) listTenantRunners(
	ctx context.Context,
	tenantID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[runnerrecord.RunnerRecord], error) {
	if err := recordcodec.ValidateID(ids.KindTenant, tenantID); err != nil {
		return etcdstore.Page[runnerrecord.RunnerRecord]{}, err
	}
	prefix := runnerrecord.RunnerTenantCursorPrefix(tenantID)
	limit, revision, startKey, query, err := recordquery.NormalizePageRequest(
		request, "runners", "tenant", tenantID, prefix, ids.KindRunner,
	)
	if err != nil {
		return etcdstore.Page[runnerrecord.RunnerRecord]{}, err
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{runnerrecord.RunnerTenantQuotaKey(tenantID)}, Revision: revision,
	})
	if err != nil {
		return etcdstore.Page[runnerrecord.RunnerRecord]{}, err
	}
	if read == nil || len(read.Values) != 1 {
		return etcdstore.Page[runnerrecord.RunnerRecord]{}, errs.New(errs.KindInternal, "runner tenant quota read is incomplete")
	}
	quota := runnerallocation.RunnerTenantQuota{RunnerIDs: []string{}}
	if read.Values[0] != nil {
		quota, err = runnerrecord.DecodeRunnerTenantQuota(read.Values[0].Value)
		if err != nil || quota.Validate() != nil {
			return etcdstore.Page[runnerrecord.RunnerRecord]{}, runnerrecord.CorruptRunnerTenantQuota()
		}
	}
	startID := strings.TrimPrefix(startKey, prefix)
	start := 0
	if startID != "" {
		start = sort.SearchStrings(quota.RunnerIDs, startID)
		if start < len(quota.RunnerIDs) && quota.RunnerIDs[start] == startID {
			start++
		}
	}
	end := start + limit
	if end > len(quota.RunnerIDs) {
		end = len(quota.RunnerIDs)
	}
	idsPage := quota.RunnerIDs[start:end]
	keys := make([]string, len(idsPage))
	for index, runnerID := range idsPage {
		keys[index] = runnerrecord.RunnerKey(runnerID)
	}
	page := etcdstore.Page[runnerrecord.RunnerRecord]{Items: []etcdstore.Versioned[runnerrecord.RunnerRecord]{}, Revision: read.ReadRevision}
	if len(keys) != 0 {
		records, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: read.ReadRevision})
		if err != nil {
			return etcdstore.Page[runnerrecord.RunnerRecord]{}, err
		}
		if records == nil || len(records.Values) != len(keys) {
			return etcdstore.Page[runnerrecord.RunnerRecord]{}, errs.New(errs.KindInternal, "runner tenant membership is corrupt")
		}
		for index, value := range records.Values {
			if value == nil {
				return etcdstore.Page[runnerrecord.RunnerRecord]{}, errs.New(errs.KindInternal, "runner tenant membership is corrupt")
			}
			record, err := runnerrecord.DecodeRunnerDesiredAggregate(value.Value)
			if err != nil || record.Desired.ID != idsPage[index] || record.Desired.TenantID != tenantID {
				return etcdstore.Page[runnerrecord.RunnerRecord]{}, errs.New(errs.KindInternal, "runner tenant membership is corrupt")
			}
			page.Items = append(page.Items, etcdstore.Versioned[runnerrecord.RunnerRecord]{
				Record: record, Revision: value.ModRevision, ReadRevision: records.ReadRevision,
			})
		}
	}
	if end < len(quota.RunnerIDs) {
		page.NextCursor, err = recordcodec.EncodeCursor(recordcodec.Cursor{
			Version: recordcodec.CursorVersion, Revision: read.ReadRevision, LastID: quota.RunnerIDs[end-1], Query: query,
		})
		if err != nil {
			return etcdstore.Page[runnerrecord.RunnerRecord]{}, err
		}
	}
	return repository.hydrateRunnerPage(ctx, page)
}

func (repository *RunnerRepository) hydrateRunnerPage(
	ctx context.Context,
	page etcdstore.Page[runnerrecord.RunnerRecord],
) (etcdstore.Page[runnerrecord.RunnerRecord], error) {
	if len(page.Items) == 0 {
		return page, nil
	}
	keys := make([]string, len(page.Items))
	for index := range page.Items {
		keys[index] = runnerrecord.RunnerLifecycleKey(page.Items[index].Record.Desired.ID)
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: page.Revision})
	if err != nil {
		return etcdstore.Page[runnerrecord.RunnerRecord]{}, err
	}
	if result == nil || len(result.Values) != len(keys) {
		return etcdstore.Page[runnerrecord.RunnerRecord]{}, errs.New(errs.KindInternal, "runner lifecycle page is incomplete")
	}
	for index, value := range result.Values {
		if value == nil {
			return etcdstore.Page[runnerrecord.RunnerRecord]{}, errs.New(errs.KindInternal, "runner lifecycle page is incomplete")
		}
		lifecycle, err := runnerrecord.DecodeRunnerLifecycleRecord(value.Value)
		if err != nil || lifecycle.RunnerID != page.Items[index].Record.Desired.ID {
			return etcdstore.Page[runnerrecord.RunnerRecord]{}, errs.New(errs.KindInternal, "runner lifecycle page is corrupt")
		}
		page.Items[index].Record.RunnerLifecycleRecord = lifecycle
		page.Items[index].Record.LifecycleRevision = value.ModRevision
	}
	return page, nil
}
