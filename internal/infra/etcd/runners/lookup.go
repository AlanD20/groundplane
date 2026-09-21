package runners

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	"github.com/AlanD20/groundplane/pkg/errs"
	"sort"
	"strings"
)

func (repository *Reader) ResolveRunner(
	ctx context.Context,
	tenantID string,
	reference string,
) (etcdstore.Versioned[RunnerRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[RunnerRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindTenant, tenantID); err != nil {
		return etcdstore.Versioned[RunnerRecord]{}, err
	}
	if ids.Validate(ids.KindRunner, reference) == nil {
		current, err := repository.GetRunner(ctx, reference)
		if err != nil {
			return etcdstore.Versioned[RunnerRecord]{}, err
		}
		if current.Record.Desired.TenantID != tenantID {
			return etcdstore.Versioned[RunnerRecord]{}, errs.New(
				errs.KindScopeUnauthorized,
				"Runner is outside the Tenant scope",
			)
		}
		return current, nil
	}
	if reference == "" {
		return etcdstore.Versioned[RunnerRecord]{}, errs.New(errs.KindRunnerNotFound, "Runner was not found")
	}
	index, err := repository.store.Get(ctx, RunnerTenantSlugKey(tenantID, reference))
	if err != nil {
		return etcdstore.Versioned[RunnerRecord]{}, err
	}
	if index == nil || index.Entry == nil {
		return etcdstore.Versioned[RunnerRecord]{}, errs.New(errs.KindRunnerNotFound, "Runner was not found")
	}
	id := string(index.Entry.Value)
	if ids.Validate(ids.KindRunner, id) != nil {
		return etcdstore.Versioned[RunnerRecord]{}, errs.New(errs.KindInternal, "runner slug index is corrupt")
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{RunnerKey(id), RunnerLifecycleKey(id)}, Revision: index.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[RunnerRecord]{}, err
	}
	if result == nil || len(result.Values) != 2 || result.Values[0] == nil || result.Values[1] == nil {
		return etcdstore.Versioned[RunnerRecord]{}, errs.New(errs.KindInternal, "runner slug index is corrupt")
	}
	record, err := DecodeRunnerAggregate(result.Values[0], result.Values[1])
	if err != nil || record.Desired.ID != id || record.Desired.TenantID != tenantID ||
		record.Desired.Slug != reference {
		return etcdstore.Versioned[RunnerRecord]{}, errs.New(errs.KindInternal, "runner slug index is corrupt")
	}
	return etcdstore.Versioned[RunnerRecord]{
		Record:       record,
		Revision:     result.Values[0].ModRevision,
		ReadRevision: result.ReadRevision,
	}, nil
}

func (repository *Reader) GetRunner(ctx context.Context, id string) (etcdstore.Versioned[RunnerRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[RunnerRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindRunner, id); err != nil {
		return etcdstore.Versioned[RunnerRecord]{}, err
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{RunnerKey(id), RunnerLifecycleKey(id)},
	})
	if err != nil {
		return etcdstore.Versioned[RunnerRecord]{}, err
	}
	if result == nil || len(result.Values) != 2 {
		return etcdstore.Versioned[RunnerRecord]{}, errs.New(errs.KindInternal, "runner read is incomplete")
	}
	if result.Values[0] == nil && result.Values[1] == nil {
		return etcdstore.Versioned[RunnerRecord]{}, errs.New(errs.KindRunnerNotFound, "Runner was not found")
	}
	if result.Values[0] == nil || result.Values[1] == nil {
		return etcdstore.Versioned[RunnerRecord]{}, errs.New(
			errs.KindInternal,
			"runner desired/lifecycle pair is incomplete",
		)
	}
	record, err := DecodeRunnerAggregate(result.Values[0], result.Values[1])
	if err != nil || record.Desired.ID != id {
		return etcdstore.Versioned[RunnerRecord]{}, errs.New(
			errs.KindInternal,
			"runner desired/lifecycle pair is corrupt",
		)
	}
	return etcdstore.Versioned[RunnerRecord]{
		Record: record, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func DecodeRunnerAggregate(desiredValue *etcdstore.KeyValue, lifecycleValue *etcdstore.KeyValue) (RunnerRecord, error) {
	if desiredValue == nil || lifecycleValue == nil || desiredValue.ModRevision <= 0 ||
		lifecycleValue.ModRevision <= 0 {
		return RunnerRecord{}, errs.New(errs.KindInternal, "runner desired/lifecycle pair is incomplete")
	}
	desired, err := DecodeRunnerDesiredRecord(desiredValue.Value)
	if err != nil {
		return RunnerRecord{}, err
	}
	lifecycle, err := DecodeRunnerLifecycleRecord(lifecycleValue.Value)
	if err != nil || lifecycle.RunnerID != desired.ID {
		return RunnerRecord{}, errs.New(errs.KindInternal, "runner desired/lifecycle pair is corrupt")
	}
	return RunnerRecord{
		Desired: desired, RunnerLifecycleRecord: lifecycle, LifecycleRevision: lifecycleValue.ModRevision,
	}, nil
}

func (repository *Reader) ListRunners(
	ctx context.Context,
	filter RunnerFilter,
	request etcdstore.PageRequest,
) (etcdstore.Page[RunnerRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Page[RunnerRecord]{}, err
	}
	if (filter.TenantID == "") == (filter.ProjectID == "") {
		return etcdstore.Page[RunnerRecord]{}, errs.New(
			errs.KindValidationFailed,
			"runner list requires exactly one tenant or project filter",
		)
	}
	if filter.ProjectID != "" {
		if err := recordcodec.ValidateID(ids.KindProject, filter.ProjectID); err != nil {
			return etcdstore.Page[RunnerRecord]{}, err
		}
		page, err := recordquery.ListIndex(
			ctx, repository.store, "runners", "project", filter.ProjectID,
			RunnerOwnerPrefix(RunnerOwnerProject, filter.ProjectID), RunnerKey, ids.KindRunner, request,
			DecodeRunnerDesiredAggregate,
			func(record RunnerRecord) string { return record.Desired.ID },
			func(record RunnerRecord) bool {
				return record.Desired.OwnerKind == RunnerOwnerProject && record.Desired.OwnerID == filter.ProjectID
			},
		)
		if err != nil {
			return etcdstore.Page[RunnerRecord]{}, err
		}
		return repository.hydrateRunnerPage(ctx, page)
	}
	return repository.listTenantRunners(ctx, filter.TenantID, request)
}

func (repository *Reader) listTenantRunners(
	ctx context.Context,
	tenantID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[RunnerRecord], error) {
	if err := recordcodec.ValidateID(ids.KindTenant, tenantID); err != nil {
		return etcdstore.Page[RunnerRecord]{}, err
	}
	prefix := RunnerTenantCursorPrefix(tenantID)
	limit, revision, startKey, query, err := recordquery.NormalizePageRequest(
		request, "runners", "tenant", tenantID, prefix, ids.KindRunner,
	)
	if err != nil {
		return etcdstore.Page[RunnerRecord]{}, err
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{RunnerTenantQuotaKey(tenantID)}, Revision: revision,
	})
	if err != nil {
		return etcdstore.Page[RunnerRecord]{}, err
	}
	if read == nil || len(read.Values) != 1 {
		return etcdstore.Page[RunnerRecord]{}, errs.New(errs.KindInternal, "runner tenant quota read is incomplete")
	}
	quota := runnerallocation.RunnerTenantQuota{RunnerIDs: []string{}}
	if read.Values[0] != nil {
		quota, err = DecodeRunnerTenantQuota(read.Values[0].Value)
		if err != nil || quota.Validate() != nil {
			return etcdstore.Page[RunnerRecord]{}, CorruptRunnerTenantQuota()
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
		keys[index] = RunnerKey(runnerID)
	}
	page := etcdstore.Page[RunnerRecord]{Items: []etcdstore.Versioned[RunnerRecord]{}, Revision: read.ReadRevision}
	if len(keys) != 0 {
		records, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: read.ReadRevision})
		if err != nil {
			return etcdstore.Page[RunnerRecord]{}, err
		}
		if records == nil || len(records.Values) != len(keys) {
			return etcdstore.Page[RunnerRecord]{}, errs.New(errs.KindInternal, "runner tenant membership is corrupt")
		}
		for index, value := range records.Values {
			if value == nil {
				return etcdstore.Page[RunnerRecord]{}, errs.New(
					errs.KindInternal,
					"runner tenant membership is corrupt",
				)
			}
			record, err := DecodeRunnerDesiredAggregate(value.Value)
			if err != nil || record.Desired.ID != idsPage[index] || record.Desired.TenantID != tenantID {
				return etcdstore.Page[RunnerRecord]{}, errs.New(
					errs.KindInternal,
					"runner tenant membership is corrupt",
				)
			}
			page.Items = append(page.Items, etcdstore.Versioned[RunnerRecord]{
				Record: record, Revision: value.ModRevision, ReadRevision: records.ReadRevision,
			})
		}
	}
	if end < len(quota.RunnerIDs) {
		page.NextCursor, err = recordcodec.EncodeCursor(recordcodec.Cursor{
			Version: recordcodec.CursorVersion, Revision: read.ReadRevision, LastID: quota.RunnerIDs[end-1], Query: query,
		})
		if err != nil {
			return etcdstore.Page[RunnerRecord]{}, err
		}
	}
	return repository.hydrateRunnerPage(ctx, page)
}

func (repository *Reader) hydrateRunnerPage(
	ctx context.Context,
	page etcdstore.Page[RunnerRecord],
) (etcdstore.Page[RunnerRecord], error) {
	if len(page.Items) == 0 {
		return page, nil
	}
	keys := make([]string, len(page.Items))
	for index := range page.Items {
		keys[index] = RunnerLifecycleKey(page.Items[index].Record.Desired.ID)
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: page.Revision})
	if err != nil {
		return etcdstore.Page[RunnerRecord]{}, err
	}
	if result == nil || len(result.Values) != len(keys) {
		return etcdstore.Page[RunnerRecord]{}, errs.New(errs.KindInternal, "runner lifecycle page is incomplete")
	}
	for index, value := range result.Values {
		if value == nil {
			return etcdstore.Page[RunnerRecord]{}, errs.New(errs.KindInternal, "runner lifecycle page is incomplete")
		}
		lifecycle, err := DecodeRunnerLifecycleRecord(value.Value)
		if err != nil || lifecycle.RunnerID != page.Items[index].Record.Desired.ID {
			return etcdstore.Page[RunnerRecord]{}, errs.New(errs.KindInternal, "runner lifecycle page is corrupt")
		}
		page.Items[index].Record.RunnerLifecycleRecord = lifecycle
		page.Items[index].Record.LifecycleRevision = value.ModRevision
	}
	return page, nil
}
