package etcd

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const resolverMetadataCursorKey = "/v1/maintenance/resolver-dns-metadata/cursor"
const resolverMetadataCursorKind = "resolver_dns_metadata_cursor"

type resolverMetadataCursor struct {
	After string `json:"after"`
}

// ReconcileResolverDNSMetadata retires declaration-only DNS ownership from
// actual resolver state. It never creates a Task or performs a host effect.
// A durable scan cursor prevents one Environment from monopolizing cleanup.
func (repository *TaskRepository) ReconcileResolverDNSMetadata(ctx context.Context) error {
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{resolverMetadataCursorKey}})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 {
		return errs.New(errs.KindInternal, "DNS metadata cleanup cursor read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	var cursor resolverMetadataCursor
	if read.Values[0] != nil {
		cursor, err = recordcodec.Decode[resolverMetadataCursor](read.Values[0].Value, resolverMetadataCursorKind)
		if err != nil {
			return err
		}
		if cursor.After != "" && !strings.HasPrefix(cursor.After, environmentqueries.EnvironmentDesiredHeadScanPrefix) {
			return errs.New(errs.KindInternal, "DNS metadata cleanup cursor is corrupt")
		}
	}
	ack, ready, err := readResolverStateAcknowledgement(ctx, repository.store, read.ReadRevision, resolverWithdrawalBeforeEffects)
	if err != nil || !ready {
		return err
	}
	if ack.component.Desired.ID == "" || ack.state.ClaimOwner != nil {
		return nil
	}
	page, err := repository.store.Range(ctx, keyvalue.RangeRequest{
		Prefix: environmentqueries.EnvironmentDesiredHeadScanPrefix, StartExclusive: cursor.After,
		Limit: 32, Revision: read.ReadRevision,
	})
	if err != nil {
		return err
	}
	if page == nil || page.ReadRevision != read.ReadRevision || page.More && len(page.Values) == 0 {
		return errs.New(errs.KindInternal, "DNS metadata cleanup scan is incomplete")
	}
	defer keyvalue.ClearRangeValues(page.Values)
	conditions := append([]keyvalue.Condition{{Key: resolverMetadataCursorKey, ModRevision: keyvalue.RevisionOf(read.Values[0])}}, ack.conditions...)
	var mutations []keyvalue.Mutation
	defer func() { keyvalue.ClearMutationValues(mutations) }()
	for _, value := range page.Values {
		cursor.After = value.Key
		if !strings.HasSuffix(value.Key, "/current") {
			continue
		}
		environmentID := strings.TrimSuffix(strings.TrimPrefix(value.Key, environmentqueries.EnvironmentDesiredHeadScanPrefix), "/current")
		if ids.Validate(ids.KindEnvironment, environmentID) != nil {
			return errs.New(errs.KindInternal, "DNS metadata cleanup Environment head is corrupt")
		}
		guards, changes, err := repository.prepareResolverDNSMetadata(ctx, environmentID, ack, read.ReadRevision)
		if err != nil {
			return err
		}
		if len(changes) == 0 {
			continue
		}
		conditions = append(conditions, guards...)
		mutations = changes
		break
	}
	if len(mutations) == 0 && !page.More {
		cursor.After = ""
	}
	raw, err := recordcodec.Encode(resolverMetadataCursorKind, cursor)
	if err != nil {
		return err
	}
	defer clear(raw)
	mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: resolverMetadataCursorKey, Value: raw})
	budget, err := repository.store.MeasureTransaction(ctx, conditions, mutations)
	if err != nil {
		return err
	}
	if !budget.Fits() {
		return errs.New(errs.KindInternal, "DNS metadata cleanup exceeds transaction budget")
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return err
	}
	keyvalue.ClearValues(result.FailureReads)
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "DNS metadata cleanup source changed")
	}
	return nil
}
