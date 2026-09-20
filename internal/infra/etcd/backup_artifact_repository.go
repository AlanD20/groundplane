package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
)

const maximumBackupRuntimeListLimit = 96
const maximumBackupPruneBatch = 11

type BackupRuntimeListRequest struct {
	Limit          int
	StartExclusive string
	Revision       int64
}

type BackupRuntimePage[T any] struct {
	Items    []etcdstore.Versioned[T]
	Next     string
	Revision int64
}

// BackupRecoveryPointPageRequest is the stable-id public paging seam. The
// repository alone translates AfterID to its private inverted index key.
type BackupRecoveryPointPageRequest struct {
	Limit    int
	AfterID  string
	Revision int64
}

// BackupRecoveryPointPage never exposes an etcd key or inverted-id layout.
type BackupRecoveryPointPage struct {
	Items    []etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]
	NextID   string
	Revision int64
}

func allBackupRuntimeValuesAbsent(values []*etcdstore.KeyValue) bool {
	for _, value := range values {
		if value != nil {
			return false
		}
	}
	return true
}

func getOptionalBackupRuntimeRecord[T any](
	ctx context.Context,
	store hierarchyStore,
	key string,
	stableID string,
	decode func([]byte) (T, error),
	id func(T) string,
) (etcdstore.Versioned[T], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[T]{}, false, err
	}
	result, err := store.Get(ctx, key)
	if err != nil {
		return etcdstore.Versioned[T]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[T]{}, false, errs.New(
			errs.KindInternal,
			"backup runtime record read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[T]{ReadRevision: result.ReadRevision}, false, nil
	}
	defer clear(result.Entry.Value)
	record, err := decode(result.Entry.Value)
	if err != nil || id(record) != stableID {
		return etcdstore.Versioned[T]{}, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	return etcdstore.Versioned[T]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func validateBackupRuntimeListRequest(prefix string, request BackupRuntimeListRequest) error {
	if request.Limit <= 0 || request.Limit > maximumBackupRuntimeListLimit ||
		request.Revision < 0 ||
		(request.StartExclusive != "" &&
			(request.Revision <= 0 || !validBackupRuntimeListCursor(prefix, request.StartExclusive))) {
		return errs.New(errs.KindValidationFailed, "backup runtime list request is invalid")
	}
	return nil
}

func validBackupRuntimeListCursor(prefix string, cursor string) bool {
	if !strings.HasPrefix(cursor, prefix) {
		return false
	}
	suffix := strings.TrimPrefix(cursor, prefix)
	if suffix == "" || strings.Contains(suffix, "/") {
		return false
	}
	switch {
	case strings.HasPrefix(prefix, backupruntime.BackupRecoveryPointEnvironmentPrefix),
		strings.HasPrefix(prefix, backupruntime.BackupRecoveryPointSourcePrefix):
		_, ok := backupruntime.InvertBackupRecoveryPointULIDBody(suffix)
		return ok
	case strings.HasPrefix(prefix, backupruntime.BackupRecoveryPointConnectorPrefix),
		strings.HasPrefix(prefix, backupruntime.BackupOrphanEnvironmentPrefix),
		strings.HasPrefix(prefix, backupruntime.BackupRetentionPrefix):
		return recordcodec.ValidateID(ids.KindRecoveryPoint, suffix) == nil
	case strings.HasPrefix(prefix, backupruntime.BackupRunEnvironmentPrefix),
		strings.HasPrefix(prefix, backupruntime.BackupRestoreEnvironmentPrefix):
		return recordcodec.ValidateID(ids.KindTask, suffix) == nil
	default:
		return true
	}
}

func clearRangeValues(values []etcdstore.KeyValue) {
	for index := range values {
		clear(values[index].Value)
		values[index].Value = nil
	}
}
