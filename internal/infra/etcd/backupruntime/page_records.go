package backupruntime

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
)

const maximumBackupRuntimeListLimit = 96
const MaximumBackupPruneBatch = 11

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
	Items    []etcdstore.Versioned[BackupRecoveryPointRecord]
	NextID   string
	Revision int64
}

func ValidateBackupRuntimeListRequest(prefix string, request BackupRuntimeListRequest) error {
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
	case strings.HasPrefix(prefix, BackupRecoveryPointEnvironmentPrefix),
		strings.HasPrefix(prefix, BackupRecoveryPointSourcePrefix):
		_, ok := InvertBackupRecoveryPointULIDBody(suffix)
		return ok
	case strings.HasPrefix(prefix, BackupRecoveryPointConnectorPrefix),
		strings.HasPrefix(prefix, BackupOrphanEnvironmentPrefix),
		strings.HasPrefix(prefix, BackupRetentionPrefix):
		return recordcodec.ValidateID(ids.KindRecoveryPoint, suffix) == nil
	case strings.HasPrefix(prefix, BackupRunEnvironmentPrefix),
		strings.HasPrefix(prefix, BackupRestoreEnvironmentPrefix):
		return recordcodec.ValidateID(ids.KindTask, suffix) == nil
	default:
		return true
	}
}
