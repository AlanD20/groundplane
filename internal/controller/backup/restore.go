package backup

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const restoreRoute = "/environments/{id}/restore"

// RestoreService selects an immutable target before publishing one
// native Restore and Task atomically. Identity bytes never enter that write.
type RestoreService struct {
	accepted        *acceptedaction.Submitter
	versionObserver DatabaseVersionObserver
	runtime         *etcd.BackupRuntimeRepository
	coordinator     *requestidempotency.Coordinator
	idempotency     *etcd.IdempotencyRepository
	secrets         *BackupSecretResolver
	volumeRoot      string
	resolveDatabase backupplanning.BackupDatabaseIdentityResolver
	serviceFacts    backupplanning.BackupServiceFactResolver
}

func NewRestoreService(runtime *etcd.BackupRuntimeRepository,
	coordinator *requestidempotency.Coordinator, idempotencyRepository *etcd.IdempotencyRepository,
	secrets *BackupSecretResolver, resolveDatabase backupplanning.BackupDatabaseIdentityResolver,
	serviceFacts backupplanning.BackupServiceFactResolver, volumeRoot string,
	versionObserver DatabaseVersionObserver, accepted *acceptedaction.Submitter,
) (*RestoreService, error) {
	if runtime == nil || coordinator == nil || idempotencyRepository == nil || secrets == nil ||
		resolveDatabase == nil || serviceFacts == nil || versionObserver == nil ||
		!filepath.IsAbs(volumeRoot) || filepath.Clean(volumeRoot) != volumeRoot {
		return nil, errs.New(errs.KindInternal, "Restore dependencies are required")
	}
	return &RestoreService{runtime: runtime, coordinator: coordinator, idempotency: idempotencyRepository,
		secrets: secrets, resolveDatabase: resolveDatabase, serviceFacts: serviceFacts, volumeRoot: volumeRoot, versionObserver: versionObserver, accepted: accepted}, nil
}

func restorePublicationUnknown(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, errs.New(errs.KindStorageUnavailable, ""))
}
