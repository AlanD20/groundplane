package network

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) ResolveResourceHost(ctx context.Context) (workexecution.Placement, error) {
	return repository.publication.ResolveResourceHost(ctx)
}

func (repository *Repository) PublishNetworkWork(
	ctx context.Context,
	request etcd.NetworkWorkPublication,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.publication.PublishNetworkWork(ctx, request)
}

func (repository *Repository) SealBackingHookInputsAtRevision(ctx context.Context, operationID, projectID string,
	configuration backinghook.Configuration, revision int64,
) (*taskconfiguration.BackingHookEncryptedInputs, error) {
	return repository.facts.SealBackingHookInputsAtRevision(ctx, operationID, projectID, configuration, revision)
}

// CaptureBackingZoneDetaches freezes all cleanup inputs at the impact snapshot,
// before publication can expose executable work. It never refreshes one consumer
// independently and silently combines versions from different reads.
func (repository *Repository) CaptureBackingZoneDetaches(ctx context.Context, projectID, zoneID string,
	revision int64,
) (snapshots []attachremoval.DetachSnapshot, resultErr error) {
	defer func() {
		if resultErr != nil {
			for index := range snapshots {
				snapshots[index].Clear()
			}
			snapshots = nil
		}
	}()
	attaches, err := repository.attaches.ListAttachesByBackingNetworkAtRevision(ctx, projectID, zoneID, revision)
	if err != nil {
		return nil, err
	}
	if len(attaches) >= int(workexecution.MaximumTaskItems) {
		return nil, errs.New(errs.KindValidationFailed, "Backing Zone removal exceeds the Task work limit")
	}
	for _, attach := range attaches {
		if attach.ReadRevision != revision || attach.Record.BackingProjectID != projectID ||
			attach.Record.BackingNetworkID != zoneID {
			return snapshots, errs.New(
				errs.KindStateConflict,
				"Backing Zone consumer list differs from the removal snapshot",
			)
		}
		snapshot, err := repository.attaches.CaptureDetachSnapshot(ctx, attach)
		if err != nil {
			return snapshots, err
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}
