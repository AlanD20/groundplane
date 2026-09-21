package volume

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *MutationService) prepareRemoval(
	ctx context.Context, source etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], request volumeMutationRequest,
	task *etcd.TaskRecord, marker idempotencyrecord.IdempotencyMarker,
) (removal.InitialPublication, error) {
	impact, err := hex.DecodeString(request.impactToken)
	if err != nil || len(impact) != sha256.Size || len(task.Steps) == 0 {
		return removal.InitialPublication{}, errs.New(
			errs.KindValidationFailed,
			"Volume removal impact or steps are invalid",
		)
	}
	runtime := removal.Runtime{
		OperationID: task.OperationID, EnvironmentID: request.environmentID, VolumeID: request.volumeID, Key: request.key,
		DesiredRevisionID: task.Params[blueprints.EnvironmentDesiredRevisionParam], DesiredGeneration: uint64(task.RenderGeneration),
		ImpactSHA256: [sha256.Size]byte(impact), IntentSHA256: sha256.Sum256(marker.Intent.Ciphertext),
		RootResponseSHA256: sha256.Sum256(marker.Response.Body),
		RootLocator: removal.ReplayLocator{
			ScopeKind: string(marker.Locator.ScopeKind), ScopeID: marker.Locator.ScopeID,
			Method: marker.Locator.Method, Route: marker.Locator.Route, Key: marker.Locator.Key,
		},
		OriginTaskID: task.ID, CurrentTaskID: task.ID, AttemptOrdinal: 1, StepID: task.Steps[len(task.Steps)-1].ID,
		Checkpoint: removal.DesiredPublished, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
	previous, found, err := service.removalEvidence.Manifest(ctx, runtime.OperationID)
	if err != nil {
		return removal.InitialPublication{}, err
	}
	if found {
		// Own staging advances MVCC time, not the accepted source. Preserve the
		// original read boundary, then compare every reconstructed manifest field.
		if previous.ReadRevision > source.ReadRevision {
			return removal.InitialPublication{}, errs.New(
				errs.KindStateConflict,
				"Volume removal source read moved backwards",
			)
		}
		source.ReadRevision = previous.ReadRevision
	}
	manifest, rows, err := volumeRemovalEvidence(runtime, source)
	if err != nil {
		return removal.InitialPublication{}, err
	}
	if found && previous != manifest {
		return removal.InitialPublication{}, errs.New(errs.KindStateConflict, "Volume removal accepted source changed")
	}
	state, err := service.removalEvidence.Begin(ctx, manifest)
	if err != nil {
		return removal.InitialPublication{}, err
	}
	for state.Cursor.Record.CompletedRows < manifest.TotalRows {
		start := state.Cursor.Record.CompletedRows
		end := min(start+removal.EvidenceBatchRows, manifest.TotalRows)
		staged, err := service.removalEvidence.Stage(ctx, manifest, rows[start:end])
		if err != nil {
			return removal.InitialPublication{}, err
		}
		state = staged.State
	}
	sealed, err := service.removalEvidence.Seal(ctx, manifest)
	if err != nil {
		return removal.InitialPublication{}, err
	}
	runtime.EvidenceManifestSHA256 = sealed.Seal.Record.ManifestSHA256
	task.Params = etcd.EnvironmentVolumeRemovalTaskParams(runtime, 1)
	task.TimeoutSeconds = removal.TimeoutSeconds
	return removal.PrepareInitialPublication(runtime)
}
