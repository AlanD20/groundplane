package volume

import (
	"context"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ReadRepository interface {
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
	GetEnvironmentComposeProjectionRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
	FindEnvironmentVolume(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], projectionrecord.EnvironmentVolumeIdentity, error)
	ResolveEnvironmentVolumeAtRevision(
		context.Context,
		string,
		string,
		string,
		int64,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], projectionrecord.EnvironmentVolumeIdentity, error)
	ResolveVolumeRemovalImpactAtRevision(
		context.Context,
		string,
		string,
		int64,
		time.Time,
	) (etcd.BackupVolumeRemovalImpact, error)
}

type MutationRepository interface {
	ReadRepository
	PublishEnvironmentVolumeRemovalWithTask(
		context.Context,
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		int64,
		etcd.EnvironmentBlueprintStageClaim,
		projectionrecord.EnvironmentComposeProjection,
		etcd.VolumeRemovalBackupPolicyPreparation,
		removal.InitialPublication,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	GetTenant(context.Context, string) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironmentBlueprintHead(
		context.Context,
		string,
	) (etcdstore.Versioned[etcd.EnvironmentBlueprintHead], bool, error)
	ClaimEnvironmentBlueprintStage(
		context.Context,
		etcd.EnvironmentBlueprintStageClaimRequest,
	) (etcd.EnvironmentBlueprintStageClaim, error)
	StageEnvironmentBlueprintRevision(
		context.Context,
		etcd.EnvironmentBlueprintStageRequest,
	) (etcd.EnvironmentBlueprintSeal, error)
	PublishEnvironmentDesiredRevisionWithTask(
		context.Context,
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		int64,
		etcd.EnvironmentBlueprintStageClaim,
		etcd.EnvironmentDesiredRevisionIdentity,
		projectionrecord.EnvironmentComposeProjection,
		[]etcd.EnvironmentBlueprintZoneChange,
		[]etcd.EnvironmentBlueprintServiceChange,
		[]etcd.EnvironmentBlueprintRouteChange,
		etcd.ReleaseGroupBlueprintPreparedMutation,
		etcd.ComponentTaskPreparation,
		etcd.BlueprintAttachTaskPreparation,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

func desiredState(
	environmentID string,
	head etcdstore.Versioned[etcd.EnvironmentBlueprintHead],
	hasHead bool,
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	hasProjection bool,
) (int64, uint64, error) {
	if hasHead != hasProjection {
		return 0, 0, errs.New(errs.KindInternal, "Environment desired-state pointers are inconsistent")
	}
	if !hasHead {
		return 0, 1, nil
	}
	if head.Record.EnvironmentID != environmentID || projection.Record.EnvironmentID != environmentID ||
		head.Record.RevisionID != projection.Record.RevisionID || head.Revision <= 0 ||
		projection.Revision != head.Revision || projection.Record.RenderGeneration == math.MaxUint64 {
		return 0, 0, errs.New(errs.KindInternal, "Environment desired-state pointers are corrupt")
	}
	return head.Revision, projection.Record.RenderGeneration + 1, nil
}
