package volume

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumework"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ReadRepository interface {
	ReadAppliedVolumes(context.Context, []string, int64) (map[string]volumework.AppliedVolume, error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetEnvironmentOwnedIdentities(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities], bool, error)
	GetEnvironmentOwnedIdentitiesRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities], bool, error)
	FindEnvironmentVolumeIdentity(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities], projectionrecord.EnvironmentVolumeIdentity, error)
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
	) (backupruntime.BackupVolumeRemovalImpact, error)
}

type MutationRepository interface {
	ReadRepository
	ResolveResourceHost(context.Context) (workexecution.Placement, error)
	GetEnvironmentDesiredInput(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput], bool, error)
	PublishVolumeWork(context.Context, etcd.VolumeWorkPublication) (etcd.IdempotencyTransactionResult, error)
	GetTenant(context.Context, string) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironmentBlueprintHead(
		context.Context,
		string,
	) (etcdstore.Versioned[blueprints.EnvironmentBlueprintHead], bool, error)
	ClaimEnvironmentBlueprintStage(
		context.Context,
		blueprints.EnvironmentBlueprintStageClaimRequest,
	) (blueprints.EnvironmentBlueprintStageClaim, error)
	StageEnvironmentBlueprintRevision(
		context.Context,
		blueprints.EnvironmentBlueprintStageRequest,
	) (blueprints.EnvironmentBlueprintSeal, error)
}

func desiredState(
	environmentID string,
	head etcdstore.Versioned[blueprints.EnvironmentBlueprintHead],
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
