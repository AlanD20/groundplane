package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintRuntimeProjectionSeal is the complete authority for publishing one
// derived runtime projection. The desired input and owned identities remain
// immutable revision records; this seal never advances applied runtime state.
type BlueprintRuntimeProjectionSeal struct {
	Parent                  TaskRecord
	ExpectedHeadRevision    int64
	ExpectedEpochRevision   int64
	DesiredRootRevision     int64
	OwnedIdentitiesRevision int64
	Tenant                  etcdstore.Versioned[hierarchyrecord.TenantRecord]
	Project                 etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	Environment             etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	Projection              projectionrecord.EnvironmentComposeProjection
}

// SealEnvironmentBlueprintRuntimeProjection publishes one complete immutable
// artifact only while the same desired head, coordinator claim, planning epoch,
// source revision and hierarchy are still current.
func (repository *EnvironmentBlueprintRepository) SealEnvironmentBlueprintRuntimeProjection(
	ctx context.Context,
	input BlueprintRuntimeProjectionSeal,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	parent := input.Parent
	projection := input.Projection
	if validateBlueprintParentClaimTask(parent) != nil ||
		parent.Status != taskjournal.TaskStatusRunning ||
		parent.Owner.EnvironmentID == "" ||
		parent.Params[blueprints.EnvironmentDesiredRevisionParam] != parent.ID ||
		projection.EnvironmentID != parent.Owner.EnvironmentID ||
		projection.RevisionID != parent.ID ||
		projection.RenderGeneration != uint64(parent.RenderGeneration) ||
		input.ExpectedHeadRevision <= 0 || input.ExpectedEpochRevision < 0 ||
		input.DesiredRootRevision <= 0 || input.OwnedIdentitiesRevision <= 0 ||
		input.Project.Revision <= 0 || input.Environment.Revision <= 0 {
		return errs.New(errs.KindValidationFailed, "Blueprint runtime projection seal authority is invalid")
	}
	if err := hierarchyrecord.ValidateProject(input.Project.Record); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateEnvironment(input.Environment.Record); err != nil {
		return err
	}
	expectedOwner, err := taskjournal.EnvironmentTaskOwner(input.Project.Record, input.Environment.Record)
	if err != nil {
		return err
	}
	if expectedOwner != parent.Owner {
		return errs.New(errs.KindStateConflict, "Blueprint runtime projection hierarchy changed")
	}
	switch input.Project.Record.Kind {
	case hierarchyrecord.ProjectKindTenant:
		if input.Tenant.Revision <= 0 || hierarchyrecord.ValidateTenant(input.Tenant.Record) != nil ||
			input.Project.Record.TenantID != input.Tenant.Record.ID {
			return errs.New(errs.KindStateConflict, "Blueprint runtime projection Tenant changed")
		}
	case hierarchyrecord.ProjectKindBacking:
		if input.Tenant.Revision != 0 || input.Tenant.Record.ID != "" || input.Project.Record.TenantID != "" {
			return errs.New(errs.KindStateConflict, "Blueprint runtime projection backing owner changed")
		}
	default:
		return errs.New(errs.KindStateConflict, "Blueprint runtime projection Project kind changed")
	}
	value, err := projectionrecord.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		return err
	}
	defer clear(value)
	parentKey := taskjournal.TaskStorageKey(parent.ID)
	claimKey := taskjournal.BlueprintParentClaimKey(parent.ID)
	headKey := blueprints.EnvironmentBlueprintHeadKey(projection.EnvironmentID)
	epochKey := blueprintunits.EpochKey(projection.EnvironmentID)
	stopKey := taskjournal.BlueprintParentAbortKey(parent.ID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{parentKey, claimKey}})
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil {
		return errs.New(errs.KindStateConflict, "Blueprint runtime projection parent claim is unavailable")
	}
	defer etcdstore.ClearValues(read.Values)
	storedParent, parentErr := DecodeTaskRecord(read.Values[0].Value)
	claimID, claimErr := idempotencyrecord.DecodeTaskReference(read.Values[1].Value)
	if parentErr != nil || claimErr != nil || claimID != parent.ID ||
		validateBlueprintParentClaimTask(storedParent) != nil ||
		storedParent.Status != taskjournal.TaskStatusRunning ||
		storedParent.Owner != parent.Owner || storedParent.ID != parent.ID ||
		storedParent.PlanID != parent.PlanID ||
		storedParent.RenderGeneration != parent.RenderGeneration ||
		storedParent.Params[blueprints.EnvironmentDesiredRevisionParam] != parent.ID {
		return errs.New(errs.KindStateConflict, "Blueprint runtime projection parent claim changed")
	}
	conditions := []etcdstore.Condition{
		{Key: blueprints.EnvironmentBlueprintEffectiveProjectionKey(projection.EnvironmentID, parent.ID)},
		{Key: headKey, ModRevision: input.ExpectedHeadRevision},
		{Key: epochKey, ModRevision: input.ExpectedEpochRevision},
		{Key: parentKey, ModRevision: read.Values[0].ModRevision},
		{Key: claimKey, ModRevision: read.Values[1].ModRevision},
		{Key: stopKey},
		{Key: blueprints.EnvironmentBlueprintRootKey(projection.EnvironmentID, parent.ID), ModRevision: input.DesiredRootRevision},
		{Key: blueprints.EnvironmentBlueprintOwnedIdentitiesKey(projection.EnvironmentID, parent.ID), ModRevision: input.OwnedIdentitiesRevision},
		{Key: hierarchyrecord.ProjectKey(input.Project.Record.ID), ModRevision: input.Project.Revision},
		{Key: hierarchyrecord.EnvironmentKey(input.Environment.Record.ID), ModRevision: input.Environment.Revision},
	}
	if input.Project.Record.Kind == hierarchyrecord.ProjectKindTenant {
		conditions = append(conditions, etcdstore.Condition{
			Key: hierarchyrecord.TenantKey(input.Tenant.Record.ID), ModRevision: input.Tenant.Revision,
		})
	}
	mutations := []etcdstore.Mutation{{
		Type:  etcdstore.MutationPut,
		Key:   blueprints.EnvironmentBlueprintEffectiveProjectionKey(projection.EnvironmentID, parent.ID),
		Value: value,
	}}
	result, err := executeEnvironmentBlueprintTransaction(ctx, repository.transactions, conditions, mutations)
	etcdstore.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Blueprint runtime projection seal raced")
	}
	return nil
}
