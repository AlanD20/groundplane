package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateScriptMutationMarker(marker idempotencyrecord.IdempotencyMarker, environmentID string) error {
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment || marker.Locator.ScopeID != environmentID {
		return errs.New(
			errs.KindValidationFailed,
			"Script mutation marker must be a completed Environment-scoped direct mutation",
		)
	}
	return idempotencyrecord.ValidateIdempotencyMarker(marker)
}

func scriptWriteConditions(
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[ServiceRecord],
	record scriptrecord.Record,
	current *etcdstore.Versioned[scriptrecord.Record],
	active etcdstore.Versioned[scriptrecord.SetGenerationRecord],
	ownerRevision int64,
	slugRevision int64,
) []etcdstore.Condition {
	scriptCondition := etcdstore.Condition{
		Key: scriptrecord.ScriptSetScriptKey(record.EnvironmentID, active.Record.GenerationID, record.Desired.ID),
	}
	ownerCondition := etcdstore.Condition{
		Key: scriptrecord.ScriptSetOwnerKey(record.EnvironmentID, active.Record.GenerationID, record.Desired.ID),
	}
	slugCondition := etcdstore.Condition{
		Key: scriptrecord.ScriptSetSlugKey(record.EnvironmentID, active.Record.GenerationID, record.Desired.Slug),
	}
	if current != nil {
		scriptCondition.ModRevision = current.Revision
		ownerCondition.ModRevision = ownerRevision
		slugCondition.ModRevision = slugRevision
	}
	conditions := []etcdstore.Condition{
		scriptCondition,
		ownerCondition,
		slugCondition,
		{Key: hierarchyrecord.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		serviceDesiredCondition(target),
		{Key: deletionTombstoneKey("script", record.Desired.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
		{Key: deletionTombstoneKey("service", target.Record.Desired.ID)},
		{Key: scriptrecord.ScriptSetActiveKey(record.EnvironmentID), ModRevision: active.Revision},
	}
	if current == nil {
		conditions = append(conditions,
			etcdstore.Condition{Key: scriptrecord.ScriptLocatorKey(record.Desired.ID)},
			etcdstore.Condition{Key: scriptrecord.ScriptEnvironmentLocatorKey(record.EnvironmentID, record.Desired.ID)},
		)
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, etcdstore.Condition{Key: deletionTombstoneKey("tenant", project.Record.TenantID)})
	}
	return conditions
}

func validateScriptHierarchy(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[ServiceRecord],
	record scriptrecord.Record,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateEnvironment(environment.Record); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateProject(project.Record); err != nil {
		return err
	}
	if err := validateServiceVersion(target); err != nil {
		return err
	}
	if target.Record.Desired.Replicas < 1 {
		return errs.New(errs.KindValidationFailed, "Script target Service must have positive replicas")
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant || target.Record.Desired.Adapter != "" ||
		target.Record.BackingNetworkID != "" {
		return errs.New(errs.KindValidationFailed, "Script target must be an operator-owned Service")
	}
	if err := scriptrecord.ValidateRecord(record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision || project.Revision <= 0 ||
		project.ReadRevision < project.Revision || record.EnvironmentID != environment.Record.ID ||
		environment.Record.ProjectID != project.Record.ID || target.Record.EnvironmentID != environment.Record.ID ||
		target.Record.Desired.ID != record.ServiceID || target.Record.Desired.Name != record.Desired.ServiceName {
		return errs.New(errs.KindValidationFailed, "Script hierarchy or target Service is invalid")
	}
	return nil
}

func validateScriptVersion(current etcdstore.Versioned[scriptrecord.Record]) error {
	if err := scriptrecord.ValidateRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Script version metadata is invalid")
	}
	return nil
}
