package secrets

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SecretOwner is a closed project-or-platform ownership input. A nil Project
// denotes platform scope.
type Owner struct {
	Project *etcdstore.Versioned[hierarchyrecord.ProjectRecord]
}

func ProjectOwner(project etcdstore.Versioned[hierarchyrecord.ProjectRecord]) Owner {
	return Owner{Project: &project}
}

func PlatformOwner() Owner { return Owner{} }

func ValidateSecretDeletionFence(value *etcdstore.KeyValue, secretID string) error {
	if value == nil {
		return errs.New(errs.KindInternal, "Secret deletion fence is missing")
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(value.Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetSecret || tombstone.TargetID != secretID ||
		tombstone.Phase != deletionrecord.DeletionPhaseFinalizing {
		return deletionrecord.CorruptDeletionTombstone()
	}
	return nil
}

func ValidateSecretValueBinding(record Record, value EncryptedValue) error {
	if err := ValidateEncryptedValue(value); err != nil {
		return err
	}
	if record.Secret.ID != value.SecretID {
		return errs.New(errs.KindValidationFailed, "Secret encrypted value does not match metadata")
	}
	return nil
}

func ValidateSecretOwnership(ctx context.Context, owner Owner, record Record) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := ValidateRecord(record); err != nil {
		return err
	}
	if owner.Project == nil {
		if record.Secret.Scope != core.SecretScopePlatform || record.Secret.ProjectID != "" {
			return errs.New(errs.KindValidationFailed, "Secret platform ownership is invalid")
		}
		return nil
	}
	if err := hierarchyrecord.ValidateProject(owner.Project.Record); err != nil {
		return err
	}
	if owner.Project.Revision <= 0 || owner.Project.ReadRevision < owner.Project.Revision ||
		record.Secret.Scope != core.SecretScopeProject ||
		record.Secret.ProjectID != owner.Project.Record.ID {
		return errs.New(errs.KindValidationFailed, "Secret project ownership or revision is invalid")
	}
	return nil
}

func ValidateSecretVersion(current etcdstore.Versioned[Record]) error {
	if err := ValidateRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Secret version metadata is invalid")
	}
	return nil
}

func ValidateSecretListScope(scope core.SecretScope, projectID string) error {
	switch scope {
	case core.SecretScopeProject:
		return recordcodec.ValidateID(ids.KindProject, projectID)
	case core.SecretScopePlatform:
		if projectID != "" {
			return errs.New(errs.KindValidationFailed, "Platform Secret list must not set project id")
		}
		return nil
	default:
		return errs.New(errs.KindValidationFailed, "Secret list scope is invalid")
	}
}

func SecretCreateConditions(owner Owner, record Record) []etcdstore.Condition {
	conditions := []etcdstore.Condition{
		{Key: RecordKey(record.Secret.ID)},
		{Key: SecretOwnerKey(record.Secret)},
		{Key: SecretScopedKey(record.Secret)},
		{Key: ValueKey(record.Secret.ID)},
	}
	if owner.Project != nil {
		conditions = append(conditions,
			etcdstore.Condition{Key: hierarchyrecord.ProjectKey(owner.Project.Record.ID), ModRevision: owner.Project.Revision},
		)
	}
	conditions = append(conditions, etcdstore.Condition{Key: deletionrecord.TombstoneKey("secret", record.Secret.ID)})
	if owner.Project != nil {
		conditions = append(conditions, etcdstore.Condition{Key: deletionrecord.TombstoneKey("project", owner.Project.Record.ID)})
		if owner.Project.Record.TenantID != "" {
			conditions = append(
				conditions,
				etcdstore.Condition{Key: deletionrecord.TombstoneKey("tenant", owner.Project.Record.TenantID)},
			)
		}
	}
	return conditions
}

func SecretDeleteConditions(
	owner Owner,
	current etcdstore.Versioned[Record],
	dependencies []*etcdstore.KeyValue,
) []etcdstore.Condition {
	conditions := []etcdstore.Condition{
		{Key: RecordKey(current.Record.Secret.ID), ModRevision: current.Revision},
		{Key: SecretOwnerKey(current.Record.Secret), ModRevision: dependencies[0].ModRevision},
		{Key: SecretScopedKey(current.Record.Secret), ModRevision: dependencies[1].ModRevision},
		{Key: ValueKey(current.Record.Secret.ID), ModRevision: dependencies[2].ModRevision},
	}
	if owner.Project != nil {
		conditions = append(conditions,
			etcdstore.Condition{Key: hierarchyrecord.ProjectKey(owner.Project.Record.ID), ModRevision: owner.Project.Revision},
		)
	}
	conditions = append(
		conditions,
		etcdstore.Condition{Key: deletionrecord.TombstoneKey("secret", current.Record.Secret.ID)},
	)
	if owner.Project != nil {
		conditions = append(conditions, etcdstore.Condition{Key: deletionrecord.TombstoneKey("project", owner.Project.Record.ID)})
		if owner.Project.Record.TenantID != "" {
			conditions = append(
				conditions,
				etcdstore.Condition{Key: deletionrecord.TombstoneKey("tenant", owner.Project.Record.TenantID)},
			)
		}
	}
	return conditions
}

func ClassifySecretCreateConflict(
	values []*etcdstore.KeyValue,
	owner Owner,
	record Record,
) error {
	expected := 5
	if owner.Project != nil {
		expected += 2
		if owner.Project.Record.TenantID != "" {
			expected++
		}
	}
	if len(values) != expected {
		return errs.New(errs.KindInternal, "Secret create compare evidence is incomplete")
	}
	if values[0] != nil || values[1] != nil || values[3] != nil {
		return errs.New(errs.KindStateConflict, "Secret stable identity is already in use")
	}
	if values[2] != nil {
		return errs.New(errs.KindNameConflict, "Secret key is already defined in this scope")
	}
	position := 4
	if owner.Project != nil {
		if values[position] == nil {
			return errs.New(errs.KindProjectNotFound, "Secret project was not found")
		}
		if values[position].ModRevision != owner.Project.Revision {
			return recordcodec.StateConflict("project", owner.Project.Record.ID)
		}
		position++
	}
	if values[position] != nil {
		return errs.New(errs.KindResourceInUse, "Secret deletion is in progress")
	}
	position++
	for ; position < len(values); position++ {
		if values[position] != nil {
			return errs.New(errs.KindResourceInUse, "Secret owner deletion is in progress")
		}
	}
	return recordcodec.StateConflict("secret", record.Secret.ID)
}
