package etcd

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *AttachRepository) RenameAttachIdempotent(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[attachrecord.Record],
	name string,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := attachrecord.ValidateAttachVersion(current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replacement := current.Record
	replacement.Name = name
	if err := attachrecord.ValidateAttachRecord(replacement); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if environment.Record.ID != current.Record.EnvironmentID || project.Record.ID != environment.Record.ProjectID ||
		environment.Revision <= 0 || project.Revision <= 0 {
		return IdempotencyTransactionResult{}, errs.New(errs.KindScopeUnauthorized, "Attach rename scope is invalid")
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != current.Record.EnvironmentID || marker.ReplayTarget == nil ||
		marker.ReplayTarget.Kind != idempotencyrecord.IdempotencyReplayTargetAttach || marker.ReplayTarget.ID != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Attach rename marker must be a completed Environment-scoped direct mutation",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	mutationContext, err := environmentfence.LoadMutationContext(
		ctx,
		repository.store,
		current.Record.EnvironmentID,
		attachrecord.AttachKey(current.Record.ID),
		project.Record.ID,
		project.Record.TenantID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	secondary, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			attachrecord.AttachNameKey(current.Record.EnvironmentID, current.Record.Name),
			attachrecord.AttachOwnerKey(current.Record.EnvironmentID, current.Record.ID),
		},
		Revision: mutationContext.ReadRevision(),
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if len(secondary.Values) != 2 || secondary.Values[0] == nil ||
		string(secondary.Values[0].Value) != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Attach name index is missing or mismatched")
	}
	if secondary.Values[1] == nil || string(secondary.Values[1].Value) != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"Attach owner index is missing or mismatched",
		)
	}
	conditions := []etcdstore.Condition{
		{Key: attachrecord.AttachKey(current.Record.ID), ModRevision: current.Revision},
		{
			Key:         attachrecord.AttachNameKey(current.Record.EnvironmentID, current.Record.Name),
			ModRevision: secondary.Values[0].ModRevision,
		},
		{
			Key:         attachrecord.AttachOwnerKey(current.Record.EnvironmentID, current.Record.ID),
			ModRevision: secondary.Values[1].ModRevision,
		},
		{Key: hierarchyrecord.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletions.TombstoneKey("attach", current.Record.ID)},
		{Key: deletions.TombstoneKey("environment", environment.Record.ID)},
		{Key: deletions.TombstoneKey("project", project.Record.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, etcdstore.Condition{Key: deletions.TombstoneKey("tenant", project.Record.TenantID)})
	}
	renaming := replacement.Name != current.Record.Name
	newNameIndex := -1
	mutations := []etcdstore.Mutation(nil)
	if renaming {
		newNameIndex = len(conditions)
		conditions = append(conditions, etcdstore.Condition{Key: attachrecord.AttachNameKey(current.Record.EnvironmentID, replacement.Name)})
		value, encodeErr := attachrecord.EncodeAttachRecord(replacement)
		if encodeErr != nil {
			return IdempotencyTransactionResult{}, encodeErr
		}
		defer clear(value)
		mutations = []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(current.Record.ID), Value: value},
			{Type: etcdstore.MutationDelete, Key: attachrecord.AttachNameKey(current.Record.EnvironmentID, current.Record.Name)},
			{
				Type:  etcdstore.MutationPut,
				Key:   attachrecord.AttachNameKey(current.Record.EnvironmentID, replacement.Name),
				Value: []byte(current.Record.ID),
			},
		}
	}
	originalClassify := func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "Attach rename compare evidence is incomplete")
		}
		if values[0] == nil {
			return errs.New(errs.KindAttachNotFound, "Attach was not found")
		}
		if newNameIndex >= 0 && values[newNameIndex] != nil {
			return errs.New(errs.KindNameConflict, "Attach name already exists")
		}
		if values[5] != nil || values[6] != nil || values[7] != nil ||
			project.Record.TenantID != "" && values[8] != nil {
			return errs.New(errs.KindResourceInUse, "Attach ownership deletion is in progress")
		}
		if values[0].ModRevision != current.Revision {
			return errs.New(errs.KindStateConflict, "Attach changed concurrently")
		}
		if values[1] == nil || string(values[1].Value) != current.Record.ID ||
			values[2] == nil || string(values[2].Value) != current.Record.ID {
			return errs.New(errs.KindInternal, "Attach indexes are missing or mismatched")
		}
		return errs.New(errs.KindStateConflict, "Attach rename scope changed concurrently")
	}
	binding, err := mutationContext.Bind(ctx, repository.store, conditions, mutations, renaming)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer binding.Clear()
	defer etcdstore.ClearMutationValues(binding.Mutations())
	classify := func(revision int64, values []*etcdstore.KeyValue) error {
		return binding.ClassifyConflict(revision, values, originalClassify)
	}
	plan, err := NewIdempotencyMutationPlan(binding.Conditions(), binding.Mutations(), classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}
