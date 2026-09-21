package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateBackingServicePublicationBudget(
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) error {
	return validateBackingServicePublicationOperationCounts(
		len(conditions),
		len(mutations),
		len(conditions),
	)
}

func validateBackingServicePublicationOperationCounts(
	comparisons int,
	successMutations int,
	failureReads int,
) error {
	selectedOperations := comparisons + successMutations
	requestOperations := selectedOperations + failureReads
	if selectedOperations > etcdstore.MaximumOperations ||
		requestOperations > maximumBackingServiceTransactionRequestOperations {
		return errs.Newf(
			errs.KindValidationFailed,
			"Backing-service publication exceeds transaction bounds (%d/%d/%d; selected %d/%d; request %d/%d)",
			comparisons,
			successMutations,
			failureReads,
			selectedOperations,
			etcdstore.MaximumOperations,
			requestOperations,
			maximumBackingServiceTransactionRequestOperations,
		)
	}
	return nil
}

func validateBackingServiceCreation(ctx context.Context, creation BackingServiceCreation) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateProject(creation.Project); err != nil {
		return err
	}
	if creation.Project.Kind != hierarchyrecord.ProjectKindBacking || creation.Project.TenantID != "" {
		return errs.New(errs.KindValidationFailed, "Backing-service Project ownership is invalid")
	}
	if err := validateBackingServiceCreationStage(creation.Stage.Record); err != nil {
		return err
	}
	if creation.Stage.Revision <= 0 || creation.Stage.ReadRevision < creation.Stage.Revision ||
		creation.Stage.Record.ProjectID != creation.Project.ID ||
		creation.Stage.Record.EnvironmentID != creation.Environment.ID ||
		creation.Stage.Record.TaskID != creation.Task.ID || creation.Stage.Record.Locator != creation.Marker.Locator {
		return errs.New(errs.KindValidationFailed, "Backing-service creation stage does not match publication")
	}
	if err := hierarchyrecord.ValidateEnvironment(creation.Environment); err != nil {
		return err
	}
	if creation.Environment.ProjectID != creation.Project.ID || creation.Environment.Name != "main" ||
		creation.Environment.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
		creation.Environment.CreateTaskID != creation.Task.ID ||
		!creation.Environment.CreatedAt.Equal(creation.Task.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Backing-service Environment lifecycle is invalid")
	}
	if err := hierarchyrecord.ValidateEnvironmentVolumeDir(creation.VolumeRoot, creation.Project, creation.Environment); err != nil {
		return err
	}
	if err := validateBackingServiceComponents(creation.Components); err != nil {
		return err
	}
	if creation.PoolRegistry.Revision < 0 ||
		creation.PoolRegistry.ReadRevision < creation.PoolRegistry.Revision ||
		creation.PoolRegistry.Record.Reservations[creation.Environment.ID] != creation.Environment.NetworkPool {
		return errs.New(errs.KindValidationFailed, "Backing-service Environment pool reservation is invalid")
	}
	if err := validateEnvironmentPoolRegistry(creation.PoolRegistry.Record); err != nil {
		return err
	}
	if err := zonerecord.ValidateRecord(creation.Zone); err != nil {
		return err
	}
	if creation.Zone.EnvironmentID != creation.Environment.ID ||
		creation.Zone.Desired.OwnerKind != "backing_project" ||
		creation.Zone.Desired.OwnerID != creation.Project.ID {
		return errs.New(errs.KindValidationFailed, "Backing-service Zone ownership is invalid")
	}
	if _, err := (zonePoolRegistry{Reservations: map[string]string{}}).reserve(creation.Environment, creation.Zone); err != nil {
		return err
	}
	if err := servicerecord.ValidateServiceRecord(creation.Service); err != nil {
		return err
	}
	if creation.Service.EnvironmentID != creation.Environment.ID || creation.Service.Desired.Adapter == "" ||
		creation.Service.BackingNetworkID != creation.Zone.Desired.ID {
		return errs.New(errs.KindValidationFailed, "Backing-service adapter Service is invalid")
	}
	customCreation := creation.Service.Desired.Adapter == "custom"
	if err := validateBackingServiceAdapterCreationShape(creation.Service.Desired, customCreation); err != nil {
		return err
	}
	if len(creation.Entries) != len(creation.EntryValues) || customCreation != (len(creation.Entries) == 0) {
		return errs.New(errs.KindValidationFailed, "Backing-service bootstrap entries are invalid")
	}
	seenEntries := make(map[string]struct{}, len(creation.Entries))
	for index, entry := range creation.Entries {
		if entry.EnvironmentID != creation.Environment.ID {
			return errs.New(errs.KindValidationFailed, "Backing-service bootstrap Entry ownership is invalid")
		}
		if _, exists := seenEntries[entry.Entry.ID]; exists {
			return errs.New(errs.KindValidationFailed, "Backing-service bootstrap Entry identity is duplicated")
		}
		seenEntries[entry.Entry.ID] = struct{}{}
		_, value, err := prepareEntryGeneration(entry, creation.EntryValues[index])
		if err != nil {
			return err
		}
		clear(value)
	}
	if len(creation.Secrets) != len(creation.SecretValues) || customCreation != (len(creation.Secrets) == 0) {
		return errs.New(errs.KindValidationFailed, "Backing-service bootstrap Secrets are invalid")
	}
	seenSecrets := make(map[string]struct{}, len(creation.Secrets))
	for index, secret := range creation.Secrets {
		if secret.Secret.ProjectID != creation.Project.ID || secret.Secret.Scope != "project" ||
			secret.Secret.ID != creation.SecretValues[index].SecretID {
			return errs.New(errs.KindValidationFailed, "Backing-service bootstrap Secret ownership is invalid")
		}
		if _, exists := seenSecrets[secret.Secret.ID]; exists {
			return errs.New(errs.KindValidationFailed, "Backing-service bootstrap Secret identity is duplicated")
		}
		seenSecrets[secret.Secret.ID] = struct{}{}
		if err := secretrecord.ValidateRecord(secret); err != nil {
			return err
		}
		if err := secretrecord.ValidateEncryptedValue(creation.SecretValues[index]); err != nil {
			return err
		}
	}
	if err := validateBackingServiceProjection(creation); err != nil {
		return err
	}
	wantOwner, err := taskjournal.EnvironmentTaskOwner(creation.Project, creation.Environment)
	if err != nil {
		return err
	}
	healthServiceID, hasHealthStep := creation.Task.Params[TaskBackingServiceHealthParam]
	desiredHealth := creation.Service.Desired.Healthcheck
	hasDesiredHealth := desiredHealth.HTTP != "" || desiredHealth.TCP != "" || desiredHealth.Pgrep != ""
	afterStartServiceID, hasAfterStartStep := creation.Task.Params[TaskBackingServiceAfterStartParam]
	hasDesiredAfterStart := creation.Service.Desired.Hooks != nil &&
		creation.Service.Desired.Hooks.AfterStart != nil
	if creation.Task.Owner != wantOwner || creation.Task.Actor != taskjournal.TaskActorOperator ||
		creation.Task.Executor != taskjournal.TaskExecutorAgent || creation.Task.Type != taskjournal.TaskUpdate ||
		creation.Task.Target != creation.Environment.ID || creation.Task.Status != taskjournal.TaskStatusPending ||
		creation.Task.RenderGeneration != 1 ||
		creation.Task.Params[EnvironmentDesiredRevisionParam] != creation.Task.ID ||
		creation.Task.Params[TaskMaterializationEnvironmentParam] != creation.Environment.ID ||
		creation.Task.Params[TaskBackingServiceCreationParam] != creation.Service.Desired.ID ||
		hasHealthStep != hasDesiredHealth || hasHealthStep && healthServiceID != creation.Service.Desired.ID ||
		hasAfterStartStep != hasDesiredAfterStart ||
		hasAfterStartStep && afterStartServiceID != creation.Service.Desired.ID ||
		creation.Task.Params[TaskBackingServiceVolumeDirectoryParam] != creation.Environment.VolumeDir {
		return errs.New(errs.KindValidationFailed, "Backing-service creation Task is invalid")
	}
	if creation.Marker.Kind != idempotencyrecord.IdempotencyMarkerTask || creation.Marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		creation.Marker.TaskID != creation.Task.ID ||
		creation.Marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopePlatform || creation.Marker.Locator.ScopeID != "-" ||
		!creation.Marker.CreatedAt.Equal(creation.Task.CreatedAt) ||
		!creation.Marker.UpdatedAt.Equal(creation.Marker.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Backing-service creation marker is invalid")
	}
	if err := validateTaskRecord(creation.Task); err != nil {
		return err
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(creation.Marker); err != nil {
		return err
	}
	return nil
}
