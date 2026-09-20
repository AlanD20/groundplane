package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"net/http"
	"net/netip"
	"strconv"

	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// volumeRemovalInitialPublication is a private fragment of the one desired
// publisher, not an independently executable removal transaction. Task and
// marker are supplied at construction so unrelated publication cannot reuse it.
type volumeRemovalInitialPublication struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

// PublishEnvironmentVolumeRemovalWithTask is the closed Volume entry into the
// sole desired publisher. It requires prepared policy and initial operation
// authority; evidence, ownership and Task writes remain one transaction.
func (repository *EnvironmentBlueprintRepository) PublishEnvironmentVolumeRemovalWithTask(
	ctx context.Context,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord], environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	expectedHeadRevision int64, claim EnvironmentBlueprintStageClaim,
	projection EnvironmentComposeProjection, policy VolumeRemovalBackupPolicyPreparation,
	initial removalrecord.InitialPublication, task TaskRecord, marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	return repository.publishEnvironmentDesiredRevisionWithTask(
		ctx,
		netip.Prefix{},
		environment.Record.NetworkPool,
		project,
		environment,
		expectedHeadRevision,
		claim,
		EnvironmentDesiredRevisionIdentity{EnvironmentID: claim.EnvironmentID, RevisionID: claim.RevisionID},
		projection,
		nil,
		nil,
		nil,
		ReleaseGroupBlueprintPreparedMutation{},
		ComponentTaskPreparation{},
		BlueprintAttachTaskPreparation{},
		BlueprintBackupPolicyPreparation{},
		BlueprintScriptPublication{},
		BlueprintReleasePublication{},
		BlueprintRequirementGate{},
		policy,
		&initial,
		task,
		marker,
		repository.transactions,
	)
}

func (repository *HierarchyRepository) prepareVolumeRemovalDesiredPublication(
	ctx context.Context,
	initial removalrecord.InitialPublication,
	claim EnvironmentBlueprintStageClaim,
	projection EnvironmentComposeProjection,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
	removedVolumeID string,
	readRevision int64,
) (volumeRemovalInitialPublication, error) {
	runtime, _, _, err := initial.Records()
	if err != nil {
		return volumeRemovalInitialPublication{}, err
	}
	if claim.SourceKind != EnvironmentBlueprintSourceMutation || runtime.VolumeID != removedVolumeID ||
		runtime.EnvironmentID != projection.EnvironmentID || runtime.DesiredRevisionID != projection.RevisionID ||
		runtime.DesiredGeneration != uint64(projection.RenderGeneration) {
		return volumeRemovalInitialPublication{}, errs.New(
			errs.KindValidationFailed,
			"Volume removal records do not match desired publication",
		)
	}
	previous, found, err := repository.getEnvironmentBlueprintProjectionAtRevision(
		ctx,
		runtime.EnvironmentID,
		readRevision,
	)
	if err != nil {
		return volumeRemovalInitialPublication{}, err
	}
	if found {
		for _, volume := range previous.Record.Volumes {
			if volume.ID == runtime.VolumeID && volume.Key == runtime.Key {
				evidenceConditions, err := repository.volumeRemovalEvidenceConditions(
					ctx,
					runtime,
					previous.Record.RevisionID,
					readRevision,
				)
				if err != nil {
					return volumeRemovalInitialPublication{}, err
				}
				lockValue, err := removalrecord.EncodeOwner(removalrecord.Owner{
					VolumeID: runtime.VolumeID, EnvironmentID: runtime.EnvironmentID, OperationID: runtime.OperationID,
				})
				if err != nil {
					return volumeRemovalInitialPublication{}, err
				}
				publication, err := prepareVolumeRemovalInitialPublication(initial, task, marker)
				if err != nil {
					clear(lockValue)
					return volumeRemovalInitialPublication{}, err
				}
				lockKey := removalrecord.EnvironmentLockKey(runtime.EnvironmentID)
				publication.conditions = append(publication.conditions,
					etcdstore.Condition{Key: taskMaterializationWriterKey(runtime.EnvironmentID)}, etcdstore.Condition{Key: lockKey},
				)
				publication.conditions = append(publication.conditions, evidenceConditions...)
				publication.mutations = append(publication.mutations, etcdstore.Mutation{
					Type: etcdstore.MutationPut, Key: lockKey, Value: lockValue,
				})
				ancestry, err := bindHierarchyMutation(ctx, repository.store, readRevision,
					HierarchyMutationScope{TenantID: task.Owner.TenantID, ProjectID: task.Owner.ProjectID}, nil, nil)
				if err != nil {
					clearBackupRuntimeMutations(publication.mutations)
					return volumeRemovalInitialPublication{}, err
				}
				// The combined desired publisher owns and clears these epoch values.
				publication.conditions = append(publication.conditions, ancestry.conditions...)
				publication.mutations = append(publication.mutations, ancestry.mutations...)
				return publication, nil
			}
		}
	}
	return volumeRemovalInitialPublication{}, errs.New(
		errs.KindValidationFailed,
		"Volume removal immutable key does not match desired baseline",
	)
}

func (publication volumeRemovalInitialPublication) classifyConflict(
	baseCount int,
	previous idempotencyPlanClassifier,
) idempotencyPlanClassifier {
	return func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) != baseCount+len(publication.conditions) {
			return errs.New(errs.KindInternal, "Volume removal publication compare evidence is incomplete")
		}
		if err := previous(revision, values[:baseCount]); err != nil {
			return err
		}
		for index, value := range values[baseCount:] {
			if !conditionMatchesRead(publication.conditions[index], value) {
				return errs.New(errs.KindStateConflict, "Volume removal publication authority changed")
			}
		}
		return nil
	}
}

// EnvironmentVolumeRemovalTaskParams is the single closed parameter contract
// shared by initial publication and runtime attempt validation. The caller
// validates the runtime and attempt ownership; this function only renders their
// immutable references plus standard Task routing/materialization authority.
func EnvironmentVolumeRemovalTaskParams(runtime removalrecord.Runtime, attemptOrdinal uint32) map[string]string {
	return map[string]string{
		TaskResourceKindParam:               TaskResourceVolume,
		TaskMaterializationEnvironmentParam: runtime.EnvironmentID,
		EnvironmentDesiredRevisionParam:     runtime.DesiredRevisionID,
		removalrecord.EnvironmentParam:      runtime.EnvironmentID,
		removalrecord.OriginTaskParam:       runtime.OriginTaskID,
		removalrecord.AttemptParam:          strconv.FormatUint(uint64(attemptOrdinal), 10),
		removalrecord.KeyParam:              runtime.Key,
		removalrecord.ImpactParam:           hex.EncodeToString(runtime.ImpactSHA256[:]),
		removalrecord.ManifestParam:         hex.EncodeToString(runtime.EvidenceManifestSHA256[:]),
		removalrecord.IntentParam:           hex.EncodeToString(runtime.IntentSHA256[:]),
	}
}

func prepareVolumeRemovalInitialPublication(
	initial removalrecord.InitialPublication,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (volumeRemovalInitialPublication, error) {
	runtime, attempt, progress, err := initial.Records()
	if err != nil {
		return volumeRemovalInitialPublication{}, err
	}
	if err := validateVolumeRemovalInitialBinding(runtime, task, marker); err != nil {
		return volumeRemovalInitialPublication{}, err
	}
	ownerValue, err := removalrecord.EncodeOwner(removalrecord.Owner{
		VolumeID: runtime.VolumeID, EnvironmentID: runtime.EnvironmentID, OperationID: runtime.OperationID,
	})
	if err != nil {
		return volumeRemovalInitialPublication{}, err
	}
	runtimeValue, err := removalrecord.EncodeRuntime(runtime)
	if err != nil {
		clear(ownerValue)
		return volumeRemovalInitialPublication{}, err
	}
	attemptValue, err := removalrecord.EncodeAttempt(attempt)
	if err != nil {
		clear(ownerValue)
		clear(runtimeValue)
		return volumeRemovalInitialPublication{}, err
	}
	progressValue, err := removalrecord.EncodeProgress(progress)
	if err != nil {
		clear(ownerValue)
		clear(runtimeValue)
		clear(attemptValue)
		return volumeRemovalInitialPublication{}, err
	}
	return volumeRemovalInitialPublication{
		// Any retained record excludes operation-id reuse, including orphaned
		// successor/completion evidence. One bounded prefix fence covers them.
		conditions: []etcdstore.Condition{
			{Key: removalrecord.Root(runtime.OperationID), Prefix: true},
			{Key: removalrecord.OwnerKey(runtime.VolumeID)},
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: removalrecord.RuntimeKey(runtime.OperationID), Value: runtimeValue},
			{Type: etcdstore.MutationPut, Key: removalrecord.AttemptKey(runtime.OperationID, 1), Value: attemptValue},
			{Type: etcdstore.MutationPut, Key: removalrecord.ProgressKey(runtime.OperationID), Value: progressValue},
			{Type: etcdstore.MutationPut, Key: removalrecord.OwnerKey(runtime.VolumeID), Value: ownerValue},
		},
	}, nil
}

func validateVolumeRemovalInitialBinding(
	runtime removalrecord.Runtime,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) error {
	if err := validateTaskRecord(task); err != nil {
		return err
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return err
	}
	if task.ID != runtime.OriginTaskID || task.OperationID != runtime.OperationID || task.RetryOf != "" ||
		task.Owner.EnvironmentID != runtime.EnvironmentID || task.Target != runtime.VolumeID ||
		task.Actor != TaskActorOperator || task.Executor != TaskExecutorAgent || task.Type != TaskRemove ||
		task.Status != TaskStatusPending || task.RenderGeneration != int32(runtime.DesiredGeneration) ||
		task.TimeoutSeconds != removalrecord.TimeoutSeconds || task.IdempotencyKey != runtime.RootLocator.Key ||
		!EnvironmentVolumeRemovalStepMatches(task.Steps, runtime.StepID) ||
		!task.CreatedAt.Equal(runtime.CreatedAt) || !task.UpdatedAt.Equal(runtime.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "initial Volume removal Task does not match its records")
	}
	expectedParams := EnvironmentVolumeRemovalTaskParams(runtime, 1)
	if len(task.Params) != len(expectedParams) {
		return errs.New(errs.KindValidationFailed, "initial Volume removal Task parameters changed")
	}
	for key, value := range expectedParams {
		if task.Params[key] != value {
			return errs.New(errs.KindValidationFailed, "initial Volume removal Task parameters changed")
		}
	}
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeKind(runtime.RootLocator.ScopeKind), ScopeID: runtime.RootLocator.ScopeID,
		Method: runtime.RootLocator.Method, Route: runtime.RootLocator.Route, Key: runtime.RootLocator.Key,
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != runtime.OriginTaskID || marker.Locator != locator ||
		marker.ReplayTarget == nil || marker.ReplayTarget.Kind != idempotencyrecord.IdempotencyReplayTargetVolume ||
		marker.ReplayTarget.ID != runtime.VolumeID || marker.Response.Status != http.StatusAccepted ||
		!bytes.Equal(marker.Response.Body, []byte(`{"task_id":"`+runtime.OriginTaskID+`"}`)) ||
		sha256.Sum256(marker.Response.Body) != runtime.RootResponseSHA256 ||
		sha256.Sum256(marker.Intent.Ciphertext) != runtime.IntentSHA256 ||
		!marker.CreatedAt.Equal(runtime.CreatedAt) || !marker.UpdatedAt.Equal(runtime.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "initial Volume removal marker does not match its records")
	}
	return nil
}

// EnvironmentVolumeRemovalStepMatches binds the path checkpoint to the final
// step. Detachment and Docker removal may precede it; plan reconstruction
// validates their exact payloads against the sealed source projection.
func EnvironmentVolumeRemovalStepMatches(steps []TaskStepRecord, pathStepID string) bool {
	return len(steps) >= 1 && len(steps) <= 3 && steps[len(steps)-1].ID == pathStepID
}
