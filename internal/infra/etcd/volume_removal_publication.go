package etcd

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"

	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// volumeRemovalInitialPublication is a private fragment of the one desired
// publisher, not an independently executable removal transaction. Task and
// marker are supplied at construction so unrelated publication cannot reuse it.
type volumeRemovalInitialPublication struct {
	conditions []Condition
	mutations  []Mutation
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
	marker IdempotencyMarker,
) (volumeRemovalInitialPublication, error) {
	runtime, attempt, progress, err := initial.Records()
	if err != nil {
		return volumeRemovalInitialPublication{}, err
	}
	if err := validateVolumeRemovalInitialBinding(runtime, task, marker); err != nil {
		return volumeRemovalInitialPublication{}, err
	}
	runtimeValue, err := removalrecord.EncodeRuntime(runtime)
	if err != nil {
		return volumeRemovalInitialPublication{}, err
	}
	attemptValue, err := removalrecord.EncodeAttempt(attempt)
	if err != nil {
		clear(runtimeValue)
		return volumeRemovalInitialPublication{}, err
	}
	progressValue, err := removalrecord.EncodeProgress(progress)
	if err != nil {
		clear(runtimeValue)
		clear(attemptValue)
		return volumeRemovalInitialPublication{}, err
	}
	return volumeRemovalInitialPublication{
		// Any retained record excludes operation-id reuse, including orphaned
		// successor/completion evidence. One bounded prefix fence covers them.
		conditions: []Condition{{Key: removalrecord.Root(runtime.OperationID), Prefix: true}},
		mutations: []Mutation{
			{Type: MutationPut, Key: removalrecord.RuntimeKey(runtime.OperationID), Value: runtimeValue},
			{Type: MutationPut, Key: removalrecord.AttemptKey(runtime.OperationID, 1), Value: attemptValue},
			{Type: MutationPut, Key: removalrecord.ProgressKey(runtime.OperationID), Value: progressValue},
		},
	}, nil
}

func validateVolumeRemovalInitialBinding(
	runtime removalrecord.Runtime,
	task TaskRecord,
	marker IdempotencyMarker,
) error {
	if err := validateTaskRecord(task); err != nil {
		return err
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return err
	}
	if task.ID != runtime.OriginTaskID || task.OperationID != runtime.OperationID || task.RetryOf != "" ||
		task.Owner.EnvironmentID != runtime.EnvironmentID || task.Target != runtime.VolumeID ||
		task.Actor != TaskActorOperator || task.Executor != TaskExecutorAgent || task.Type != TaskRemove ||
		task.Status != TaskStatusPending || task.RenderGeneration != int32(runtime.DesiredGeneration) ||
		task.TimeoutSeconds != removalrecord.TimeoutSeconds || task.IdempotencyKey != runtime.RootLocator.Key ||
		len(task.Steps) != 1 || task.Steps[0].ID != runtime.StepID ||
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
	locator := IdempotencyLocator{
		ScopeKind: IdempotencyScopeKind(runtime.RootLocator.ScopeKind), ScopeID: runtime.RootLocator.ScopeID,
		Method: runtime.RootLocator.Method, Route: runtime.RootLocator.Route, Key: runtime.RootLocator.Key,
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != runtime.OriginTaskID || marker.Locator != locator ||
		marker.ReplayTarget == nil || marker.ReplayTarget.Kind != IdempotencyReplayTargetVolume ||
		marker.ReplayTarget.ID != runtime.VolumeID || marker.Response.Status != http.StatusAccepted ||
		!bytes.Equal(marker.Response.Body, []byte(`{"task_id":"`+runtime.OriginTaskID+`"}`)) ||
		sha256.Sum256(marker.Response.Body) != runtime.RootResponseSHA256 ||
		sha256.Sum256(marker.Intent.Ciphertext) != runtime.IntentSHA256 ||
		!marker.CreatedAt.Equal(runtime.CreatedAt) || !marker.UpdatedAt.Equal(runtime.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "initial Volume removal marker does not match its records")
	}
	return nil
}
