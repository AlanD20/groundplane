package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintProxyServiceRollout is supplemental native preparation, not a
// published Task. Large common Environment inputs are retained by reference;
// the stored Members and Hook sources omit that projection and are rehydrated
// exclusively from the candidate input's verified immutable chunks.
type BlueprintProxyServiceRollout struct {
	Input              componentplanning.ProxyCandidateInputReference        `json:"input"`
	Task               TaskRecord                                            `json:"task"`
	Manifest           releases.VersionedReleaseManifest                     `json:"manifest"`
	NativePredecessors []BlueprintNativePredecessorCapture                   `json:"native_predecessors"`
	Members            []releaserender.ReleaseTaskRenderMember               `json:"members"`
	Hooks              map[string]scriptsourcequeries.ScriptExecutionSources `json:"hooks"`
}

func BlueprintProxyServiceRolloutKey(environmentID, parentID, taskID string) string {
	return componentplanning.ProxyServiceRolloutKey(environmentID, parentID, taskID)
}

func (ledger *ReleaseLedger) StageBlueprintProxyServiceRollout(ctx context.Context,
	preparation BlueprintProxyServiceRollout, capture releaserender.BlueprintServiceRolloutCapture,
) (componentplanning.ProxyServiceRolloutReference, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	if ledger == nil || ledger.store == nil || preparation.Input.Validate() != nil ||
		preparation.Task.Owner.EnvironmentID != preparation.Input.EnvironmentID ||
		preparation.Task.Params[taskjournal.TaskBlueprintParentParam] != preparation.Input.ParentTaskID ||
		preparation.Task.Status != taskjournal.TaskStatusPending || preparation.Task.Configuration != nil ||
		preparation.Task.Materializations != nil || len(preparation.Members) != 1 ||
		len(preparation.Manifest.Record.Members) != 1 || preparation.Manifest.Revision <= 0 ||
		preparation.Manifest.Record.OperationID != preparation.Task.OperationID {
		return componentplanning.ProxyServiceRolloutReference{}, errs.New(
			errs.KindValidationFailed,
			"pre-switch Service rollout preparation is invalid",
		)
	}
	if err := validateBlueprintServiceRolloutTask(capture, preparation.Task); err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	stored, err := ledger.StageBlueprintServiceRollout(ctx, capture)
	if err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	defer clear(stored.Record.NativePlan)
	value, err := recordcodec.Encode("blueprint-proxy-service-rollout", preparation)
	if err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	defer clear(value)
	if len(value) > keyvalue.MaximumBytes {
		return componentplanning.ProxyServiceRolloutReference{}, errs.New(
			errs.KindReleasePlanTooLarge,
			"pre-switch Service source preparation exceeds the storage request budget",
		)
	}
	digest := sha256.Sum256(value)
	key := BlueprintProxyServiceRolloutKey(
		preparation.Input.EnvironmentID,
		preparation.Input.ParentTaskID,
		preparation.Task.ID,
	)
	conditions := []keyvalue.Condition{
		{Key: key},
		{Key: componentplanning.ProxyCandidateInputKey(preparation.Input), ModRevision: 0},
	}
	// The input exists before this inert preparation. Fence its real root revision,
	// rather than publishing the referenced projection as acknowledged runtime.
	input, err := componentplanning.ReadProxyCandidateInput(ctx, ledger.store, preparation.Input, 0)
	if err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	conditions[1].ModRevision = input.Revision
	result, err := ledger.store.Transact(
		ctx,
		conditions,
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}},
	)
	if err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	defer keyvalue.ClearValues(result.FailureReads)
	revision := result.Revision
	if !result.Succeeded {
		if len(result.FailureReads) != len(conditions) || result.FailureReads[0] == nil ||
			result.FailureReads[1] == nil || result.FailureReads[1].ModRevision != input.Revision ||
			!bytes.Equal(result.FailureReads[0].Value, value) {
			return componentplanning.ProxyServiceRolloutReference{}, errs.New(
				errs.KindStateConflict,
				"pre-switch Service rollout staging changed",
			)
		}
		revision = result.FailureReads[0].ModRevision
	}
	return componentplanning.ProxyServiceRolloutReference{
		ServiceID: preparation.Members[0].Intent.ServiceID, TaskID: preparation.Task.ID,
		SHA256: hex.EncodeToString(digest[:]), Revision: revision, NativeRevision: stored.Revision,
	}, nil
}

func (ledger *ReleaseLedger) ReadBlueprintProxyCandidateInput(ctx context.Context,
	reference componentplanning.ProxyCandidateInputReference, revision int64,
) (keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection], error) {
	return componentplanning.ReadProxyCandidateInput(ctx, ledger.store, reference, revision)
}

// ReadBlueprintProxyServiceRollout consumes the exact preparation referenced by
// the router recipe. No newer Service, Script set, mutable image tag or desired
// head can substitute for these captured values after router acknowledgement.
func (ledger *ReleaseLedger) ReadBlueprintProxyServiceRollout(ctx context.Context,
	environmentID, parentID string, reference componentplanning.ProxyServiceRolloutReference,
) (BlueprintProxyServiceRollout, keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	if ledger == nil || ledger.store == nil || reference.Validate() != nil {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, errs.New(
			errs.KindValidationFailed,
			"pre-switch Service rollout read is invalid",
		)
	}
	key := BlueprintProxyServiceRolloutKey(environmentID, parentID, reference.TaskID)
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}})
	if err != nil {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil || read.Values[0].Key != key ||
		read.Values[0].ModRevision != reference.Revision {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, errs.New(
			errs.KindStateConflict,
			"pre-switch Service rollout preparation is missing or changed",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	digest := sha256.Sum256(read.Values[0].Value)
	if hex.EncodeToString(digest[:]) != reference.SHA256 {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, errs.New(
			errs.KindStateConflict,
			"pre-switch Service source preparation digest changed",
		)
	}
	preparation, err := recordcodec.Decode[BlueprintProxyServiceRollout](
		read.Values[0].Value,
		"blueprint-proxy-service-rollout",
	)
	if err != nil {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	if preparation.Input.EnvironmentID != environmentID || preparation.Input.ParentTaskID != parentID ||
		preparation.Task.ID != reference.TaskID || preparation.Task.Status != taskjournal.TaskStatusPending ||
		preparation.Task.Configuration != nil || preparation.Task.Materializations != nil ||
		len(preparation.Members) != 1 || preparation.Members[0].Intent.ServiceID != reference.ServiceID ||
		preparation.Manifest.Record.OperationID != preparation.Task.OperationID {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, errs.New(
			errs.KindStateConflict,
			"pre-switch Service preparation scope changed",
		)
	}
	input, err := componentplanning.ReadProxyCandidateInput(ctx, ledger.store, preparation.Input, read.ReadRevision)
	if err != nil {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	for index := range preparation.Members {
		preparation.Members[index].Render.Projection = projectionrecord.CloneEnvironmentComposeProjection(input.Record)
	}
	for executionID, sources := range preparation.Hooks {
		sources.DesiredProjection.Record = projectionrecord.CloneEnvironmentComposeProjection(input.Record)
		sources.RenderInput.Record.Projection = projectionrecord.CloneEnvironmentComposeProjection(input.Record)
		preparation.Hooks[executionID] = sources
	}
	capture, err := ledger.GetBlueprintServiceRollout(ctx, preparation.Task)
	if err != nil {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	if capture.Revision != reference.NativeRevision {
		clear(capture.Record.NativePlan)
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, errs.New(
			errs.KindStateConflict,
			"pre-switch native Service rollout revision changed",
		)
	}
	return preparation, capture, nil
}

func (publication BlueprintReleasePublication) WithProxyServiceRolloutCapture(
	input componentplanning.ProxyCandidateInputReference,
	reference componentplanning.ProxyServiceRolloutReference, task TaskRecord,
) (BlueprintReleasePublication, error) {
	if input.Validate() != nil || reference.Validate() != nil || task.ID != reference.TaskID ||
		task.Owner.EnvironmentID != input.EnvironmentID || task.Params[taskjournal.TaskBlueprintParentParam] != input.ParentTaskID ||
		publication.environmentID != input.EnvironmentID || publication.operationID != task.OperationID {
		return BlueprintReleasePublication{}, errs.New(
			errs.KindValidationFailed,
			"pre-switch Service capture differs from its final publication",
		)
	}
	publication.conditions = append(slices.Clone(publication.conditions), keyvalue.Condition{
		Key: BlueprintProxyServiceRolloutKey(
			input.EnvironmentID,
			input.ParentTaskID,
			reference.TaskID,
		), ModRevision: reference.Revision,
	})
	if condition, held := reference.SourceHoldCondition(); held {
		publication.conditions = append(publication.conditions, condition)
	}
	return publication, nil
}
