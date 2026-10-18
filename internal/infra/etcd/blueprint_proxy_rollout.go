package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintProxyServiceRollout is supplemental native preparation, not a
// published Task. Large common Environment inputs are retained by reference;
// the stored Members and Hook sources omit that projection and are rehydrated
// exclusively from the candidate input's verified immutable chunks.
type BlueprintProxyServiceRollout struct {
	Input              componentplanning.ProxyCandidateInputReference        `json:"input"`
	WorkItemID         string                                                `json:"work_item_id"`
	OperationID        string                                                `json:"operation_id"`
	Manifest           releases.VersionedReleaseManifest                     `json:"manifest"`
	NativePredecessors []BlueprintNativePredecessorCapture                   `json:"native_predecessors"`
	Members            []releaserender.ReleaseTaskRenderMember               `json:"members"`
	Hooks              map[string]scriptsourcequeries.ScriptExecutionSources `json:"hooks"`
}

func BlueprintProxyServiceRolloutKey(environmentID, parentID, taskID string) string {
	return componentplanning.ProxyServiceRolloutKey(environmentID, parentID, taskID)
}

func (ledger *ReleaseLedger) StageBlueprintProxyServiceRollout(ctx context.Context,
	preparation BlueprintProxyServiceRollout, capture releaserender.ServiceRolloutCapture,
) (componentplanning.ProxyServiceRolloutReference, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	if ledger == nil || ledger.store == nil || preparation.Input.Validate() != nil ||
		ids.Validate(ids.KindWorkItem, preparation.WorkItemID) != nil ||
		ids.Validate(ids.KindOperation, preparation.OperationID) != nil ||
		capture.Scope.EnvironmentID != preparation.Input.EnvironmentID || capture.TaskID != preparation.Input.ParentTaskID ||
		capture.WorkItemID != preparation.WorkItemID || capture.OperationID != preparation.OperationID ||
		len(preparation.Members) != 1 || len(preparation.Manifest.Record.Members) != 1 ||
		preparation.Manifest.Revision <= 0 || preparation.Manifest.Record.OperationID != preparation.OperationID ||
		preparation.Members[0].Intent.ServiceID != capture.Render.ServiceID ||
		preparation.Manifest.Record.Members[0].ServiceID != capture.Render.ServiceID {
		return componentplanning.ProxyServiceRolloutReference{}, errs.New(errs.KindValidationFailed, "Pre-switch Service rollout preparation is invalid")
	}
	input, err := componentplanning.ReadProxyCandidateInput(ctx, ledger.store, preparation.Input, 0)
	if err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	if err := validateProxyRolloutCapture(preparation, capture, input.Record); err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	stored, err := ledger.StageServiceRollout(ctx, capture)
	if err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
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
		preparation.WorkItemID,
	)
	conditions := []keyvalue.Condition{
		{Key: key},
		{Key: componentplanning.ProxyCandidateInputKey(preparation.Input), ModRevision: input.Revision},
		{Key: releaserender.ServiceRolloutKey(preparation.Input.EnvironmentID, preparation.Input.ParentTaskID,
			preparation.WorkItemID), ModRevision: stored.Revision},
	}
	// The input exists before this inert preparation. Fence its real root revision,
	// rather than publishing the referenced projection as acknowledged runtime.
	guards, err := ledger.rolloutPreparationGuards(ctx, capture)
	if err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	conditions = append(conditions, guards...)
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
			result.FailureReads[2] == nil || result.FailureReads[2].ModRevision != stored.Revision ||
			!bytes.Equal(result.FailureReads[0].Value, value) {
			return componentplanning.ProxyServiceRolloutReference{}, errs.New(
				errs.KindStateConflict,
				"pre-switch Service rollout staging changed",
			)
		}
		revision = result.FailureReads[0].ModRevision
	}
	return componentplanning.ProxyServiceRolloutReference{
		ServiceID: preparation.Members[0].Intent.ServiceID, WorkItemID: preparation.WorkItemID,
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
) (BlueprintProxyServiceRollout, keyvalue.Versioned[releaserender.ServiceRolloutCapture], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.ServiceRolloutCapture]{}, err
	}
	if ledger == nil || ledger.store == nil || reference.Validate() != nil ||
		ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindTask, parentID) != nil {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.ServiceRolloutCapture]{}, errs.New(
			errs.KindValidationFailed,
			"pre-switch Service rollout read is invalid",
		)
	}
	key := BlueprintProxyServiceRolloutKey(environmentID, parentID, reference.WorkItemID)
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}})
	if err != nil {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.ServiceRolloutCapture]{}, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 || read.Values[0] == nil || read.Values[0].Key != key ||
		read.Values[0].ModRevision != reference.Revision || len(read.Values[0].Value) > keyvalue.MaximumBytes {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.ServiceRolloutCapture]{}, errs.New(
			errs.KindStateConflict,
			"pre-switch Service rollout preparation is missing or changed",
		)
	}
	digest := sha256.Sum256(read.Values[0].Value)
	if hex.EncodeToString(digest[:]) != reference.SHA256 {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.ServiceRolloutCapture]{}, errs.New(
			errs.KindStateConflict,
			"pre-switch Service source preparation digest changed",
		)
	}
	preparation, err := recordcodec.Decode[BlueprintProxyServiceRollout](
		read.Values[0].Value,
		"blueprint-proxy-service-rollout",
	)
	if err != nil {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.ServiceRolloutCapture]{}, err
	}
	if preparation.Input.Validate() != nil || preparation.Input.EnvironmentID != environmentID || preparation.Input.ParentTaskID != parentID ||
		preparation.WorkItemID != reference.WorkItemID || ids.Validate(ids.KindOperation, preparation.OperationID) != nil ||
		len(preparation.Members) != 1 || preparation.Members[0].Intent.ServiceID != reference.ServiceID ||
		len(preparation.Manifest.Record.Members) != 1 || preparation.Manifest.Revision <= 0 ||
		preparation.Manifest.Record.Members[0].ServiceID != reference.ServiceID ||
		preparation.Manifest.Record.OperationID != preparation.OperationID {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.ServiceRolloutCapture]{},
			errs.New(errs.KindStateConflict, "Pre-switch Service preparation scope changed")
	}
	input, err := componentplanning.ReadProxyCandidateInput(ctx, ledger.store, preparation.Input, read.ReadRevision)
	if err != nil {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.ServiceRolloutCapture]{}, err
	}
	for index := range preparation.Members {
		preparation.Members[index].Render.Projection = projectionrecord.CloneEnvironmentComposeProjection(input.Record)
	}
	for executionID, sources := range preparation.Hooks {
		sources.DesiredProjection.Record = projectionrecord.CloneEnvironmentComposeProjection(input.Record)
		sources.RenderInput.Record.Projection = projectionrecord.CloneEnvironmentComposeProjection(input.Record)
		preparation.Hooks[executionID] = sources
	}
	captureKey := releaserender.ServiceRolloutKey(environmentID, parentID, reference.WorkItemID)
	native, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{captureKey}, Revision: read.ReadRevision})
	if err != nil {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.ServiceRolloutCapture]{}, err
	}
	if native != nil {
		defer keyvalue.ClearValues(native.Values)
	}
	if native == nil || native.ReadRevision != read.ReadRevision || len(native.Values) != 1 {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.ServiceRolloutCapture]{},
			errs.New(errs.KindStateConflict, "Captured Service rollout read is incomplete")
	}
	value := native.Values[0]
	if value == nil || value.Key != captureKey || value.ModRevision != reference.NativeRevision {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.ServiceRolloutCapture]{},
			errs.New(errs.KindStateConflict, "Captured Service rollout revision changed")
	}
	record, err := releaserender.DecodeServiceRolloutCapture(value.Value)
	if err != nil {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.ServiceRolloutCapture]{}, err
	}
	if record.TaskID != parentID || record.WorkItemID != reference.WorkItemID ||
		record.Scope.EnvironmentID != environmentID || record.OperationID != preparation.OperationID ||
		record.Render.ServiceID != reference.ServiceID {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.ServiceRolloutCapture]{},
			errs.New(errs.KindStateConflict, "Captured Service rollout belongs to another work item")
	}
	capture := keyvalue.Versioned[releaserender.ServiceRolloutCapture]{
		Record: record, Revision: value.ModRevision, ReadRevision: native.ReadRevision}
	if err := validateProxyRolloutCapture(preparation, record, input.Record); err != nil {
		return BlueprintProxyServiceRollout{}, keyvalue.Versioned[releaserender.ServiceRolloutCapture]{}, err
	}
	return preparation, capture, nil
}

// ProxyServiceRolloutConditions joins the exact supplemental capture and its
// retained hook sources to native Release input publication.
func ProxyServiceRolloutConditions(input componentplanning.ProxyCandidateInputReference,
	reference componentplanning.ProxyServiceRolloutReference, task workexecution.Task, item workexecution.Item,
) ([]keyvalue.Condition, error) {
	if input.Validate() != nil || reference.Validate() != nil || task.Validate() != nil || item.Validate() != nil ||
		item.ID != reference.WorkItemID || item.TaskID != task.ID || item.Operation != workexecution.Release ||
		item.TargetID != reference.ServiceID || task.Scope.EnvironmentID != input.EnvironmentID || task.ID != input.ParentTaskID {
		return nil, errs.New(errs.KindValidationFailed, "Pre-switch Service capture differs from native work")
	}
	conditions := []keyvalue.Condition{{Key: BlueprintProxyServiceRolloutKey(input.EnvironmentID, input.ParentTaskID,
		reference.WorkItemID), ModRevision: reference.Revision}}
	if condition, held := reference.SourceHoldCondition(); held {
		conditions = append(conditions, condition)
	}
	return conditions, nil
}
