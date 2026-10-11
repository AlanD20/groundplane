package etcd

import (
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (repository *TaskRepository) prepareReleaseNetworkPublication(
	evidence ReleasePublicationEvidence,
) (releaseHookPublicationFragment, string, error) {
	var source *agentpb.ComposeArtifact
	for _, artifact := range evidence.Plan.GetArtifacts() {
		if artifact.GetArtifactId() == evidence.Task.Params[taskjournal.TaskComposeArtifactParam] {
			source = artifact
			break
		}
	}
	artifactID := ids.DeriveAt(ids.KindConfig, evidence.PublishedAt, evidence.Task.PlanID, "release-network-admission")
	if evidence.NetworkTask != nil {
		planTime, err := ids.Timestamp(ids.KindPlan, evidence.NetworkTask.PlanID)
		if err != nil {
			return releaseHookPublicationFragment{}, "", err
		}
		artifactID = ids.DeriveAt(ids.KindConfig, planTime, evidence.NetworkTask.PlanID, "release-network-preparation")
	}
	preparation, err := executionplan.CaptureReleaseNetworkPreparation(source, artifactID)
	if err != nil {
		return releaseHookPublicationFragment{}, "", err
	}
	if preparation.Artifact == nil {
		if evidence.NetworkTask != nil || evidence.NetworkPlan != nil ||
			evidence.Task.Params[taskjournal.TaskReleaseNetworkChildParam] != "" {
			return releaseHookPublicationFragment{}, "", errs.New(
				errs.KindValidationFailed,
				"release has an unnecessary network prerequisite",
			)
		}
		return releaseHookPublicationFragment{}, "", nil
	}
	if evidence.NetworkTask == nil || evidence.NetworkPlan == nil {
		return releaseHookPublicationFragment{}, "", errs.New(
			errs.KindValidationFailed,
			"release DNS network prerequisite is missing",
		)
	}
	child := cloneTaskRecord(*evidence.NetworkTask)
	parent := evidence.Task
	if child.Owner != parent.Owner || child.Target != parent.Owner.EnvironmentID ||
		parent.Params[taskjournal.TaskReleaseNetworkChildParam] != child.ID ||
		child.Params[taskjournal.TaskReleaseNetworkParentParam] != parent.ID || child.RenderGeneration != parent.RenderGeneration ||
		child.Status != taskjournal.TaskStatusPending || child.TimeoutSeconds != parent.TimeoutSeconds ||
		!child.CreatedAt.Equal(parent.CreatedAt) || child.OperationID == parent.OperationID ||
		child.PlanID == parent.PlanID || len(child.Materializations) != 0 || child.Configuration != nil || child.EntryRuntime != nil {
		return releaseHookPublicationFragment{}, "", errs.New(
			errs.KindValidationFailed,
			"release network prerequisite authority is invalid",
		)
	}
	expected, err := executionplan.BuildReleaseNetworkPreparation(
		child.PlanID,
		uint64(child.RenderGeneration),
		uint32(child.TimeoutSeconds),
		preparation,
	)
	if err != nil {
		return releaseHookPublicationFragment{}, "", err
	}
	if !proto.Equal(expected, evidence.NetworkPlan) || hex.EncodeToString(expected.GetPlanHash()) != child.PlanHash ||
		len(child.Steps) != len(expected.GetSteps()) {
		return releaseHookPublicationFragment{}, "", errs.New(
			errs.KindValidationFailed,
			"release network prerequisite plan differs from captured networks",
		)
	}
	for index, step := range expected.GetSteps() {
		if child.Steps[index].ID != step.GetStepId() {
			return releaseHookPublicationFragment{}, "", errs.New(
				errs.KindValidationFailed,
				"release network prerequisite step differs from captured networks",
			)
		}
	}
	locator := idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID: child.Owner.EnvironmentID, Method: http.MethodPost, Route: "/internal/release-network-child", Key: child.ID}
	child.IdempotencyKey, child.idempotencyMarker = child.ID, &locator
	if err := ValidateTaskRecord(child); err != nil {
		return releaseHookPublicationFragment{}, "", err
	}
	response, err := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{child.ID})
	if err != nil {
		return releaseHookPublicationFragment{}, "", errs.Wrap(errs.KindInternal, err)
	}
	marker := idempotency.IdempotencyMarker{Kind: idempotency.IdempotencyMarkerTask,
		State: idempotency.IdempotencyMarkerPending, Locator: locator, Intent: evidence.Marker.Intent,
		TaskID: child.ID, CreatedAt: child.CreatedAt, UpdatedAt: child.CreatedAt,
		Response: idempotency.IdempotencyResponse{
			Status:      http.StatusAccepted,
			ContentKind: "application/json",
			Body:        response,
		}}
	markerValue, err := idempotency.EncodeIdempotencyMarker(marker)
	if err != nil {
		return releaseHookPublicationFragment{}, "", err
	}
	defer clear(markerValue)
	markerKey, err := idempotency.IdempotencyMarkerKey(locator)
	if err != nil {
		return releaseHookPublicationFragment{}, "", err
	}
	value, err := EncodeTaskRecord(child)
	if err != nil {
		return releaseHookPublicationFragment{}, "", err
	}
	reference, err := idempotency.EncodeTaskReference(child.ID)
	if err != nil {
		clear(value)
		return releaseHookPublicationFragment{}, "", err
	}
	ownerKeys, err := taskOwnerIndexKeys(child.Owner, child.ID)
	if err != nil {
		clear(value)
		clear(reference)
		return releaseHookPublicationFragment{}, "", err
	}
	mutations := []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskStorageKey(child.ID), Value: value},
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskQueueKey(child.Executor, child.ID), Value: reference},
		{
			Type:  keyvalue.MutationPut,
			Key:   taskjournal.TaskActiveOperationKey(child.OperationID),
			Value: append([]byte(nil), reference...),
		},
		{
			Type:  keyvalue.MutationPut,
			Key:   taskjournal.TaskOperationIndexKey(child.OperationID, child.ID),
			Value: append([]byte(nil), reference...),
		},
		{Type: keyvalue.MutationPut, Key: markerKey, Value: append([]byte(nil), markerValue...)},
	}
	for _, key := range ownerKeys {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: []byte(child.ID)})
	}
	return releaseHookPublicationFragment{conditions: []keyvalue.Condition{
		{Key: taskjournal.TaskStorageKey(child.ID)}, {Key: markerKey},
		{Key: taskjournal.TaskActiveOperationKey(child.OperationID)},
	}, mutations: mutations}, child.ID, nil
}
