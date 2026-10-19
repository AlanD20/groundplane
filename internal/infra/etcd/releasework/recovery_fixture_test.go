package releasework

import (
	"context"
	"crypto/sha256"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"gopkg.in/yaml.v3"
)

type recoveryTestStore struct {
	revision int64
	values   map[string]*keyvalue.KeyValue
}

func (store *recoveryTestStore) GetMany(
	_ context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	result := &keyvalue.GetManyResult{
		ReadRevision: request.Revision,
		Values:       make([]*keyvalue.KeyValue, len(request.Keys)),
	}
	for index, key := range request.Keys {
		if value := store.values[key]; value != nil {
			copy := *value
			copy.Value = slices.Clone(value.Value)
			result.Values[index] = &copy
		}
	}
	return result, nil
}

func (store *recoveryTestStore) put(key string, value []byte) {
	store.revision++
	store.values[key] = &keyvalue.KeyValue{Key: key, Value: slices.Clone(value), ModRevision: store.revision}
}

func (store *recoveryTestStore) publish(change workqueue.Change) {
	store.revision++
	for _, mutation := range change.Mutations {
		store.values[mutation.Key] = &keyvalue.KeyValue{
			Key:         mutation.Key,
			Value:       slices.Clone(mutation.Value),
			ModRevision: store.revision,
		}
	}
}

func nativeRecoveryFixture(
	t *testing.T,
	count int,
) (releaseexecution.RecoveryInput, workqueue.ResumeView, *recoveryTestStore) {
	t.Helper()
	at := time.Now().UTC().Add(-time.Minute)
	forward := workexecution.Attempt{
		ID:         ids.New(ids.KindAttempt),
		TaskID:     ids.New(ids.KindTask),
		WorkItemID: ids.New(ids.KindWorkItem),
		Number:     1,
		Direction:  workexecution.Forward,
		AssignedAt: at,
		StartedAt:  &at,
		Deadline:   at.Add(time.Hour),
		Input:      workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)},
		Executor: workexecution.Identity{
			Executor:   workexecution.Agent,
			HostID:     ids.New(ids.KindHost),
			AgentID:    ids.New(ids.KindAgent),
			Generation: 1,
		},
	}
	environmentID, serviceID, releaseID := ids.New(
		ids.KindEnvironment,
	), ids.New(
		ids.KindService,
	), ids.New(
		ids.KindDeployment,
	)
	labels := map[string]string{
		"com.groundplane.environment-id":    environmentID,
		"com.groundplane.service-id":        serviceID,
		"com.groundplane.plan-id":           forward.Input.ID,
		"com.groundplane.render-generation": "1",
		"com.groundplane.runtime-role":      "singleton",
		"com.groundplane.release-id":        releaseID,
		"com.groundplane.managed":           "true",
		"com.groundplane.kind":              "service",
	}
	image := "sha256:" + strings.Repeat("b", 64)
	raw, err := yaml.Marshal(
		map[string]any{
			"services": map[string]any{"app": map[string]any{"image": image, "network_mode": "none", "labels": labels}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	artifact := &agentpb.ComposeArtifact{ArtifactId: ids.New(ids.KindConfig), OwnerId: environmentID,
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, ProjectName: "gp-" + strings.ToLower(environmentID),
		AuthorizedVolumeDir: "/var/lib/groundplane/volumes/" + environmentID, CanonicalYaml: raw, YamlSha256: digest[:]}
	service := &agentpb.ComposeService{
		ComposeName:      "app",
		ServiceId:        serviceID,
		ImageReference:   image,
		ExpectedReplicas: 1,
		Role:             agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		service.ExpectedLabels = append(service.ExpectedLabels, &agentpb.LabelPair{Key: key, Value: labels[key]})
	}
	artifact.Services = []*agentpb.ComposeService{service}
	release := &agentpb.ReleaseWorkInput{OperationId: ids.New(ids.KindOperation), EnvironmentId: environmentID,
		ServiceId: serviceID, ReleaseId: releaseID, RenderGeneration: 1, Candidate: artifact,
		Strategy:      agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE,
		FailurePolicy: agentpb.WorkReleaseFailurePolicy_WORK_RELEASE_FAILURE_POLICY_SWITCH_BACK,
		Predecessor:   &agentpb.ReleaseWorkPredecessor{Absent: true},
		Configuration: &agentpb.WorkConfigurationReference{
			ConfigurationId: ids.New(ids.KindConfig),
			EnvironmentId:   environmentID,
			Generation:      1,
			Sha256:          digest[:],
		}}
	for index := range count {
		release.Hooks = append(
			release.Hooks,
			&agentpb.ReleaseWorkHook{ExecutionId: ids.NewULID(), ScriptId: ids.New(ids.KindScript),
				Phase: "on-failure", Order: uint32(count - index), Input: &agentpb.WorkInputReference{InputId: ids.New(ids.KindPlan), Sha256: digest[:]}},
		)
	}
	failedAt := at.Add(time.Second)
	failure := releaseexecution.Failure{
		AttemptID:  forward.ID,
		Input:      forward.Input,
		Reason:     releaseexecution.ExecutionFailed,
		RecordedAt: failedAt,
	}
	_, failureRef, err := releaseexecution.EncodeFailure(forward, failure)
	if err != nil {
		t.Fatal(err)
	}
	forward.FinishedAt, forward.Result, forward.Outcome = &failedAt, &failureRef, workexecution.Failed
	attempt := forward
	started := failedAt.Add(time.Second)
	attempt.ID, attempt.Number, attempt.Direction, attempt.AssignedAt, attempt.StartedAt = ids.New(
		ids.KindAttempt,
	), 2, workexecution.Recovery, started, &started
	attempt.FinishedAt, attempt.Result, attempt.Outcome = nil, nil, ""
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(attempt, release)
	if err != nil {
		t.Fatal(err)
	}
	input := releaseexecution.RecoveryInput{
		Input:       releaseexecution.Input{Attempt: attempt, Release: release, Artifacts: artifacts},
		Forward:     forward,
		Failure:     failure,
		Disposition: agentpb.WorkReleaseRecoveryDisposition_WORK_RELEASE_RECOVERY_DISPOSITION_RESTORE_CAPTURED,
	}
	if err := input.Validate(); err != nil {
		t.Fatal(err)
	}
	item := workexecution.Item{
		ID:              attempt.WorkItemID,
		TaskID:          attempt.TaskID,
		Operation:       workexecution.Release,
		TargetID:        serviceID,
		Placement:       workexecution.Placement{Executor: workexecution.Agent, HostID: attempt.Executor.HostID},
		Claims:          []workexecution.Claim{{ScopeID: serviceID, HostID: attempt.Executor.HostID}},
		Input:           &attempt.Input,
		Phase:           workexecution.Running,
		AttemptNumber:   2,
		ActiveAttemptID: attempt.ID,
		Outcome:         workexecution.Failed,
		Result:          &failureRef,
		Cleanup:         workexecution.CleanupRunning,
		CreatedAt:       at,
		UpdatedAt:       started,
		Deadline:        attempt.Deadline,
	}
	view := workqueue.ResumeView{ActiveAttempt: true, CurrentExecutor: true, ExecutorRevision: 1,
		Attempt: workqueue.AttemptSnapshot{Attempt: attempt, Revision: 1},
		Work:    workqueue.View{ReadRevision: 1, Item: workqueue.ItemSnapshot{Item: item, Revision: 1}}}
	return input, view, &recoveryTestStore{revision: 1, values: make(map[string]*keyvalue.KeyValue)}
}
