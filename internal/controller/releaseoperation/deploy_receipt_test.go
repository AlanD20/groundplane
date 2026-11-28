package releaseoperation

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/acceptedactions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func TestDeployReceiptRecoversAcceptedTaskAndFencesLateAcceptance(t *testing.T) {
	ctx := t.Context()
	store := &directPublicationStore{revision: 1, values: make(map[string]*keyvalue.KeyValue)}
	protector, err := secretvalue.NewProtector(releaseOperationTestCipher{}, releaseOperationTestCipher{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := etcd.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{coordinator: coordinator, idempotency: replay}
	target := ids.New(ids.KindService)
	scope := workexecution.Scope{
		TenantID: ids.New(
			ids.KindTenant,
		), ProjectID: ids.New(ids.KindProject), EnvironmentID: ids.New(ids.KindEnvironment),
	}
	request := release.ServiceDeployInput{
		Image:     "api:old",
		Strategy:  release.StrategyBlueGreen,
		OnFailure: release.OnFailureSwitchBack,
	}
	const key = "test-git-receipt"
	const fenceKey = "/test/git-activation"
	if _, err := store.Put(ctx, fenceKey, []byte(key)); err != nil {
		t.Fatal(err)
	}
	response, absent, err := service.ReadDeployServiceReceipt(ctx, scope.EnvironmentID, target, request, key)
	if err != nil || response != nil || absent.Key == "" {
		t.Fatalf("unaccepted request did not supply an absence comparison: %v", err)
	}
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	payloadRepository, err := acceptedactions.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	payloads, err := acceptedaction.NewPayloads(payloadRepository, protector)
	if err != nil {
		t.Fatal(err)
	}
	submitter, err := acceptedaction.NewSubmitter(payloads, queue, replay, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	locator, protected, durable, _, err := service.begin(ctx, scope.EnvironmentID, http.MethodPost,
		serviceDeployRoute, target, key, serviceDeployBody(request))
	if err != nil {
		t.Fatal(err)
	}
	defer protected.Destroy()
	defer clear(durable.Ciphertext)
	at := time.Now().UTC()
	taskID := ids.New(ids.KindTask)
	accepted, err := acceptedaction.Submit(ctx, submitter, acceptedaction.Submission{
		Task: workexecution.Task{
			ID: taskID, Actor: workexecution.OperatorActor, Action: workexecution.ActionDeploy,
			TargetID: target, TargetName: "api", Scope: scope, CreatedAt: at, UpdatedAt: at, Deadline: at.Add(time.Hour),
		},
		Kind: "service-deploy", Locator: locator, Intent: durable, Evidence: protected,
	}, request)
	if err != nil {
		t.Fatal(err)
	}
	task, revision, err := queue.ReadTask(ctx, taskID, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = acceptedaction.Open(
		ctx,
		payloads,
		task,
		"service-deploy",
		revision,
		func(input acceptedServiceDeploy) error {
			if input.Source != nil || input.Image != request.Image || input.Strategy != request.Strategy ||
				input.OnFailure != request.OnFailure {
				t.Fatal(
					"pending deploy encoded by the old Controller did not reopen with its original rollout settings",
				)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Transact(
		ctx,
		[]keyvalue.Condition{absent},
		[]keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: fenceKey}},
	)
	if err != nil || result.Succeeded {
		t.Fatalf("stale receipt absence released the fence after native acceptance: %v", err)
	}
	response, _, err = service.ReadDeployServiceReceipt(ctx, scope.EnvironmentID, target, request, key)
	if err != nil || response == nil || response.Status != accepted.Status ||
		!bytes.Equal(response.Body, accepted.Body) {
		t.Fatalf("accepted receipt could not be recovered without a current Service: %v", err)
	}
	request.OnFailure = release.OnFailureLeaveActive
	if response, _, err := service.ReadDeployServiceReceipt(ctx, scope.EnvironmentID, target, request, key); err == nil ||
		response != nil {
		t.Fatal("a different rollout policy recovered another request's receipt")
	}
}
