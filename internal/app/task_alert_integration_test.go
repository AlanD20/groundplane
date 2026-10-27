package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/controller/taskalerts"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/discordwebhook"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	records "github.com/AlanD20/groundplane/internal/infra/etcd/taskalerts"
	api "github.com/AlanD20/groundplane/pkg/api"
	"io"
	"log/slog"
	"testing"
	"time"
)

// ALERT-01: replaying a lost save must preserve the exact configuration revision
// without returning or storing the plaintext destination in any readable record.
// ALERT-02: restart/retry, scope routing and destination changes use durable state.
func TestTaskAlertsDurableRoutingReplayAndBoundedDelivery(t *testing.T) {
	f := newNativeVolumeFixture(t)
	ctx := t.Context()
	key, err := ageinfra.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	cipher := manualJourneyAgeCipher{key: key}
	protector, err := secretvalue.NewProtector(cipher, cipher)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := etcd.NewIdempotencyRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := records.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	service, err := taskalerts.NewService(repository, replay, coordinator, cipher)
	if err != nil {
		t.Fatal(err)
	}
	webhook := "https://discord.com/api/webhooks/123456789/qa_token_abcdefghijklmnop"
	request := api.TaskAlertSettingsWrite{ExpectedRevision: "none", Enabled: true, WebhookURL: &webhook}
	requestKey := ids.NewULID()
	first, err := service.Save(ctx, "platform", requestKey, request)
	if err != nil {
		t.Fatal(err)
	}
	again, err := service.Save(ctx, "platform", requestKey, request)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Body, again.Body) || bytes.Contains(first.Body, []byte(webhook)) {
		t.Fatal("lost save did not replay the redacted original response")
	}
	request.Enabled = false
	if _, err := service.Save(ctx, "platform", requestKey, request); err == nil {
		t.Fatal("same key accepted a different alert setting")
	}
	for _, versions := range f.store.history {
		for _, version := range versions {
			if bytes.Contains(version.value, []byte(webhook)) {
				t.Fatal("webhook plaintext escaped encryption")
			}
		}
	}
	var saved api.TaskAlertSettings
	if err := json.Unmarshal(first.Body, &saved); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Save(ctx, f.environmentID, ids.NewULID(), api.TaskAlertSettingsWrite{ExpectedRevision: "none", Enabled: true, WebhookURL: &webhook}); err != nil {
		t.Fatal(err)
	}
	sender := &alertSender{failure: true}
	worker := func() *taskalerts.Worker {
		result, err := taskalerts.NewWorker(repository, cipher, sender, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	now := time.Now().UTC().Add(time.Second)
	enqueue := func(scope string) string {
		before := workexecution.Task{
			ID:         ids.New(ids.KindTask),
			Action:     workexecution.ActionDeploy,
			TargetName: "api",
			UpdatedAt:  now,
		}
		before.Scope.EnvironmentID = scope
		after := before
		after.Progress.Attention = 1
		mutations, err := records.PrepareTransition(before, after)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.Transact(ctx, nil, mutations); err != nil {
			t.Fatal(err)
		}
		keyvalue.ClearMutationValues(mutations)
		duplicate, err := records.PrepareTransition(after, after)
		if err != nil || len(duplicate) != 0 {
			t.Fatal("unchanged attention queued another notification")
		}
		return after.ID
	}
	platformTask, envTask := enqueue(""), enqueue(f.environmentID)
	if err := worker().Process(ctx, now); err != nil {
		t.Fatal(err)
	}
	if len(sender.messages) != 2 {
		t.Fatalf("first attempts = %d", len(sender.messages))
	}
	// A fresh worker has no in-memory delivery history; the persisted retry time
	// must still prevent early replay, then permit one later successful attempt.
	if err := worker().Process(ctx, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if len(sender.messages) != 2 {
		t.Fatal("restart ignored persisted retry schedule")
	}
	sender.failure = false
	if err := worker().Process(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	platform, _ := service.Show(ctx, "platform")
	environment, _ := service.Show(ctx, f.environmentID)
	if platform.LastTaskID != platformTask || environment.LastTaskID != envTask || platform.DeliveryState != "sent" ||
		environment.DeliveryState != "sent" {
		t.Fatal("notifications crossed scopes or failed to settle")
	}
	enqueue("")
	if _, err := service.Save(ctx, "platform", ids.NewULID(), api.TaskAlertSettingsWrite{ExpectedRevision: saved.Revision, Enabled: false}); err != nil {
		t.Fatal(err)
	}
	calls := len(sender.messages)
	if err := worker().Process(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if len(sender.messages) != calls {
		t.Fatal("disabled destination delivered an older notification")
	}
	// Test a persistently unavailable Environment destination: retries terminate.
	enqueue(f.environmentID)
	sender.failure = true
	current := now.Add(2 * time.Minute)
	for range 6 {
		if err := worker().Process(ctx, current); err != nil {
			t.Fatal(err)
		}
		current = current.Add(2 * time.Hour)
	}
	pending, _, err := repository.Pending(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 || len(sender.messages)-calls != 5 {
		t.Fatal("retry limit did not drain failed delivery")
	}
	final, err := service.Show(ctx, f.environmentID)
	if err != nil || final.DeliveryState != "failed" {
		t.Fatal("permanent failure is not visible")
	}
}

type alertSender struct {
	failure  bool
	messages []string
}

func (sender *alertSender) Send(_ context.Context, _ string, content string) (discordwebhook.Result, error) {
	sender.messages = append(sender.messages, content)
	if sender.failure {
		return discordwebhook.Result{}, errors.New("temporary transport failure")
	}
	return discordwebhook.Result{}, nil
}
