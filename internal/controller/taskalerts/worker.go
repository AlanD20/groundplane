package taskalerts

import (
	"context"
	"fmt"
	"github.com/AlanD20/groundplane/internal/infra/discordwebhook"
	records "github.com/AlanD20/groundplane/internal/infra/etcd/taskalerts"
	"github.com/AlanD20/groundplane/pkg/errs"
	"log/slog"
	"time"
)

type Sender interface {
	Send(context.Context, string, string) (discordwebhook.Result, error)
}
type Worker struct {
	repository *records.Repository
	cipher     Cipher
	sender     Sender
	logger     *slog.Logger
	after      string
}

func NewWorker(repository *records.Repository, cipher Cipher, sender Sender, logger *slog.Logger) (*Worker, error) {
	if repository == nil || cipher == nil || sender == nil || logger == nil {
		return nil, errs.New(errs.KindInternal, "Task alert worker dependencies are required")
	}
	return &Worker{repository: repository, cipher: cipher, sender: sender, logger: logger}, nil
}
func (worker *Worker) Run(ctx context.Context) {
	timer := time.NewTicker(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			pass, cancel := context.WithTimeout(ctx, 15*time.Second)
			err := worker.Process(pass, time.Now().UTC())
			cancel()
			if err != nil && ctx.Err() == nil {
				worker.logger.Error("Task alert delivery pass failed", "error", err)
			}
		}
	}
}
func (worker *Worker) Process(ctx context.Context, now time.Time) error {
	events, more, err := worker.repository.Pending(ctx, worker.after)
	if err != nil {
		return err
	}
	for _, pending := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		if pending.Event.NextAttempt.After(now) {
			worker.after = pending.Key
			continue
		}
		if err := worker.deliver(ctx, pending, now); err != nil {
			return err
		}
		worker.after = pending.Key
	}
	if !more {
		worker.after = ""
	}
	return nil
}
func (worker *Worker) deliver(ctx context.Context, pending records.Pending, now time.Time) error {
	event := pending.Event
	current, err := worker.repository.Read(ctx, event.Scope)
	if err != nil {
		return err
	}
	if current.Config == nil {
		return worker.repository.Drop(ctx, pending)
	}
	config := current.Config
	defer clear(config.Webhook)
	if !config.Enabled || event.At.Before(config.SavedAt) ||
		(event.Generation != "" && event.Generation != config.Generation) {
		return worker.repository.Drop(ctx, pending)
	}
	webhook, err := worker.cipher.Open(ctx, config.Webhook)
	defer clear(webhook)
	result := discordwebhook.Result{Permanent: true}
	if err == nil {
		result, err = worker.sender.Send(ctx, string(webhook), message(event, config.ConsoleURL))
	}
	event.Attempts++
	event.Generation = config.Generation
	delivery := records.Delivery{
		NotificationID: event.ID,
		Generation:     config.Generation,
		State:          "sent",
		TaskID:         event.TaskID,
		EventStatus:    event.Status,
		At:             now,
	}
	if err == nil {
		return worker.repository.Settle(ctx, pending, current, delivery, nil)
	}
	delivery.State = "failed"
	delivery.Error = "Discord notification could not be delivered. Check the saved webhook and channel permissions."
	if result.Permanent || event.Attempts >= 5 {
		return worker.repository.Settle(ctx, pending, current, delivery, nil)
	}
	delay := []time.Duration{10 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute}[event.Attempts-1]
	if result.RetryAfter > delay {
		delay = result.RetryAfter
	}
	event.NextAttempt = now.Add(delay)
	delivery.State = "retrying"
	delivery.Error = "Discord delivery failed; a bounded retry is scheduled."
	return worker.repository.Settle(ctx, pending, current, delivery, &event)
}
func message(event records.Event, consoleURL string) string {
	if event.Status == "test" {
		return "Groundplane test notification\nScope: " + event.Scope + "\nDiscord Task alerts are connected."
	}
	status := "failed"
	if event.Status == "attention_required" {
		status = "needs operator attention"
	}
	text := fmt.Sprintf("Groundplane Task %s\nAction: %s\nTarget: %s\nScope: %s\nTask: %s\nOccurred: %s",
		status, event.Action, event.Target, event.Scope, event.TaskID, event.At.Format(time.RFC3339))
	if consoleURL != "" {
		text += "\n" + consoleURL + "/platform/activity?task=" + event.TaskID
	}
	return text
}
