// Package taskalerts owns Discord alert settings and the durable delivery queue.
package taskalerts

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
	"time"
)

const OutboxPrefix = "/v1/task-alerts/outbox/"

func ConfigKey(scope string) string    { return "/v1/task-alerts/config/" + scope }
func DeliveryKey(scope string) string  { return "/v1/task-alerts/delivery/" + scope }
func EventKey(scope, id string) string { return OutboxPrefix + scope + "/" + id }
func ValidScope(scope string) bool {
	return scope == "platform" || ids.Validate(ids.KindEnvironment, scope) == nil
}

type Config struct {
	Generation string    `json:"generation"`
	Enabled    bool      `json:"enabled"`
	Webhook    []byte    `json:"webhook"`
	ConsoleURL string    `json:"console_url"`
	SavedAt    time.Time `json:"saved_at"`
}
type Delivery struct {
	NotificationID string    `json:"notification_id"`
	Generation     string    `json:"generation"`
	State          string    `json:"state"`
	TaskID         string    `json:"task_id,omitempty"`
	EventStatus    string    `json:"event_status,omitempty"`
	At             time.Time `json:"at"`
	Error          string    `json:"error,omitempty"`
}
type Event struct {
	ID          string    `json:"id"`
	Scope       string    `json:"scope"`
	TaskID      string    `json:"task_id,omitempty"`
	Target      string    `json:"target"`
	Action      string    `json:"action"`
	Status      string    `json:"status"`
	At          time.Time `json:"at"`
	Generation  string    `json:"generation,omitempty"`
	Attempts    int       `json:"attempts"`
	NextAttempt time.Time `json:"next_attempt"`
}

func alertState(task workexecution.Task) string {
	if task.FinishedAt != nil {
		if task.Outcome == workexecution.Failed {
			return "failed"
		}
		return ""
	}
	if task.Progress.Attention > 0 {
		return "attention_required"
	}
	return ""
}

// The Task's own CAS makes the alert inseparable from its state transition.
// No work input, output, Secret, or journal payload enters the notification.
func PrepareTransition(before, after workexecution.Task) ([]keyvalue.Mutation, error) {
	status := alertState(after)
	if status == "" || status == alertState(before) {
		return nil, nil
	}
	scope := after.Scope.EnvironmentID
	if scope == "" {
		scope = "platform"
	}
	event := Event{ID: ids.New(ids.KindOperation), Scope: scope, TaskID: after.ID,
		Target: after.TargetName, Action: string(after.Action), Status: status, At: after.UpdatedAt}
	return PutEvent(event)
}
func PutEvent(event Event) ([]keyvalue.Mutation, error) {
	if !ValidScope(event.Scope) || ids.Validate(ids.KindOperation, event.ID) != nil ||
		(event.Status != "failed" && event.Status != "attention_required" && event.Status != "test") ||
		(event.Status != "test" && ids.Validate(ids.KindTask, event.TaskID) != nil) || event.At.IsZero() ||
		event.Attempts < 0 || event.Attempts > 5 || len(event.Target) > 512 || len(event.Action) > 64 {
		return nil, errs.New(errs.KindValidationFailed, "Task alert delivery record is invalid")
	}
	raw, err := recordcodec.Encode("task_alert_event", event)
	if err != nil {
		return nil, err
	}
	return []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: EventKey(event.Scope, event.ID), Value: raw}}, nil
}
func DecodeConfig(raw []byte) (Config, error) {
	value, err := recordcodec.Decode[Config](raw, "task_alert_config")
	if err != nil || ids.Validate(ids.KindConfig, value.Generation) != nil || value.SavedAt.IsZero() ||
		len(value.Webhook) > 16384 || (value.Enabled && len(value.Webhook) == 0) || len(value.ConsoleURL) > 2048 {
		return Config{}, errs.New(errs.KindInternal, "Task alert settings record is invalid")
	}
	return value, nil
}
func DecodeEvent(raw []byte) (Event, error) {
	value, err := recordcodec.Decode[Event](raw, "task_alert_event")
	if err != nil {
		return Event{}, err
	}
	mutations, err := PutEvent(value)
	keyvalue.ClearMutationValues(mutations)
	if err != nil {
		return Event{}, errs.New(errs.KindInternal, "Task alert delivery record is invalid")
	}
	return value, nil
}
func ValidEventCursor(cursor string) bool {
	return cursor == "" || strings.HasPrefix(cursor, OutboxPrefix)
}
