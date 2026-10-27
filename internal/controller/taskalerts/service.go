// Package taskalerts owns scoped Discord alert configuration and delivery policy.
package taskalerts

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/discordwebhook"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	records "github.com/AlanD20/groundplane/internal/infra/etcd/taskalerts"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/url"
	"strings"
	"time"
)

type Cipher interface {
	secretvalue.Sealer
	secretvalue.Opener
}
type Service struct {
	repository  *records.Repository
	replay      *etcd.IdempotencyRepository
	coordinator *requestidempotency.Coordinator
	cipher      Cipher
}

func NewService(repository *records.Repository, replay *etcd.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator, cipher Cipher) (*Service, error) {
	if repository == nil || replay == nil || coordinator == nil || cipher == nil {
		return nil, errs.New(errs.KindInternal, "Task alert dependencies are required")
	}
	return &Service{repository: repository, replay: replay, coordinator: coordinator, cipher: cipher}, nil
}
func (service *Service) Show(ctx context.Context, scope string) (apiTypes.TaskAlertSettings, error) {
	snapshot, err := service.repository.Read(ctx, scope)
	if err != nil {
		return apiTypes.TaskAlertSettings{}, err
	}
	if snapshot.Config != nil {
		defer clear(snapshot.Config.Webhook)
	}
	if _, err := service.repository.OwnerConditions(ctx, scope, snapshot.ReadRevision); err != nil {
		return apiTypes.TaskAlertSettings{}, err
	}
	return document(snapshot), nil
}
func document(snapshot records.Snapshot) apiTypes.TaskAlertSettings {
	result := apiTypes.TaskAlertSettings{Revision: "none", DeliveryState: "never"}
	if snapshot.Config != nil {
		result.Revision = snapshot.Config.Generation
		result.Enabled = snapshot.Config.Enabled
		result.WebhookConfigured = len(snapshot.Config.Webhook) > 0
		result.ConsoleURL = snapshot.Config.ConsoleURL
	}
	if snapshot.Delivery != nil {
		result.DeliveryState = snapshot.Delivery.State
		result.LastTaskID = snapshot.Delivery.TaskID
		result.LastNotificationID = snapshot.Delivery.NotificationID
		result.LastEventStatus = snapshot.Delivery.EventStatus
		result.LastError = snapshot.Delivery.Error
		result.LastAttemptAt = snapshot.Delivery.At.Format(time.RFC3339)
	}
	return result
}
func validateConsoleURL(raw string) error {
	if raw == "" {
		return nil
	}
	value, err := url.Parse(raw)
	if err != nil || len(raw) > 512 || (value.Scheme != "http" && value.Scheme != "https") || value.Host == "" ||
		value.User != nil ||
		value.RawQuery != "" ||
		value.Fragment != "" ||
		(value.Path != "" && value.Path != "/") {
		return errs.New(
			errs.KindValidationFailed,
			"Console URL must be its HTTP or HTTPS origin without credentials, path or query",
		)
	}
	return nil
}

func (service *Service) prepareConfig(
	ctx context.Context,
	scope string,
	input apiTypes.TaskAlertSettingsWrite,
) (records.Change, apiTypes.TaskAlertSettings, error) {
	if err := validateConsoleURL(input.ConsoleURL); err != nil {
		return records.Change{}, apiTypes.TaskAlertSettings{}, err
	}
	current, err := service.repository.Read(ctx, scope)
	if err != nil {
		return records.Change{}, apiTypes.TaskAlertSettings{}, err
	}
	if current.Config != nil {
		defer clear(current.Config.Webhook)
	}
	if document(current).Revision != input.ExpectedRevision {
		return records.Change{}, apiTypes.TaskAlertSettings{}, errs.New(
			errs.KindStateConflict,
			"Alert settings changed; refresh before saving",
		)
	}
	next := records.Config{
		Generation: ids.New(ids.KindConfig),
		Enabled:    input.Enabled,
		ConsoleURL: strings.TrimSuffix(input.ConsoleURL, "/"),
		SavedAt:    time.Now().UTC(),
	}
	if current.Config != nil {
		next.Webhook = append([]byte(nil), current.Config.Webhook...)
	}
	defer func() { clear(next.Webhook) }()
	if input.WebhookURL != nil {
		clear(next.Webhook)
		next.Webhook = nil
		if *input.WebhookURL != "" {
			if err := discordwebhook.ValidateURL(*input.WebhookURL); err != nil {
				return records.Change{}, apiTypes.TaskAlertSettings{}, err
			}
			next.Webhook, err = service.cipher.Seal(ctx, []byte(*input.WebhookURL))
			if err != nil {
				return records.Change{}, apiTypes.TaskAlertSettings{}, err
			}
		}
	}
	if next.Enabled && len(next.Webhook) == 0 {
		return records.Change{}, apiTypes.TaskAlertSettings{}, errs.New(
			errs.KindValidationFailed,
			"A Discord webhook URL is required to enable alerts",
		)
	}
	change, err := service.repository.PrepareSave(ctx, scope, current, next)
	return change, document(records.Snapshot{Config: &next}), err
}
