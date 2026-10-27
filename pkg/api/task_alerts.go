package api

type TaskAlertSettings struct {
	Revision           string `json:"revision"`
	Enabled            bool   `json:"enabled"`
	WebhookConfigured  bool   `json:"webhook_configured"`
	ConsoleURL         string `json:"console_url"`
	DeliveryState      string `json:"delivery_state"`
	LastTaskID         string `json:"last_task_id,omitempty"`
	LastNotificationID string `json:"last_notification_id,omitempty"`
	LastEventStatus    string `json:"last_event_status,omitempty"`
	LastAttemptAt      string `json:"last_attempt_at,omitempty"`
	LastError          string `json:"last_error,omitempty"`
}
type TaskAlertSettingsWrite struct {
	ExpectedRevision string  `json:"expected_revision" minLength:"1" maxLength:"64"`
	Enabled          bool    `json:"enabled"`
	WebhookURL       *string `json:"webhook_url,omitempty" maxLength:"2048" writeOnly:"true"`
	ConsoleURL       string  `json:"console_url" maxLength:"512"`
}
type TaskAlertTestRequest struct {
	ExpectedRevision string `json:"expected_revision" minLength:"1" maxLength:"64"`
}
type TaskAlertTestAccepted struct {
	Queued         bool   `json:"queued"`
	NotificationID string `json:"notification_id"`
}
