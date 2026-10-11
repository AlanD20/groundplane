package taskdescription

import (
	"slices"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ConfiguredHealthcheck is a public-safe projection of a selected sealed
// workload. Commands, arguments, process patterns and URLs never enter it.
// Empty timing fields and nil retries mean no override was authored; they do
// not assert the image's or Docker's effective defaults.
type ConfiguredHealthcheck struct {
	ServiceID        string  `json:"service_id"`
	Workload         string  `json:"workload"`
	Kind             string  `json:"kind" enum:"http,tcp,process,custom_exec,custom_shell,disabled,image_default,unavailable"`
	ExpectedReplicas uint32  `json:"expected_replicas"`
	RequiresHealthy  bool    `json:"requires_healthy"`
	LoopbackPort     uint16  `json:"loopback_port,omitempty"`
	HTTPProtocol     string  `json:"http_protocol,omitempty" enum:"http,https"`
	Interval         string  `json:"interval,omitempty"`
	Timeout          string  `json:"timeout,omitempty"`
	StartPeriod      string  `json:"start_period,omitempty"`
	StartInterval    string  `json:"start_interval,omitempty"`
	Retries          *uint32 `json:"retries,omitempty"`
}

func ValidateHealthchecks(values []ConfiguredHealthcheck) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !safeHealthcheckLabel(value.ServiceID, 160) || !safeHealthcheckLabel(value.Workload, 255) ||
			value.ExpectedReplicas == 0 || value.ExpectedReplicas > 65536 {
			return errs.New(errs.KindValidationFailed, "task configured healthcheck identity is invalid")
		}
		if _, duplicate := seen[value.Workload]; duplicate {
			return errs.New(errs.KindValidationFailed, "task configured healthcheck workload is repeated")
		}
		seen[value.Workload] = struct{}{}
		switch value.Kind {
		case "http", "tcp", "process", "custom_exec", "custom_shell", "disabled", "image_default", "unavailable":
		default:
			return errs.New(errs.KindValidationFailed, "task configured healthcheck kind is invalid")
		}
		if value.LoopbackPort != 0 && value.Kind != "http" && value.Kind != "tcp" ||
			value.HTTPProtocol != "" && (value.Kind != "http" || value.LoopbackPort == 0 ||
				value.HTTPProtocol != "http" && value.HTTPProtocol != "https") {
			return errs.New(errs.KindValidationFailed, "task configured healthcheck target is invalid")
		}
		for _, duration := range []string{value.Interval, value.Timeout, value.StartPeriod, value.StartInterval} {
			if duration != "" && canonicalHealthcheckDuration(duration) != duration {
				return errs.New(errs.KindValidationFailed, "task configured healthcheck timing is invalid")
			}
		}
		if value.Retries != nil && *value.Retries > 100 {
			return errs.New(errs.KindValidationFailed, "task configured healthcheck retry count is invalid")
		}
	}
	return nil
}

func CloneHealthchecks(values []ConfiguredHealthcheck) []ConfiguredHealthcheck {
	if values == nil {
		return nil
	}
	result := append([]ConfiguredHealthcheck(nil), values...)
	for index := range result {
		if values[index].Retries != nil {
			retries := *values[index].Retries
			result[index].Retries = &retries
		}
	}
	return result
}

func HealthchecksEqual(left, right []ConfiguredHealthcheck) bool {
	return slices.EqualFunc(left, right, func(a, b ConfiguredHealthcheck) bool {
		if (a.Retries == nil) != (b.Retries == nil) ||
			a.Retries != nil && *a.Retries != *b.Retries {
			return false
		}
		a.Retries, b.Retries = nil, nil
		return a == b
	})
}

func safeHealthcheckLabel(value string, limit int) bool {
	if value == "" || len(value) > limit || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func canonicalHealthcheckDuration(value string) string {
	if len(value) > 64 {
		return ""
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < 0 || duration > 24*time.Hour {
		return ""
	}
	return duration.String()
}
