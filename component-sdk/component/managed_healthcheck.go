package component

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// ManagedHealthcheck declares an exec-form readiness check inside the managed
// Service image. Command is argv, never a shell expression. Timings are whole
// seconds; no host execution, Secret plaintext or additional capability is exposed.
type ManagedHealthcheck struct {
	Command            []string
	IntervalSeconds    uint32
	TimeoutSeconds     uint32
	StartPeriodSeconds uint32
	Retries            uint32
}

func (health ManagedHealthcheck) Validate() error {
	if len(health.Command) == 0 || len(health.Command) > 32 ||
		health.IntervalSeconds == 0 || health.IntervalSeconds > 3600 ||
		health.TimeoutSeconds == 0 || health.TimeoutSeconds > 300 ||
		health.StartPeriodSeconds > 3600 || health.Retries == 0 || health.Retries > 100 {
		return fmt.Errorf("component: managed healthcheck bounds are invalid")
	}
	for _, argument := range health.Command {
		if argument == "" || len(argument) > 4096 || !utf8.ValidString(argument) ||
			strings.ContainsAny(argument, "\x00\r\n") {
			return fmt.Errorf("component: managed healthcheck argument is invalid")
		}
	}
	return nil
}
