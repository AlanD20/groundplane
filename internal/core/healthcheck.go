package core

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

func (h Healthcheck) Empty() bool {
	return len(h.Test) == 0 && !h.Disable && h.Interval == "" && h.Timeout == "" &&
		h.StartPeriod == "" && h.StartInterval == "" && h.Retries == 0
}

func (h Healthcheck) Enabled() bool {
	return !h.Disable && len(h.Test) > 1 && h.Test[0] != "NONE"
}

func (h Healthcheck) Equal(other Healthcheck) bool {
	return slices.Equal(h.Test, other.Test) && h.Disable == other.Disable &&
		h.Interval == other.Interval && h.Timeout == other.Timeout &&
		h.StartPeriod == other.StartPeriod && h.StartInterval == other.StartInterval && h.Retries == other.Retries
}

func (h Healthcheck) Validate() error {
	if len(h.Test) > 0 {
		switch h.Test[0] {
		case "NONE":
			if len(h.Test) != 1 {
				return fmt.Errorf("healthcheck NONE does not accept arguments")
			}
		case "CMD":
			if len(h.Test) < 2 || strings.TrimSpace(h.Test[1]) == "" {
				return fmt.Errorf("healthcheck CMD requires an executable")
			}
		case "CMD-SHELL":
			if len(h.Test) != 2 || strings.TrimSpace(h.Test[1]) == "" {
				return fmt.Errorf("healthcheck CMD-SHELL requires exactly one shell command")
			}
		default:
			return fmt.Errorf("healthcheck test must start with CMD, CMD-SHELL or NONE")
		}
	}
	if len(h.Test) > 128 {
		return fmt.Errorf("healthcheck accepts at most 127 command arguments")
	}
	size := 0
	for _, arg := range h.Test {
		if strings.ContainsRune(arg, 0) {
			return fmt.Errorf("healthcheck command contains a NUL byte")
		}
		size += len(arg)
	}
	if size > 16384 {
		return fmt.Errorf("healthcheck command exceeds 16 KiB")
	}
	for _, field := range []struct {
		name, value string
	}{
		{"interval", h.Interval}, {"timeout", h.Timeout},
		{"start_period", h.StartPeriod}, {"start_interval", h.StartInterval},
	} {
		if field.value == "" {
			continue
		}
		duration, err := time.ParseDuration(field.value)
		if err != nil || duration < 0 || duration > 24*time.Hour {
			return fmt.Errorf("healthcheck %s must be a valid duration up to 24h", field.name)
		}
	}
	if h.Retries < 0 || h.Retries > 100 {
		return fmt.Errorf("healthcheck retries must be between 0 (Docker default) and 100")
	}
	return nil
}
