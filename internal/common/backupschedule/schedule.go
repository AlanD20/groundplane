// Package backupschedule evaluates five-field cron backup schedules in UTC.
package backupschedule

import (
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/robfig/cron/v3"
)

// Schedule is immutable; evaluation never advances a shared scheduler cursor.
type Schedule struct {
	spec *cron.SpecSchedule
	text string
}

// Parse accepts minute, hour, day-of-month, month and weekday fields. Seconds,
// descriptors, timezone prefixes and nonstandard Quartz extensions are excluded.
func Parse(value string) (Schedule, error) {
	if len(value) > 256 || len(strings.Fields(value)) != 5 || strings.ContainsAny(value, "?#@=:") {
		return Schedule{}, invalid(value)
	}
	for _, field := range strings.Fields(value) {
		if strings.Contains(field, ",,") || strings.HasPrefix(field, ",") || strings.HasSuffix(field, ",") {
			return Schedule{}, invalid(value)
		}
		for _, token := range strings.FieldsFunc(field, func(r rune) bool { return r == ',' || r == '-' || r == '/' }) {
			if token == "*" || isNumber(token) {
				continue
			}
			switch strings.ToUpper(token) {
			case "JAN",
				"FEB",
				"MAR",
				"APR",
				"MAY",
				"JUN",
				"JUL",
				"AUG",
				"SEP",
				"OCT",
				"NOV",
				"DEC",
				"SUN",
				"MON",
				"TUE",
				"WED",
				"THU",
				"FRI",
				"SAT":
			default:
				return Schedule{}, invalid(value)
			}
		}
	}
	parsed, err := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow).Parse(value)
	if err != nil {
		return Schedule{}, invalid(value)
	}
	spec, ok := parsed.(*cron.SpecSchedule)
	if !ok {
		return Schedule{}, invalid(value)
	}
	schedule := Schedule{spec: spec, text: value}
	// A leap year contains every possible month/day. Reject impossible calendars
	// (such as February 30) before accepting a policy that would never run.
	for day := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC); day.Year() == 2000; day = day.AddDate(0, 0, 1) {
		if schedule.matchesDay(day) {
			return schedule, nil
		}
	}
	return Schedule{}, invalid(value)
}

func isNumber(value string) bool {
	if value == "" {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func (schedule Schedule) String() string { return schedule.text }

// NextOccurrence is strictly after the input, including at an exact minute boundary.
func (schedule Schedule) NextOccurrence(after time.Time) (time.Time, error) {
	if schedule.spec == nil {
		return time.Time{}, invalid(schedule.text)
	}
	next, ok := schedule.findOccurrence(after.UTC(), 1)
	if !ok {
		return time.Time{}, errs.Newf(
			errs.KindInternal,
			"backup schedule %q has no representable next occurrence",
			schedule.text,
		)
	}
	return next, nil
}

// LatestOccurrence returns only the latest missed instant in (lastEvaluatedAt, now].
// Searching backward by calendar date avoids replaying every missed minute after
// years of downtime and keeps the existing single catch-up/no-backlog contract.
func (schedule Schedule) LatestOccurrence(lastEvaluatedAt, now time.Time) (time.Time, bool, error) {
	if schedule.spec == nil {
		return time.Time{}, false, invalid(schedule.text)
	}
	if !now.After(lastEvaluatedAt) {
		return time.Time{}, false, nil
	}
	latest, ok := schedule.findOccurrence(now.UTC(), -1)
	if !ok || !latest.After(lastEvaluatedAt) {
		return time.Time{}, false, nil
	}
	return latest, true, nil
}

func (schedule Schedule) matchesDay(day time.Time) bool {
	spec := schedule.spec
	if spec.Month&(uint64(1)<<uint(day.Month())) == 0 {
		return false
	}
	dom := spec.Dom&(uint64(1)<<uint(day.Day())) != 0
	dow := spec.Dow&(uint64(1)<<uint(day.Weekday())) != 0
	// Standard cron: restricted day-of-month and weekday fields are alternatives.
	const wildcard = uint64(1) << 63
	if spec.Dom&wildcard != 0 || spec.Dow&wildcard != 0 {
		return dom && dow
	}
	return dom || dow
}

func (schedule Schedule) findOccurrence(bound time.Time, direction int) (time.Time, bool) {
	day := time.Date(bound.Year(), bound.Month(), bound.Day(), 0, 0, 0, 0, time.UTC)
	// February 29 can be eight years apart across a non-leap century. Without a
	// year field no admitted schedule needs a longer search interval.
	for i := 0; i <= 8*366; i++ {
		if schedule.matchesDay(day) {
			for h := 0; h < 24; h++ {
				hour := h
				if direction < 0 {
					hour = 23 - h
				}
				if schedule.spec.Hour&(uint64(1)<<uint(hour)) == 0 {
					continue
				}
				for m := 0; m < 60; m++ {
					minute := m
					if direction < 0 {
						minute = 59 - m
					}
					if schedule.spec.Minute&(uint64(1)<<uint(minute)) == 0 {
						continue
					}
					candidate := time.Date(day.Year(), day.Month(), day.Day(), hour, minute, 0, 0, time.UTC)
					if candidate.Year() != day.Year() || candidate.Month() != day.Month() ||
						candidate.Day() != day.Day() ||
						candidate.Hour() != hour ||
						candidate.Minute() != minute {
						continue
					}
					if direction > 0 && candidate.After(bound) || direction < 0 && !candidate.After(bound) {
						return candidate, true
					}
				}
			}
		}
		next := day.AddDate(0, 0, direction)
		if direction > 0 && !next.After(day) || direction < 0 && !next.Before(day) {
			break
		}
		day = next
	}
	return time.Time{}, false
}

func invalid(value string) error {
	return errs.Newf(
		errs.KindValidationFailed,
		"backup schedule must be a valid five-field UTC cron expression: %q",
		value,
	)
}
