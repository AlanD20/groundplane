// Package backupschedule parses and evaluates Groundplane's bounded UTC
// backup-policy frequency grammar.
package backupschedule

import (
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Kind identifies the two frequency forms accepted by the backup policy.
type Kind uint8

const (
	// KindInvalid is the zero value and is not a usable schedule.
	KindInvalid Kind = iota
	// KindDaily identifies a schedule that runs every UTC day.
	KindDaily
	// KindWeekly identifies a schedule that runs on one UTC weekday.
	KindWeekly
)

// Schedule is an immutable parsed backup frequency.
//
// A Schedule can only be constructed through Parse. Its methods do not retain
// cursor state, so evaluating it is deterministic even when the caller's
// clock moves backwards.
type Schedule struct {
	kind    Kind
	weekday time.Weekday
	hour    uint8
	minute  uint8
	second  uint8
	text    string
}

// Parse validates and parses one canonical backup frequency.
//
// The accepted forms are exactly "*-*-* HH:MM:SS" and
// "Mon|Tue|Wed|Thu|Fri|Sat|Sun *-*-* HH:MM:SS". All occurrences are in UTC.
func Parse(value string) (Schedule, error) {
	if len(value) != 14 && len(value) != 18 {
		return Schedule{}, invalid(value)
	}

	calendar := value
	kind := KindDaily
	weekday := time.Sunday
	if len(value) == 18 {
		kind = KindWeekly
		if value[3] != ' ' {
			return Schedule{}, invalid(value)
		}
		var ok bool
		weekday, ok = parseWeekday(value[:3])
		if !ok {
			return Schedule{}, invalid(value)
		}
		calendar = value[4:]
	}

	if calendar[0:6] != "*-*-* " || calendar[8] != ':' || calendar[11] != ':' {
		return Schedule{}, invalid(value)
	}
	hour, ok := parseTwoDigits(calendar[6:8], 23)
	if !ok {
		return Schedule{}, invalid(value)
	}
	minute, ok := parseTwoDigits(calendar[9:11], 59)
	if !ok {
		return Schedule{}, invalid(value)
	}
	second, ok := parseTwoDigits(calendar[12:14], 59)
	if !ok {
		return Schedule{}, invalid(value)
	}

	return Schedule{
		kind:    kind,
		weekday: weekday,
		hour:    uint8(hour),
		minute:  uint8(minute),
		second:  uint8(second),
		text:    value,
	}, nil
}

// Kind reports whether the schedule is daily or weekly. Invalid is returned
// only by the zero value, which cannot be returned by Parse successfully.
func (schedule Schedule) Kind() Kind {
	return schedule.kind
}

// IsDaily reports whether the schedule runs every UTC day.
func (schedule Schedule) IsDaily() bool {
	return schedule.kind == KindDaily
}

// IsWeekly reports whether the schedule runs on one UTC weekday.
func (schedule Schedule) IsWeekly() bool {
	return schedule.kind == KindWeekly
}

// Weekday reports the configured UTC weekday. Daily schedules return Sunday;
// callers should use IsWeekly before interpreting it.
func (schedule Schedule) Weekday() time.Weekday {
	return schedule.weekday
}

// Hour reports the configured UTC hour.
func (schedule Schedule) Hour() int {
	return int(schedule.hour)
}

// Minute reports the configured UTC minute.
func (schedule Schedule) Minute() int {
	return int(schedule.minute)
}

// Second reports the configured UTC second.
func (schedule Schedule) Second() int {
	return int(schedule.second)
}

// String returns the original canonical frequency text.
func (schedule Schedule) String() string {
	return schedule.text
}

// NextOccurrence returns the first occurrence strictly after after. It returns
// a validation error for an invalid Schedule and an internal error when after
// is too close to time.Time's maximum civil instant for a later occurrence to
// be represented. On success, the result is always strictly after after.
//
// The input instant may have any location or sub-second precision; comparison
// is by the instant after conversion to UTC, while occurrences always have
// zero nanoseconds in UTC.
func (schedule Schedule) NextOccurrence(after time.Time) (time.Time, error) {
	if schedule.kind == KindInvalid {
		return time.Time{}, invalid(schedule.text)
	}
	afterUTC := after.UTC()
	days := 0
	if schedule.kind == KindDaily {
		if !schedule.occursLaterOnSameDate(afterUTC) {
			days = 1
		}
	} else {
		days = (int(schedule.weekday) - int(afterUTC.Weekday()) + 7) % 7
		if days == 0 && !schedule.occursLaterOnSameDate(afterUTC) {
			days = 7
		}
	}

	candidate, ok := schedule.occurrenceDaysFrom(afterUTC, days)
	if !ok || !candidate.After(afterUTC) {
		return time.Time{}, noRepresentableNext(schedule, afterUTC)
	}
	return candidate, nil
}

// LatestOccurrence returns the latest occurrence strictly within
// (lastEvaluatedAt, now]. The boolean is false when that interval contains no
// representable occurrence, including when now is not after lastEvaluatedAt or
// when the preceding calendar occurrence is below time.Time's minimum civil
// instant. It returns a validation error for an invalid Schedule. On success,
// a true result is always within (lastEvaluatedAt, now].
func (schedule Schedule) LatestOccurrence(lastEvaluatedAt, now time.Time) (time.Time, bool, error) {
	if schedule.kind == KindInvalid {
		return time.Time{}, false, invalid(schedule.text)
	}
	lastUTC := lastEvaluatedAt.UTC()
	nowUTC := now.UTC()
	if !nowUTC.After(lastUTC) {
		return time.Time{}, false, nil
	}

	days := 0
	if schedule.kind == KindWeekly {
		days = (int(nowUTC.Weekday()) - int(schedule.weekday) + 7) % 7
		if days == 0 && schedule.occursLaterOnSameDate(nowUTC) {
			days = 7
		}
	} else if schedule.occursLaterOnSameDate(nowUTC) {
		days = 1
	}

	candidate, ok := schedule.occurrenceDaysFrom(nowUTC, -days)
	if !ok {
		return time.Time{}, false, nil
	}
	if candidate.After(nowUTC) {
		return time.Time{}, false, occurrenceInvariantError(schedule, lastUTC, nowUTC, candidate)
	}
	if !candidate.After(lastUTC) {
		return time.Time{}, false, nil
	}
	return candidate, true, nil
}
func (schedule Schedule) occurrenceDaysFrom(value time.Time, days int) (time.Time, bool) {
	utc := value.UTC()
	candidate := time.Date(
		utc.Year(), utc.Month(), utc.Day()+days,
		int(schedule.hour), int(schedule.minute), int(schedule.second), 0,
		time.UTC,
	)
	year, month, day := candidate.Date()
	if candidate.Hour() != int(schedule.hour) || candidate.Minute() != int(schedule.minute) ||
		candidate.Second() != int(schedule.second) || candidate.Nanosecond() != 0 {
		return time.Time{}, false
	}
	if days > 0 && !candidate.After(utc) || days < 0 && !candidate.Before(utc) {
		return time.Time{}, false
	}
	if days == 0 && (year != utc.Year() || month != utc.Month() || day != utc.Day()) {
		return time.Time{}, false
	}
	return candidate, true
}

func (schedule Schedule) occursLaterOnSameDate(value time.Time) bool {
	scheduledSecond := int(schedule.hour)*60*60 + int(schedule.minute)*60 + int(schedule.second)
	currentSecond := value.Hour()*60*60 + value.Minute()*60 + value.Second()
	return scheduledSecond > currentSecond
}

func parseWeekday(value string) (time.Weekday, bool) {
	switch value {
	case "Mon":
		return time.Monday, true
	case "Tue":
		return time.Tuesday, true
	case "Wed":
		return time.Wednesday, true
	case "Thu":
		return time.Thursday, true
	case "Fri":
		return time.Friday, true
	case "Sat":
		return time.Saturday, true
	case "Sun":
		return time.Sunday, true
	default:
		return time.Sunday, false
	}
}

func parseTwoDigits(value string, maximum int) (int, bool) {
	if len(value) != 2 || value[0] < '0' || value[0] > '9' || value[1] < '0' || value[1] > '9' {
		return 0, false
	}
	parsed := int(value[0]-'0')*10 + int(value[1]-'0')
	return parsed, parsed <= maximum
}

func invalid(value string) error {
	return errs.Newf(errs.KindValidationFailed, "backup schedule is invalid: %q", value)
}

func noRepresentableNext(schedule Schedule, after time.Time) error {
	return errs.Newf(
		errs.KindInternal,
		"backup schedule %q has no representable occurrence strictly after %s",
		schedule.text,
		after,
	)
}

func occurrenceInvariantError(schedule Schedule, last, now, candidate time.Time) error {
	return errs.Newf(
		errs.KindInternal,
		"backup schedule %q produced occurrence %s outside (%s, %s]",
		schedule.text,
		candidate,
		last,
		now,
	)
}
