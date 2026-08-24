package backupschedule

import (
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: ADR0046 exposes only the two canonical systemd-calendar-shaped
// forms, and every weekday spelling must map to the same UTC weekday identity.
func TestParseAcceptsCanonicalDailyAndWeeklySchedules(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		kind    Kind
		weekday time.Weekday
		hour    int
		minute  int
		second  int
	}{
		{name: "daily midnight", value: "*-*-* 00:00:00", kind: KindDaily, weekday: time.Sunday},
		{
			name:    "daily last second",
			value:   "*-*-* 23:59:59",
			kind:    KindDaily,
			weekday: time.Sunday,
			hour:    23,
			minute:  59,
			second:  59,
		},
		{
			name:    "monday",
			value:   "Mon *-*-* 01:02:03",
			kind:    KindWeekly,
			weekday: time.Monday,
			hour:    1,
			minute:  2,
			second:  3,
		},
		{
			name:    "tuesday",
			value:   "Tue *-*-* 01:02:03",
			kind:    KindWeekly,
			weekday: time.Tuesday,
			hour:    1,
			minute:  2,
			second:  3,
		},
		{
			name:    "wednesday",
			value:   "Wed *-*-* 01:02:03",
			kind:    KindWeekly,
			weekday: time.Wednesday,
			hour:    1,
			minute:  2,
			second:  3,
		},
		{
			name:    "thursday",
			value:   "Thu *-*-* 01:02:03",
			kind:    KindWeekly,
			weekday: time.Thursday,
			hour:    1,
			minute:  2,
			second:  3,
		},
		{
			name:    "friday",
			value:   "Fri *-*-* 01:02:03",
			kind:    KindWeekly,
			weekday: time.Friday,
			hour:    1,
			minute:  2,
			second:  3,
		},
		{
			name:    "saturday",
			value:   "Sat *-*-* 01:02:03",
			kind:    KindWeekly,
			weekday: time.Saturday,
			hour:    1,
			minute:  2,
			second:  3,
		},
		{
			name:    "sunday",
			value:   "Sun *-*-* 01:02:03",
			kind:    KindWeekly,
			weekday: time.Sunday,
			hour:    1,
			minute:  2,
			second:  3,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schedule, err := Parse(test.value)
			if err != nil {
				t.Fatalf("Parse(%q) error = %v", test.value, err)
			}
			if schedule.Kind() != test.kind || schedule.Weekday() != test.weekday ||
				schedule.Hour() != test.hour || schedule.Minute() != test.minute || schedule.Second() != test.second {
				t.Fatalf("Parse(%q) = kind %d weekday %s %02d:%02d:%02d", test.value, schedule.Kind(),
					schedule.Weekday(), schedule.Hour(), schedule.Minute(), schedule.Second())
			}
			if schedule.String() != test.value {
				t.Fatalf("String() = %q, want %q", schedule.String(), test.value)
			}
			if schedule.IsDaily() != (test.kind == KindDaily) || schedule.IsWeekly() != (test.kind == KindWeekly) {
				t.Fatalf("kind predicates disagree for %q", test.value)
			}
		})
	}
}

// Rationale: exact widths and separators are part of the persisted contract;
// accepting whitespace variants, dates, ranges, lists, repetition, or zones
// would make equivalent policy values serialize to different schedules.
func TestParseRejectsNonCanonicalOrOutOfRangeSchedules(t *testing.T) {
	invalidValues := []string{
		"", "*-*-* 0:00:00", "*-*-* 00:0:00", "*-*-* 00:00:0",
		"*-*-* 00:00", "*-*-* 00:00:00 ", "*-*-* 00:00:00 UTC",
		"*-*-* 00:00:00+00:00", "*-*-* 00:00:00Z", "*-*-* 00:00:00.000",
		"*-*-* 24:00:00", "*-*-* 00:60:00", "*-*-* 00:00:60",
		"*-*-* -1:00:00", "*-*-* 0a:00:00", "*-*-* 00:0a:00",
		"*-*-* 00:00:0a", "*-*-* 00:00:00\t", "*-*-*  00:00:00",
		"*-*-*\t00:00:00", " *-*-* 00:00:00", "*-*-* 00:00:00\n",
		"2026-01-01 00:00:00", "*-01-* 00:00:00", "*-*-01 00:00:00",
		"*-*-* 00,00,00", "*-*-* 00:00/01:00", "*-*-* 00:00:00/1",
		"Mon-Fri *-*-* 00:00:00", "Mon,Wed *-*-* 00:00:00", "Mon *-*-* 00:00:00/1",
		"mon *-*-* 00:00:00", "Monday *-*-* 00:00:00", "Mån *-*-* 00:00:00",
		"Mon  *-*-* 00:00:00", "Mon\t*-*-* 00:00:00", "Mon *-*-* 00:00:00 UTC",
		"Mon *-*-* 00:00:00+00:00", "Mon *-*-* 00:00:00Z", "Mon *-*-* 00:00:00.000",
	}
	for _, value := range invalidValues {
		_, err := Parse(value)
		if err == nil {
			t.Errorf("Parse(%q) unexpectedly succeeded", value)
			continue
		}
		if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
			t.Errorf("Parse(%q) error = %v, want validation.failed", value, err)
		}
	}
}

// Rationale: the evaluator is a pure function of the supplied instant, so a
// repeated call after a backward clock step must not invent or regress a
// cursor occurrence.
func TestLatestOccurrenceHandlesEmptyAndBackwardIntervals(t *testing.T) {
	schedule := mustParse(t, "*-*-* 12:00:00")
	tests := []struct {
		name string
		last time.Time
		now  time.Time
	}{
		{name: "equal", last: utc(2026, 1, 2, 12, 0, 0), now: utc(2026, 1, 2, 12, 0, 0)},
		{name: "backward", last: utc(2026, 1, 3, 12, 0, 0), now: utc(2026, 1, 2, 12, 0, 0)},
		{name: "before first", last: utc(2026, 1, 2, 12, 0, 0), now: utc(2026, 1, 2, 11, 59, 59)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := mustLatest(t, schedule, test.last, test.now)
			if ok || !got.IsZero() {
				t.Fatalf("LatestOccurrence(%s, %s) = %s, %t; want no occurrence", test.last, test.now, got, ok)
			}
		})
	}
}

// Rationale: a scheduler cursor includes the left boundary but excludes an
// occurrence exactly at its previous evaluation, while now is inclusive for
// catch-up dispatch.
func TestLatestOccurrenceUsesStrictLeftAndInclusiveRightBoundaries(t *testing.T) {
	schedule := mustParse(t, "*-*-* 12:00:00")
	last := utc(2026, 2, 1, 11, 0, 0)
	now := utc(2026, 2, 3, 12, 0, 0)
	got, ok := mustLatest(t, schedule, last, now)
	if !ok || !got.Equal(utc(2026, 2, 3, 12, 0, 0)) {
		t.Fatalf("LatestOccurrence() = %s, %t; want 2026-02-03 12:00:00 UTC, true", got, ok)
	}

	last = utc(2026, 2, 3, 12, 0, 0)
	if got, ok := mustLatest(t, schedule, last, now); ok || !got.IsZero() {
		t.Fatalf("left-bound occurrence returned %s, %t", got, ok)
	}
}

// Rationale: daily schedules must remain calendar based at leap days and year
// boundaries, rather than assuming every month has a fixed number of days.
func TestDailyOccurrenceCrossesLeapAndYearBoundaries(t *testing.T) {
	schedule := mustParse(t, "*-*-* 00:00:00")
	tests := []struct {
		name  string
		after time.Time
		want  time.Time
	}{
		{name: "leap day", after: utc(2028, 2, 28, 23, 59, 59), want: utc(2028, 2, 29, 0, 0, 0)},
		{name: "after leap day", after: utc(2028, 2, 29, 0, 0, 0), want: utc(2028, 3, 1, 0, 0, 0)},
		{name: "new year", after: utc(2026, 12, 31, 23, 59, 59), want: utc(2027, 1, 1, 0, 0, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := mustNext(t, schedule, test.after); !got.Equal(test.want) {
				t.Fatalf("NextOccurrence(%s) = %s, want %s", test.after, got, test.want)
			}
		})
	}
}

// Rationale: strict-after evaluation must preserve the same UTC instant
// semantics for sub-second values and non-UTC input locations.
func TestNextOccurrenceIsStrictAndUTC(t *testing.T) {
	schedule := mustParse(t, "*-*-* 12:00:00")
	if got, want := mustNext(t, schedule, utc(2026, 3, 1, 11, 59, 59)), utc(2026, 3, 1, 12, 0, 0); !got.Equal(want) {
		t.Fatalf("before occurrence = %s, want %s", got, want)
	}
	if got, want := mustNext(t, schedule, utc(2026, 3, 1, 12, 0, 0)), utc(2026, 3, 2, 12, 0, 0); !got.Equal(want) {
		t.Fatalf("at occurrence = %s, want %s", got, want)
	}
	if got, want := mustNext(t, schedule, time.Date(2026, 3, 1, 13, 0, 0, 0,
		time.FixedZone("west", -5*60*60))), utc(2026, 3, 2, 12, 0, 0); !got.Equal(want) {
		t.Fatalf("non-UTC input = %s, want %s", got, want)
	}
	if got, want := mustNext(t, schedule, time.Date(2026, 3, 1, 12, 0, 0, 1, time.UTC)),
		utc(2026, 3, 2, 12, 0, 0); !got.Equal(want) {
		t.Fatalf("sub-second input = %s, want %s", got, want)
	}
}

// Rationale: weekly recurrence must choose the next matching weekday in both
// directions around Sunday/Monday and across the Gregorian year boundary.
func TestWeeklyOccurrenceCrossesWeekAndYearBoundaries(t *testing.T) {
	schedule := mustParse(t, "Mon *-*-* 00:00:00")
	tests := []struct {
		name  string
		after time.Time
		want  time.Time
	}{
		{name: "sunday to monday", after: utc(2026, 1, 4, 23, 59, 59), want: utc(2026, 1, 5, 0, 0, 0)},
		{name: "same monday after time", after: utc(2026, 1, 5, 0, 0, 1), want: utc(2026, 1, 12, 0, 0, 0)},
		{name: "december monday to january", after: utc(2026, 12, 28, 0, 0, 1), want: utc(2027, 1, 4, 0, 0, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := mustNext(t, schedule, test.after); !got.Equal(test.want) {
				t.Fatalf("NextOccurrence(%s) = %s, want %s", test.after, got, test.want)
			}
		})
	}
}

// Rationale: all seven weekly schedules share one invariant: the next result
// is strictly later than the input and has exactly the configured weekday and
// UTC wall-clock time, including at leap/year boundaries.
func TestWeeklyNextOccurrenceProperties(t *testing.T) {
	for weekday := time.Sunday; weekday <= time.Saturday; weekday++ {
		value := weekdayName(weekday) + " *-*-* 23:59:59"
		schedule := mustParse(t, value)
		for year := 2024; year <= 2032; year++ {
			for month := time.January; month <= time.December; month++ {
				after := time.Date(year, month, 1, 23, 59, 59, 0, time.UTC)
				got := mustNext(t, schedule, after)
				if !got.After(after) || got.Weekday() != weekday || got.Hour() != 23 || got.Minute() != 59 ||
					got.Second() != 59 ||
					got.Nanosecond() != 0 ||
					got.Location() != time.UTC {
					t.Fatalf("%s after %s = %s, violates next-occurrence invariant", value, after, got)
				}
			}
		}
	}
}

// Rationale: latest catch-up and next scheduling must agree on the same
// recurrence sequence, so a latest result is never outside the supplied
// interval and next after it advances to the following period.
func TestLatestAndNextOccurrenceAgreeAcrossCatchUp(t *testing.T) {
	tests := []string{"*-*-* 06:30:15", "Thu *-*-* 23:59:59"}
	for _, value := range tests {
		schedule := mustParse(t, value)
		last := utc(2027, 2, 27, 7, 0, 0)
		now := utc(2028, 3, 1, 23, 0, 0)
		latest, ok := mustLatest(t, schedule, last, now)
		if !ok || !latest.After(last) || latest.After(now) {
			t.Fatalf("%s latest = %s, %t outside (%s, %s]", value, latest, ok, last, now)
		}
		next := mustNext(t, schedule, latest)
		if !next.After(latest) {
			t.Fatalf("%s next after latest %s = %s, not strictly later", value, latest, next)
		}
	}
}

// Rationale: ADR0046 requires next_run_at for every valid enabled policy, so
// reaching time.Time's maximum civil instant is an internal evaluation error,
// never a wrapped or non-increasing occurrence.
func TestNextOccurrenceErrorsWhenNoLaterTimeIsRepresentable(t *testing.T) {
	schedule := mustParse(t, "*-*-* 00:00:00")
	after := maximumRepresentableTime()

	got, err := schedule.NextOccurrence(after)
	if !got.IsZero() {
		t.Fatalf("NextOccurrence(%s) = %s, want zero time", after, got)
	}
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("NextOccurrence(%s) error = %v, want internal error", after, err)
	}
}

// Rationale: the maximum civil day is only partially representable, so date
// shifting must construct the configured clock time directly instead of
// carrying an unrepresentable late clock time from the preceding day.
func TestNextOccurrenceReachesRepresentableTimeOnMaximumCivilDay(t *testing.T) {
	maximum := maximumRepresentableTime()
	after := time.Date(maximum.Year(), maximum.Month(), maximum.Day()-1, 23, 59, 59, 0, time.UTC)
	want := time.Date(maximum.Year(), maximum.Month(), maximum.Day(), 15, 0, 0, 0, time.UTC)
	values := []string{
		"*-*-* 15:00:00",
		weekdayName(maximum.Weekday()) + " *-*-* 15:00:00",
	}

	for _, value := range values {
		t.Run(value, func(t *testing.T) {
			schedule := mustParse(t, value)
			if got := mustNext(t, schedule, after); !got.Equal(want) {
				t.Fatalf("NextOccurrence(%s) = %s, want %s", after, got, want)
			}
		})
	}
}

// Rationale: LatestOccurrence's false result explicitly represents an empty
// interval, including an occurrence that would fall before time.Time's minimum
// civil date; it must not wrap that occurrence beyond now.
func TestLatestOccurrenceDoesNotWrapBeforeMinimumRepresentableDate(t *testing.T) {
	schedule := mustParse(t, "*-*-* 23:59:59")
	last := minimumRepresentableTime()
	now := last.Add(time.Hour)

	got, ok, err := schedule.LatestOccurrence(last, now)
	if err != nil {
		t.Fatalf("LatestOccurrence(%s, %s) error = %v", last, now, err)
	}
	if ok || !got.IsZero() {
		t.Fatalf("LatestOccurrence(%s, %s) = %s, %t; want no occurrence", last, now, got, ok)
	}
}

// Rationale: the representable endpoints remain usable when an occurrence
// actually lies inside them; boundary handling must reject only overflow, not
// ordinary minimum- or maximum-date recurrence.
func TestLatestOccurrenceStaysWithinExtremeRepresentableIntervals(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		last     time.Time
		now      time.Time
		wantHour int
	}{
		{
			name:     "minimum date",
			value:    "*-*-* 00:30:00",
			last:     minimumRepresentableTime(),
			now:      minimumRepresentableTime().Add(time.Hour),
			wantHour: 0,
		},
		{
			name:     "maximum date",
			value:    "*-*-* 15:00:00",
			last:     maximumRepresentableTime().Add(-time.Hour),
			now:      maximumRepresentableTime(),
			wantHour: 15,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schedule := mustParse(t, test.value)
			got, ok := mustLatest(t, schedule, test.last, test.now)
			if !ok || !got.After(test.last) || got.After(test.now) || got.Hour() != test.wantHour {
				t.Fatalf("LatestOccurrence(%s, %s) = %s, %t; want an in-range occurrence",
					test.last, test.now, got, ok)
			}
		})
	}
}

func mustParse(t *testing.T, value string) Schedule {
	t.Helper()
	schedule, err := Parse(value)
	if err != nil {
		t.Fatalf("Parse(%q) error = %v", value, err)
	}
	return schedule
}

func mustNext(t *testing.T, schedule Schedule, after time.Time) time.Time {
	t.Helper()
	occurrence, err := schedule.NextOccurrence(after)
	if err != nil {
		t.Fatalf("NextOccurrence(%s) error = %v", after, err)
	}
	return occurrence
}

func mustLatest(t *testing.T, schedule Schedule, last, now time.Time) (time.Time, bool) {
	t.Helper()
	occurrence, ok, err := schedule.LatestOccurrence(last, now)
	if err != nil {
		t.Fatalf("LatestOccurrence(%s, %s) error = %v", last, now, err)
	}
	return occurrence, ok
}

func utc(year int, month time.Month, day, hour, minute, second int) time.Time {
	return time.Date(year, month, day, hour, minute, second, 0, time.UTC)
}

func minimumRepresentableTime() time.Time {
	return time.Date(-292277022400, time.March, 1, 0, 0, 0, 0, time.UTC)
}

func maximumRepresentableTime() time.Time {
	const unixToInternal = int64(62135596800)
	const maximumInternalSecond = int64(1<<63 - 1)
	return time.Unix(maximumInternalSecond-unixToInternal, int64(time.Second-time.Nanosecond)).UTC()
}

func weekdayName(weekday time.Weekday) string {
	switch weekday {
	case time.Monday:
		return "Mon"
	case time.Tuesday:
		return "Tue"
	case time.Wednesday:
		return "Wed"
	case time.Thursday:
		return "Thu"
	case time.Friday:
		return "Fri"
	case time.Saturday:
		return "Sat"
	case time.Sunday:
		return "Sun"
	default:
		return ""
	}
}
