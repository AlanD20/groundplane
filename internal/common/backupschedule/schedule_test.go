package backupschedule

import (
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// BAK-04: independent expected UTC instants catch field, calendar, interval and
// restricted-day OR errors that would schedule a backup at the wrong time.
func TestCronNextOccurrences(t *testing.T) {
	for _, test := range []struct{ expression, after, want string }{
		{"15 * * * *", "2026-10-06T02:15:00Z", "2026-10-06T03:15:00Z"},
		{"15 3 * * *", "2026-10-06T04:00:00+02:00", "2026-10-06T03:15:00Z"},
		{"15 3 * * SUN", "2026-10-06T12:00:00Z", "2026-10-11T03:15:00Z"},
		{"0 */6 * * *", "2026-10-06T05:59:59Z", "2026-10-06T06:00:00Z"},
		{"*/15 * * * *", "2026-10-06T23:59:59Z", "2026-10-07T00:00:00Z"},
		{"5,35 9-17 * * MON-FRI", "2026-10-06T17:36:00Z", "2026-10-07T09:05:00Z"},
		{"0 0 31 * *", "2026-04-30T00:00:00Z", "2026-05-31T00:00:00Z"},
		{"0 0 29 FEB *", "2096-03-01T00:00:00Z", "2104-02-29T00:00:00Z"},
		{"0 0 1 * MON", "2026-10-06T10:00:00Z", "2026-10-12T00:00:00Z"},
		{"0 0 1-31 * MON", "2026-10-06T10:00:00Z", "2026-10-07T00:00:00Z"},
		{"0 0 * jul sun", "2026-08-01T00:00:00Z", "2027-07-04T00:00:00Z"},
		{"0 0 30 FEB MON", "2026-02-01T00:00:00Z", "2026-02-02T00:00:00Z"},
		{"0 0 * * THU", "2026-10-06T12:00:00Z", "2026-10-08T00:00:00Z"},
	} {
		t.Run(test.expression, func(t *testing.T) {
			schedule := mustSchedule(t, test.expression)
			got, err := schedule.NextOccurrence(instant(t, test.after))
			if err != nil || !got.Equal(instant(t, test.want)) || got.Location() != time.UTC {
				t.Fatalf("next = %v, %v; want %s in UTC", got, err, test.want)
			}
		})
	}
}

// BAK-04: only the latest missed occurrence can dispatch. Exact boundaries,
// repeated evaluation and backward clock movement must not duplicate work.
func TestCronLatestOccurrence(t *testing.T) {
	for _, test := range []struct{ expression, last, now, want string }{
		{"* * * * *", "2020-01-01T00:00:00Z", "2026-10-06T05:32:31Z", "2026-10-06T05:32:00Z"},
		{"15 * * * *", "2026-10-06T02:15:00Z", "2026-10-06T03:15:00Z", "2026-10-06T03:15:00Z"},
		{"15 * * * *", "2026-10-06T03:15:00Z", "2026-10-06T03:15:30Z", ""},
		{"15 3 * * SUN", "2026-10-01T00:00:00Z", "2026-10-12T00:00:00Z", "2026-10-11T03:15:00Z"},
		{"0 0 29 FEB *", "2090-01-01T00:00:00Z", "2103-03-01T00:00:00Z", "2096-02-29T00:00:00Z"},
		{"0 0 * * *", "2026-10-06T12:00:00Z", "2026-10-06T12:00:00Z", ""},
		{"0 0 * * *", "2026-10-07T12:00:00Z", "2026-10-06T12:00:00Z", ""},
	} {
		t.Run(test.expression+test.now+test.last, func(t *testing.T) {
			schedule := mustSchedule(t, test.expression)
			for i := 0; i < 2; i++ {
				got, due, err := schedule.LatestOccurrence(instant(t, test.last), instant(t, test.now))
				if err != nil {
					t.Fatal(err)
				}
				if test.want == "" {
					if due || !got.IsZero() {
						t.Fatalf("unexpected occurrence %v", got)
					}
					continue
				}
				if !due || !got.Equal(instant(t, test.want)) {
					t.Fatalf("latest = %v, %t; want %s", got, due, test.want)
				}
			}
		})
	}
}

// BAK-04: invalid or superseded expressions must fail before a policy can be
// published, not become a never-running schedule or a second grammar.
func TestCronRejectsInvalidAndSupersededSchedules(t *testing.T) {
	for _, value := range []string{"", "*-*-* 03:15:00", "Sun *-*-* 03:15:00", "0 15 3 * * *", "@daily", "CRON_TZ=UTC 0 0 * * *", "60 * * * *", "0 24 * * *", "*/0 * * * *", "0 0 30 FEB *", "0 0 31 APR *", "0 0 * * 7", "0 0 L * *", "0 0 * * MON#2", "0 0 ? * *", "0 0 * * H", "1,,2 * * * *"} {
		if _, err := Parse(value); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
			t.Errorf("Parse(%q) error = %v", value, err)
		}
	}
	var zero Schedule
	if _, err := zero.NextOccurrence(time.Now()); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("zero schedule error = %v", err)
	}
}

func instant(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func mustSchedule(t *testing.T, value string) Schedule {
	t.Helper()
	parsed, err := Parse(value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
