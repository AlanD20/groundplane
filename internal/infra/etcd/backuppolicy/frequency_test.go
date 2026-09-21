package backuppolicy

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// The Controller-owned UTC grammar accepts only exact daily or weekly clocks
// at the full range boundaries and rejects broader systemd calendar syntax.
func TestFrequencyGrammarBoundaries(t *testing.T) {
	t.Parallel()
	for _, frequency := range []string{
		"*-*-* 00:00:00", "*-*-* 23:59:59", "Mon *-*-* 00:00:00", "Sun *-*-* 23:59:59",
	} {
		if err := ValidateFrequency(frequency); err != nil {
			t.Fatalf("ValidateFrequency(%q) error = %v", frequency, err)
		}
	}
	for _, frequency := range []string{
		"0 3 * * *", "*-*-* 24:00:00", "*-*-* 23:60:00", "*-*-* 23:59:60",
		"*-*-* 3:00:00", "mon *-*-* 03:00:00", "Mon,Tue *-*-* 03:00:00",
		"Mon  *-*-* 03:00:00", "*-*-* 03:00:00 UTC", "2026-08-23 03:00:00",
	} {
		if err := ValidateFrequency(frequency); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
			t.Fatalf("ValidateFrequency(%q) error = %v", frequency, err)
		}
	}
}
