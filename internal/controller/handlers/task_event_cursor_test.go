package handlers

import (
	"errors"
	"net/http"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// TASK-06: ambiguous resume input must reject rather than silently normalize
// or overflow the cursor and skip retained observations.
func TestParseLastTaskEventIDAcceptsOnlyCanonicalSingleValues(t *testing.T) {
	for _, test := range []struct {
		name   string
		values []string
		want   uint64
		valid  bool
	}{
		{name: "absent", valid: true},
		{name: "zero", values: []string{"0"}, valid: true},
		{name: "positive", values: []string{"18446744073709551615"}, want: ^uint64(0), valid: true},
		{name: "empty", values: []string{""}},
		{name: "signed", values: []string{"+1"}},
		{name: "negative", values: []string{"-1"}},
		{name: "whitespace", values: []string{" 1"}},
		{name: "non_ascii", values: []string{"١"}},
		{name: "leading_zero", values: []string{"01"}},
		{name: "overflow", values: []string{"18446744073709551616"}},
		{name: "too_long", values: []string{"100000000000000000000"}},
		{name: "multiple", values: []string{"1", "2"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			header := make(http.Header)
			for _, value := range test.values {
				header.Add("Last-Event-ID", value)
			}
			got, err := parseLastTaskEventID(header)
			if test.valid {
				if err != nil || got != test.want {
					t.Fatalf("resume cursor = %d, %v; want %d", got, err, test.want)
				}
			} else if !errors.Is(err, errs.New(errs.KindMalformedRequest, "")) {
				t.Fatalf("ambiguous resume cursor was not rejected: %v", err)
			}
		})
	}
}
