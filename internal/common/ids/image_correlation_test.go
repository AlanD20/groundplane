package ids

import (
	"bytes"
	"strings"
	"testing"
)

// Rationale: correlation ids must never repeat or wrap within an authenticated
// session, and unavailable entropy disables only that session's image lookup.
func TestImageCorrelationCounterNeverWraps(t *testing.T) {
	seed := bytes.Repeat([]byte{255}, 16)
	seed[15] = 254
	counter := imageCorrelationFromReader(bytes.NewReader(seed))
	for _, want := range []string{strings.Repeat("f", 30) + "fe", strings.Repeat("f", 32)} {
		got, ok := counter.Next()
		if !ok || got != want {
			t.Fatalf("Next=%q,%v want=%q", got, ok, want)
		}
	}
	if value, ok := counter.Next(); ok || value != "" {
		t.Fatal("counter wrapped")
	}
	for _, seed := range [][]byte{nil, make([]byte, 16), []byte{1}} {
		counter := imageCorrelationFromReader(bytes.NewReader(seed))
		if _, ok := counter.Next(); ok {
			t.Fatal("accepted unavailable/zero seed")
		}
	}
}
