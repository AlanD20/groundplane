package jcs

import (
	"bytes"
	"embed"
	"math"
	"strconv"
	"strings"
	"testing"
)

//go:embed testdata/input/*.json testdata/output/*.json
var referenceVectors embed.FS

// Rationale: RFC 8785 reference vectors must remain byte-for-byte stable.
func TestCanonicalizeReferenceVectors(t *testing.T) {
	t.Parallel()
	for _, name := range []string{
		"arrays.json", "french.json", "structures.json", "unicode.json", "values.json", "weird.json",
	} {
		name := name
		t.Run(name, func(t *testing.T) {
			raw, err := referenceVectors.ReadFile("testdata/input/" + name)
			if err != nil {
				t.Fatal(err)
			}
			expected, err := referenceVectors.ReadFile("testdata/output/" + name)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := Canonicalize(raw)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(actual, expected) {
				t.Fatalf("canonical bytes = %q, want %q", actual, expected)
			}
		})
	}
}

// Rationale: Appendix B exercises every specified boundary in RFC 8785 number serialization.
func TestAppendixBNumberSerialization(t *testing.T) {
	t.Parallel()
	cases := []struct {
		bits uint64
		want string
	}{
		{0x0000000000000000, "0"},
		{0x8000000000000000, "0"},
		{0x0000000000000001, "5e-324"},
		{0x8000000000000001, "-5e-324"},
		{0x7fefffffffffffff, "1.7976931348623157e+308"},
		{0xffefffffffffffff, "-1.7976931348623157e+308"},
		{0x4340000000000000, "9007199254740992"},
		{0xc340000000000000, "-9007199254740992"},
		{0x4430000000000000, "295147905179352830000"},
		{0x44b52d02c7e14af5, "9.999999999999997e+22"},
		{0x44b52d02c7e14af6, "1e+23"},
		{0x44b52d02c7e14af7, "1.0000000000000001e+23"},
		{0x444b1ae4d6e2ef4e, "999999999999999700000"},
		{0x444b1ae4d6e2ef4f, "999999999999999900000"},
		{0x444b1ae4d6e2ef50, "1e+21"},
		{0x3eb0c6f7a0b5ed8c, "9.999999999999997e-7"},
		{0x3eb0c6f7a0b5ed8d, "0.000001"},
		{0x41b3de4355555553, "333333333.3333332"},
		{0x41b3de4355555554, "333333333.33333325"},
		{0x41b3de4355555555, "333333333.3333333"},
		{0x41b3de4355555556, "333333333.3333334"},
		{0x41b3de4355555557, "333333333.33333343"},
		{0xbecbf647612f3696, "-0.0000033333333333333333"},
		{0x43143ff3c1cb0959, "1424953923781206.2"},
	}
	for _, testCase := range cases {
		rawNumber := strconv.FormatFloat(math.Float64frombits(testCase.bits), 'g', -1, 64)
		raw := []byte(`{"n":` + rawNumber + `}`)
		want := []byte(`{"n":` + testCase.want + `}`)
		actual, err := Canonicalize(raw)
		if err != nil {
			t.Fatalf("bits %016x: %v", testCase.bits, err)
		}
		if !bytes.Equal(actual, want) {
			t.Fatalf("bits %016x: canonical bytes = %q, want %q", testCase.bits, actual, want)
		}
	}
	for _, raw := range []string{`{"n":NaN}`, `{"n":Infinity}`, `{"n":-Infinity}`} {
		if _, err := Canonicalize([]byte(raw)); err == nil {
			t.Fatalf("non-finite number %q was accepted", raw)
		}
	}
}

// Rationale: release input must be valid UTF-8 JSON without a leading BOM.
func TestStrictGateRejectsInvalidUTF8AndBOM(t *testing.T) {
	t.Parallel()
	if _, err := Canonicalize([]byte{'{', '"', 'x', '"', ':', 0xff, '}'}); err == nil {
		t.Fatal("invalid utf-8 was accepted")
	}
	if _, err := Canonicalize([]byte{0xef, 0xbb, 0xbf, '{', '}'}); err == nil {
		t.Fatal("utf-8 bom was accepted")
	}
}

// Rationale: JSON strings must contain only correctly ordered UTF-16 surrogate pairs.
func TestStrictGateRejectsEveryLoneOrMisorderedSurrogate(t *testing.T) {
	t.Parallel()
	invalid := []string{
		`{"x":"\uD800"}`,
		`{"x":"\uDC00"}`,
		`{"x":"\uD800\uD800"}`,
		`{"x":"\uD800\u0041"}`,
		`{"x":"\uDC00\uD800"}`,
		`{"x":"\uDC00\uDC00"}`,
		`{"x":"\uD800\n"}`,
	}
	for _, raw := range invalid {
		if _, err := Canonicalize([]byte(raw)); err == nil {
			t.Fatalf("invalid surrogate sequence %q was accepted", raw)
		}
	}
	if _, err := Canonicalize([]byte(`{"x":"\uD83D\uDE00"}`)); err != nil {
		t.Fatalf("valid surrogate pair was rejected: %v", err)
	}
}

// Rationale: release input must use the JSON number grammar and canonical number spelling.
func TestStrictGateRejectsMalformedNumbers(t *testing.T) {
	t.Parallel()
	malformed := []string{
		`{"n":+1}`, `{"n":.1}`, `{"n":1.}`, `{"n":01}`, `{"n":-01}`,
		`{"n":1e}`, `{"n":1e+}`, `{"n":1e-}`, `{"n":--1}`, `{"n":0x1}`,
		`{"n":NaN}`, `{"n":Infinity}`, `{"n":-Infinity}`,
	}
	for _, raw := range malformed {
		if _, err := Canonicalize([]byte(raw)); err == nil {
			t.Fatalf("malformed number %q was accepted", raw)
		}
	}
	if _, err := RequireCanonical([]byte(`{"n":1.0}`)); err == nil {
		t.Fatal("noncanonical numeric spelling was admitted")
	}
}

// Rationale: strict release input must reject malformed boundaries, duplicates, and non-container roots.
func TestStrictGateRejectsSplitTokensTruncationDuplicatesAndTrailingRoots(t *testing.T) {
	t.Parallel()
	invalid := []string{
		`{"n":1 e2}`, `{"n":tr ue}`, `{"n":fa\nlse}`, `{"n":nu\tll}`,
		`{"x":"abc}`, `{"x`, `{"x":`, `{"x"`,
		`{"a":{"b":1,"b":2}}`, `[ {"a":1,"a":2} ]`, `{"a":1,"\u0061":2}`,
		`{"a":1} {"b":2}`, `[] []`, `null`, `true`, `false`, `1`,
	}
	for _, raw := range invalid {
		if _, err := Canonicalize([]byte(raw)); err == nil {
			t.Fatalf("invalid JSON %q was accepted", raw)
		}
	}
}

// Rationale: typed decoding must return fresh values and never mutate caller-owned state.
func TestDecodeReturnsFreshZeroAndDoesNotMutateCallerState(t *testing.T) {
	t.Parallel()
	type payload struct {
		Name  string   `json:"name"`
		Count int      `json:"count"`
		Items []string `json:"items"`
	}
	raw := []byte(`{"count":1,"items":["one"],"name":"first"}`)
	first, err := Decode[payload](raw)
	if err != nil {
		t.Fatal(err)
	}
	first.Items[0] = "changed"
	second, err := Decode[payload](raw)
	if err != nil {
		t.Fatal(err)
	}
	if second.Items[0] != "one" {
		t.Fatal("typed decode reused mutable state")
	}
	failed, err := Decode[payload]([]byte(`{"count":1,"name":"first"}`))
	if err == nil {
		t.Fatal("missing typed member was accepted")
	}
	if failed.Name != "" || failed.Count != 0 || failed.Items != nil {
		t.Fatal("failed decode returned a nonzero value")
	}
}

// Rationale: typed release values must reject unknown fields and preserve their canonical shape.
func TestDecodeRejectsUnknownAndTypedRoundTripLoss(t *testing.T) {
	t.Parallel()
	type payload struct {
		Name string `json:"name"`
		N    int    `json:"n"`
	}
	if _, err := Decode[payload]([]byte(`{"name":"x","n":1,"extra":true}`)); err == nil {
		t.Fatal("unknown typed member was accepted")
	}
	type lossy struct {
		N int `json:"n,omitempty"`
	}
	if _, err := Decode[lossy]([]byte(`{"n":0}`)); err == nil {
		t.Fatal("typed omitempty loss was accepted")
	}
}

// Rationale: package errors must be stable, lowercase, and suitable for operator-facing contracts.
func TestErrorsUseLowercaseStableMessages(t *testing.T) {
	t.Parallel()
	_, err := Canonicalize([]byte(`{"n":+1}`))
	if err == nil || strings.ToLower(err.Error()) != err.Error() {
		t.Fatalf("error is not lowercase: %v", err)
	}
}
