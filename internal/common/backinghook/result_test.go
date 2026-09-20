package backinghook

import (
	"bytes"
	"strings"
	"testing"
)

// QA: BACK-12. Rationale: shell-looking values, embedded equals signs and empty
// facts must survive literally; command output cannot choose its sensitivity.
func TestParseResultPreservesLiteralFacts(t *testing.T) {
	schema := []FactDefinition{{Key: "TOKEN", Secret: true}, {Key: "EMPTY"}}
	literal := `"$(do-not-execute)"=a=b # literal`
	content := []byte("\nEMPTY=\nTOKEN=" + literal + "\n\n")
	output, err := ParseResult(schema, content)
	if err != nil {
		t.Fatal(err)
	}
	defer output.Clear()
	clear(content)
	if len(output.Facts) != 2 || output.Facts[0].Key != "TOKEN" ||
		string(output.Facts[0].Value) != literal || !output.Facts[0].Secret ||
		output.Facts[1].Key != "EMPTY" || len(output.Facts[1].Value) != 0 || output.Facts[1].Secret {
		t.Fatal("literal values, declared sensitivity or independent result ownership were lost")
	}
}

// QA: BACK-13. Rationale: one malformed or unexpected result must reject the
// entire hook result, not publish the otherwise valid credential prefix.
func TestParseResultRejectsInvalidOutputAtomically(t *testing.T) {
	schema := []FactDefinition{{Key: "TOKEN", Secret: true}, {Key: "HOST"}}
	cases := map[string]string{
		"missing separator": "TOKEN=private\nHOST",
		"duplicate":         "TOKEN=private\nHOST=one\nHOST=two",
		"undeclared":        "TOKEN=private\nHOST=one\nEXTRA=value",
		"missing fact":      "TOKEN=private",
		"empty key":         "TOKEN=private\nHOST=one\n=value",
		"nul":               "TOKEN=private\x00value\nHOST=one",
		"oversized":         "TOKEN=" + strings.Repeat("x", MaximumResultBytes) + "\nHOST=one",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			output, err := ParseResult(schema, []byte(content))
			defer output.Clear()
			if err == nil || len(output.Facts) != 0 {
				t.Fatal("invalid output exposed partial facts or was accepted")
			}
			if strings.Contains(err.Error(), "private") {
				t.Fatal("validation error exposed a fact value")
			}
		})
	}
}

// QA: BACK-12/13. Rationale: compiled adapter outputs and shell result files
// must obey the same byte bound and cannot downgrade a declared secret.
func TestOutputValidationPreservesSensitivityAndSizeBound(t *testing.T) {
	schema := []FactDefinition{{Key: "TOKEN", Secret: true}}
	output := Output{Facts: []Fact{{Key: "TOKEN", Secret: true,
		Value: bytes.Repeat([]byte{'x'}, MaximumResultBytes-len("TOKEN=\n"))}}}
	defer output.Clear()
	if err := ValidateOutput(schema, output); err != nil {
		t.Fatal("exact bounded output rejected:", err)
	}
	output.Facts[0].Value = append(output.Facts[0].Value, 'x')
	if err := ValidateOutput(schema, output); err == nil {
		t.Fatal("oversized compiled output accepted")
	}
	output.Facts[0].Value = []byte("credential")
	output.Facts[0].Secret = false
	if err := ValidateOutput(schema, output); err == nil {
		t.Fatal("compiled output could downgrade a secret fact")
	}
}
