package cli

import (
	"strings"
	"testing"
)

// QA: SCRIPT-01; pure YAML decoding only, not resource resolution, persistence, or execution.
// Rationale: the CLI file carries authored labels and preserves explicit false
// without importing the Controller's Blueprint parser or domain model.
func TestScriptExecutionFilePreservesAuthoredContext(t *testing.T) {
	image := "setup@sha256:" + strings.Repeat("a", 64)
	value := "mode: explicit\nimage: " + image + "\nuser: '0:0'\n" +
		"volumes:\n  - volume: tls-data\n    target: /etc/tls\n    read_only: false\nentries: [tls-seed]\n"
	got, err := decodeScriptExecutionFile(value)
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode != "explicit" || got.Image != image || got.User != "0:0" || len(got.Volumes) != 1 ||
		got.Volumes[0].Volume != "tls-data" || got.Volumes[0].Target != "/etc/tls" || got.Volumes[0].ReadOnly ||
		len(got.Entries) != 1 || got.Entries[0] != "tls-seed" {
		t.Fatalf("file lost authored context: %#v", got)
	}
	got, err = decodeScriptExecutionFile("mode: inherited\n")
	if err != nil || got.Mode != "inherited" || len(got.Volumes) != 0 || len(got.Entries) != 0 {
		t.Fatalf("inherited context = %#v, %v", got, err)
	}
}

// QA: SCRIPT-01, SCRIPT-06, UI-03; pure YAML rejection only, not Controller resource validation.
// Rationale: bounded files reject null/coerced decisions, merge-erased presence,
// duplicate/unknown fields and extra documents before resolving any resource.
func TestScriptExecutionFileRejectsAmbiguousDecisions(t *testing.T) {
	explicit := "mode: explicit\nimage: setup@sha256:" + strings.Repeat("a", 64) + "\nuser: '0:0'\n"
	for _, value := range []string{
		"", "null", "[]", "mode: other", "mode: inherited\nimage: ''", "mode: inherited\nuser: null",
		"mode: inherited\nentries: []", "mode: inherited\nunknown: true", "mode: inherited\nmode: inherited",
		"mode: inherited\n---\nmode: inherited", "mode: explicit\nimage: image\nuser: 0",
		"mode: explicit\nimage: null\nuser: '0:0'", "mode: explicit\nuser: '0:0'",
		explicit + "volumes: null", explicit + "entries: null", explicit + "entries: [null]",
		explicit + "volumes: [{volume: tls, target: /data}]",
		explicit + "volumes: [{volume: tls, target: /data, read_only: null}]",
		explicit + "volumes: [{volume: tls, target: /data, read_only: 'false'}]",
		explicit + "volumes: [{volume: tls, target: /data, read_only: false, extra: true}]",
		"mode: inherited\n<<: {entries: null}",
		explicit + "volumes: [" + strings.Repeat("{volume: tls, target: /data, read_only: false},", 33) + "]",
		explicit + "entries: [" + strings.Repeat("entry,", 65) + "]",
		strings.Repeat(" ", 64*1024+1),
	} {
		if _, err := decodeScriptExecutionFile(value); err == nil {
			t.Fatalf("accepted malformed file %.150q", value)
		}
	}
}
