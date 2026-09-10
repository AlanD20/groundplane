package blueprintparser

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
)

const scriptExecutionDocument = `x-gp-scripts:
  prepare:
    slug: prepare
    service: api
    when: pre-deploy
    script: echo prepare
    execution:
      mode: explicit
      image: example.invalid/setup@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
      user: "0:0"
      volumes:
        - volume: storage
          target: /data
          read_only: false
      entries: [SETUP_INPUT]
x-gp-entry:
  SETUP_INPUT:
    kind: env
    source: {literal: input}
    exposure: [api]
services:
  api:
    image: example/api:1
    volumes: [storage:/data:ro]
volumes:
  storage: {}
`

// Rationale: the authored context survives parsing/export with names and an
// explicit false decision; no generated runner authority enters desired YAML.
func TestParseScriptExecutionRoundTripsAuthoredGrants(t *testing.T) {
	result, err := parseScriptExecutionDocument(scriptExecutionDocument)
	if err != nil {
		t.Fatalf("Parse explicit Script: %v", err)
	}
	encoded, err := MarshalAuthoringDocument(AuthoringDocument{
		Envelope: result.Envelope, NetworkPool: result.Extensions.NetworkPool,
		Compose: []byte(
			"services: {api: {image: 'example/api:1', volumes: ['storage:/data:ro']}}\nvolumes: {storage: {}}\n",
		),
		Scripts: result.Extensions.Scripts, Entries: result.Extensions.Entries,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"execution:", "mode: explicit", "volume: storage", "read_only: false", "SETUP_INPUT"} {
		if !strings.Contains(string(encoded), expected) {
			t.Fatalf("export lost %q: %s", expected, encoded)
		}
	}
	if _, err := Parse(context.Background(), parserEnvironmentScope, parserBundle(
		[]string{"compose.yaml"}, map[string]string{"compose.yaml": string(encoded)},
	)); err != nil {
		t.Fatalf("exported explicit Script: %v", err)
	}
}

// Rationale: YAML coercion and omitted decisions must never broaden a setup
// runner's authority. Unknown, unexposed and colliding resources fail early.
func TestParseScriptExecutionRejectsInvalidDecisions(t *testing.T) {
	cases := map[string][2]string{
		"missing read only": {"          read_only: false\n", ""},
		"null read only":    {"read_only: false", "read_only: null"},
		"string read only":  {"read_only: false", "read_only: 'false'"},
		"null execution":    {"    execution:\n", "    execution: null\n    ignored_execution:\n"},
		"mutable image": {
			"example.invalid/setup@sha256:" + strings.Repeat("a", 64),
			"example.invalid/setup:latest",
		},
		"named user":            {"user: \"0:0\"", "user: app"},
		"unknown field":         {"mode: explicit", "mode: explicit\n      network: host"},
		"inherited fields":      {"mode: explicit", "mode: inherited"},
		"inherited empty field": {"mode: explicit", "mode: inherited\n      unknown: ''"},
		"unknown volume":        {"volume: storage", "volume: absent"},
		"unknown entry":         {"entries: [SETUP_INPUT]", "entries: [ABSENT]"},
		"unexposed entry":       {"exposure: [api]", "exposure: [worker]"},
		"duplicate entry":       {"entries: [SETUP_INPUT]", "entries: [SETUP_INPUT, SETUP_INPUT]"},
		"reserved target":       {"target: /data", "target: /usr/tools"},
		"file overlap":          {"kind: env", "kind: file\n    path: data/input\n    uid: 0\n    gid: 0"},
		"null grants":           {"entries: [SETUP_INPUT]", "entries: null"},
		"unknown grant field":   {"read_only: false", "read_only: false\n          source: /host"},
	}
	for name, replacement := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseScriptExecutionDocument(strings.Replace(scriptExecutionDocument, replacement[0], replacement[1], 1)); err == nil {
				t.Fatal("accepted invalid explicit execution")
			}
		})
	}
}

func parseScriptExecutionDocument(content string) (Result, error) {
	return Parse(context.Background(), parserEnvironmentScope, parserBundle(
		[]string{"compose.yaml"}, map[string]string{"compose.yaml": environmentRoot(content)},
	))
}

// Rationale: omission stays inherited; an explicit inherited choice is not an
// empty explicit context and may not carry even empty extra fields.
func TestParseScriptExecutionInheritedChoiceIsClosed(t *testing.T) {
	start := strings.Index(scriptExecutionDocument, "    execution:")
	end := strings.Index(scriptExecutionDocument, "x-gp-entry:")
	for _, execution := range []string{"", "    execution: {mode: inherited}\n"} {
		result, err := parseScriptExecutionDocument(
			scriptExecutionDocument[:start] + execution + scriptExecutionDocument[end:],
		)
		if err != nil || result.Extensions.Scripts["prepare"].When != core.ScriptPreDeploy {
			t.Fatalf("inherited Script: %v", err)
		}
	}
	for _, execution := range []string{"null", "{mode: inherited, image: ''}", "{mode: inherited, volumes: []}"} {
		if _, err := parseScriptExecutionDocument(scriptExecutionDocument[:start] + "    execution: " + execution + "\n" + scriptExecutionDocument[end:]); err == nil {
			t.Fatalf("accepted %s", execution)
		}
	}
}

// Rationale: aliases and merge keys are syntax, not permission to bypass typed
// decisions. The same closed context remains valid when authored through them.
func TestParseScriptExecutionAliasesPreserveDecisions(t *testing.T) {
	document := strings.Replace(scriptExecutionDocument, "    execution:\n", "    execution: &setup-context\n", 1)
	document = strings.Replace(document, "x-gp-entry:", `  again:
    slug: again
    service: api
    when: pre-deploy
    script: echo again
    execution:
      <<: *setup-context
      user: "1:1"
x-gp-entry:`, 1)
	if _, err := parseScriptExecutionDocument(document); err != nil {
		t.Fatalf("merged context: %v", err)
	}
	invalid := strings.Replace(document, "      user: \"1:1\"", "      user: null", 1)
	if _, err := parseScriptExecutionDocument(invalid); err == nil {
		t.Fatal("accepted merged null user")
	}
	// A merged Script field must be checked before YAML drops null into nil.
	nullContext := strings.Replace(
		scriptExecutionDocument,
		"    execution:\n",
		"    <<: {execution: null}\n    execution_unused:\n",
		1,
	)
	start, end := strings.Index(nullContext, "    execution_unused:"), strings.Index(nullContext, "x-gp-entry:")
	nullContext = nullContext[:start] + nullContext[end:]
	if _, err := parseScriptExecutionDocument(nullContext); err == nil {
		t.Fatal("accepted merged null execution")
	}
}
