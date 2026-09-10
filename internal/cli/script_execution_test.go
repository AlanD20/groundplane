package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/cli/apiclient"
	clicommon "github.com/AlanD20/groundplane/internal/cli/common"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: normal Script commands resolve mutable Volume labels and immutable
// Entry keys through paginated metadata reads, then submit only stable ids.
func TestScriptExecutionCLIResolvesScopedNames(t *testing.T) {
	const environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const serviceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const volumeID = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const entryID = "ev_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	var mutated bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/v1/tenants":
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, tenantPageJSON())
		case "/api/v1/projects":
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, projectPageJSON())
		case "/api/v1/environments":
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, environmentPageJSON())
		case "/api/v1/services":
			writeBackupPolicyTestResponse(
				t,
				writer,
				http.StatusOK,
				`{"items":[{"id":"`+serviceID+`","environment_id":"`+environmentID+`","name":"consumer"}]}`,
			)
		case "/api/v1/volumes":
			if request.URL.Query().Get("environment") != environmentID {
				t.Error("unscoped Volume lookup")
			}
			writeScriptCLIJSON(t, writer, apiTypes.Page[apiTypes.Volume]{Items: []apiTypes.Volume{
				{ID: volumeID, EnvironmentID: environmentID, Slug: "tls-data", Key: "different-compose-key"}}})
		case "/api/v1/entries":
			if request.URL.Query().Get("environment") != environmentID {
				t.Error("unscoped Entry lookup")
			}
			if request.URL.Query().Get("cursor") == "" {
				writeScriptCLIJSON(
					t,
					writer,
					apiTypes.Page[apiTypes.Entry]{Items: []apiTypes.Entry{}, NextCursor: "next"},
				)
			} else {
				writeScriptCLIJSON(t, writer, apiTypes.Page[apiTypes.Entry]{Items: []apiTypes.Entry{
					{ID: entryID, ReconciliationKey: "tls-seed", Type: "file", Path: "/seed.pem", Source: apiTypes.EntrySource{Kind: "literal"}, Exposure: []string{"consumer"}}}})
			}
		case "/api/v1/scripts":
			var input apiTypes.ScriptCreate
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Error(err)
				return
			}
			if input.Execution == nil || len(input.Execution.Volumes) != 1 ||
				input.Execution.Volumes[0].VolumeID != volumeID ||
				input.Execution.Volumes[0].ReadOnly ||
				len(input.Execution.EntryIDs) != 1 ||
				input.Execution.EntryIDs[0] != entryID {
				t.Errorf("Script grants not resolved: %#v", input.Execution)
				return
			}
			mutated = true
			writer.WriteHeader(http.StatusCreated)
			writeScriptCLIJSON(
				t,
				writer,
				apiTypes.Script{
					ID:               "scr_01ARZ3NDEKTSV4RRFFQ69G5FAV",
					Origin:           "api",
					ActiveGeneration: 1,
					Execution:        *input.Execution,
				},
			)
		default:
			t.Errorf("unexpected request %s %s", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	command := newScriptCmd()
	command.SetIn(strings.NewReader("mode: explicit\nimage: setup@sha256:" + strings.Repeat("a", 64) +
		"\nuser: '0:0'\nvolumes: [{volume: tls-data, target: /etc/tls, read_only: false}]\nentries: [tls-seed]\n"))
	executeNoun(
		t,
		command,
		server.URL,
		Scope{Tenant: "acme", Project: "storefront", Environment: "production"},
		"add",
		"prepare",
		"--service",
		"consumer",
		"--script",
		"echo prepare",
		"--execution-file",
		"-",
	)
	if !mutated {
		t.Fatal("Script mutation was not submitted")
	}
}

// Rationale: edit supports whole-context replacement/reset without changing the
// body, and conflicting context options fail before any HTTP call.
func TestScriptExecutionCLIResetAndConflictingFlags(t *testing.T) {
	const scriptID = "scr_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	server := exactRequestServer(t, http.MethodPatch, "/api/v1/scripts/"+scriptID,
		`{"execution":{"mode":"inherited"}}`, http.StatusOK,
		`{"id":"`+scriptID+`","origin":"api","active_generation":1,"order":0,"execution":{"mode":"inherited"}}`)
	defer server.Close()
	executeNoun(t, newScriptCmd(), server.URL, Scope{AsID: true}, "edit", scriptID, "--inherit-execution")
	var output bytes.Buffer
	command := newScriptCmd()
	command.SetContext(context.WithValue(context.Background(), appKey{}, &App{Client: apiclient.New("http://invalid"),
		Out: clicommon.NewWriter(clicommon.FormatJSON, true, &output), Scope: Scope{AsID: true}}))
	command.SetArgs([]string{"edit", scriptID, "--execution-file", "-", "--inherit-execution"})
	if err := command.Execute(); err == nil {
		t.Fatal("conflicting execution options accepted")
	}
}

// Rationale: ID mode can grant API-owned Entries without inventing a Blueprint
// key or reading unrelated resource metadata; context-only edit stays bodyless.
func TestScriptExecutionCLIIDsSkipResourceLookups(t *testing.T) {
	const scriptID = "scr_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const volumeID = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const entryID = "ev_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	image := "setup@sha256:" + strings.Repeat("a", 64)
	execution := `{"entry_ids":["` + entryID + `"],"image":"` + image +
		`","mode":"explicit","user":"0:0","volumes":[{"read_only":false,"target":"/etc/tls","volume_id":"` + volumeID + `"}]}`
	server := exactRequestServer(t, http.MethodPatch, "/api/v1/scripts/"+scriptID,
		`{"execution":`+execution+`}`, http.StatusOK,
		`{"id":"`+scriptID+`","origin":"api","active_generation":1,"order":0,"execution":`+execution+`}`)
	defer server.Close()
	command := newScriptCmd()
	command.SetIn(strings.NewReader("mode: explicit\nimage: " + image +
		"\nuser: '0:0'\nvolumes: [{volume: " + volumeID +
		", target: /etc/tls, read_only: false}]\nentries: [" + entryID + "]\n"))
	executeNoun(t, command, server.URL, Scope{AsID: true}, "edit", scriptID, "--execution-file", "-")
}

// Rationale: absent/ambiguous authored Entry keys must never fall back to an env
// variable name, file path, first match or mutation with an unresolved label.
func TestScriptExecutionCLIRejectsUnresolvedEntryKeys(t *testing.T) {
	for _, count := range []int{0, 2} {
		t.Run(map[int]string{0: "missing", 2: "ambiguous"}[count], func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Method != http.MethodGet {
					t.Errorf("unresolved key reached mutation: %s", request.Method)
					writer.WriteHeader(http.StatusBadRequest)
					return
				}
				switch request.URL.Path {
				case "/api/v1/tenants":
					writeBackupPolicyTestResponse(t, writer, http.StatusOK, tenantPageJSON())
				case "/api/v1/projects":
					writeBackupPolicyTestResponse(t, writer, http.StatusOK, projectPageJSON())
				case "/api/v1/environments":
					writeBackupPolicyTestResponse(t, writer, http.StatusOK, environmentPageJSON())
				case "/api/v1/entries":
					entries := make([]apiTypes.Entry, count)
					for index := range entries {
						entries[index] = apiTypes.Entry{ID: "ev_01ARZ3NDEKTSV4RRFFQ69G5FAV", ReconciliationKey: "seed",
							Type: "file", Path: "/seed", Source: apiTypes.EntrySource{Kind: "literal"}, Exposure: []string{"consumer"}}
					}
					writeScriptCLIJSON(t, writer, apiTypes.Page[apiTypes.Entry]{Items: entries})
				default:
					t.Errorf("unexpected lookup: %s", request.URL.Path)
					writer.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			var output bytes.Buffer
			command := newScriptCmd()
			command.SetContext(context.WithValue(context.Background(), appKey{}, &App{
				Client: apiclient.New(server.URL), Out: clicommon.NewWriter(clicommon.FormatJSON, true, &output),
				Scope: Scope{Tenant: "acme", Project: "storefront", Environment: "production"},
			}))
			command.SetIn(strings.NewReader("mode: explicit\nimage: setup@sha256:" + strings.Repeat("a", 64) +
				"\nuser: '0:0'\nentries: [seed]\n"))
			command.SetArgs([]string{"edit", "prepare", "--execution-file", "-"})
			err := command.Execute()
			if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
				t.Fatalf("unresolved Entry key accepted or wrong error: %v", err)
			}
		})
	}
}

func writeScriptCLIJSON[T apiTypes.Page[apiTypes.Volume] | apiTypes.Page[apiTypes.Entry] | apiTypes.Script](
	t *testing.T,
	writer http.ResponseWriter,
	value T,
) {
	t.Helper()
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		t.Error(err)
	}
}
