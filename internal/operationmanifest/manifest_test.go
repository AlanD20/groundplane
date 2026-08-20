package operationmanifest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a synthetic complete operation and a local-tool exemption prove
// the framework can express both accepted ADR 0006 branches without claiming
// that the product manifest itself is settled.
func TestValidateAcceptsCompleteParityAndExemptionEntries(t *testing.T) {
	t.Parallel()

	manifest, capture := validFixture()
	if err := Validate(manifest, capture); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

// Rationale: each manifest entry and each surface identity must have one
// owner, otherwise a superficially complete manifest could hide ambiguity.
func TestValidateRejectsDuplicateManifestIdentities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Manifest)
		want   string
	}{
		{
			name: "entry id",
			mutate: func(manifest *Manifest) {
				manifest.Entries = append(manifest.Entries, manifest.Entries[0])
			},
			want: `entry id "example.show" is declared 2 times`,
		},
		{
			name: "console action",
			mutate: func(manifest *Manifest) {
				manifest.Entries = append(manifest.Entries, Entry{
					ID: "example.second",
					Operation: &Operation{
						Console: manifest.Entries[0].Operation.Console,
						CLI:     CLILeaf{Path: []string{"example", "second"}},
						API:     validAPI("example-second", "/examples/{id}/second"),
					},
				})
			},
			want: `console action "example.show" is mapped by 2 entries`,
		},
		{
			name: "cli leaf",
			mutate: func(manifest *Manifest) {
				manifest.Entries = append(manifest.Entries, Entry{
					ID: "example.second",
					Operation: &Operation{
						Console: ConsoleAction{ID: "example.second"},
						CLI:     manifest.Entries[0].Operation.CLI,
						API:     validAPI("example-second", "/examples/{id}/second"),
					},
				})
			},
			want: `cli leaf "example show" is mapped by 2 entries`,
		},
		{
			name: "api endpoint",
			mutate: func(manifest *Manifest) {
				manifest.Entries = append(manifest.Entries, Entry{
					ID: "example.second",
					Operation: &Operation{
						Console: ConsoleAction{ID: "example.second"},
						CLI:     CLILeaf{Path: []string{"example", "second"}},
						API:     manifest.Entries[0].Operation.API,
					},
				})
			},
			want: `api endpoint "GET /examples/{id}" is mapped by 2 entries`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			manifest, capture := validFixture()
			test.mutate(&manifest)
			assertViolation(t, Validate(manifest, capture), test.want)
		})
	}
}

// Rationale: OpenAPI generators address operations by operationId, so two
// distinct endpoints sharing one id collide even when method/path pairs differ.
func TestValidateRejectsDuplicateAPIOperationIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Manifest, *Capture)
		want   string
	}{
		{
			name: "manifest",
			mutate: func(manifest *Manifest, _ *Capture) {
				manifest.Entries = append(manifest.Entries, Entry{
					ID: "example.second",
					Operation: &Operation{
						Console: ConsoleAction{ID: "example.second"},
						CLI:     CLILeaf{Path: []string{"example", "second"}},
						API:     validAPI("example-show", "/examples/{id}/second"),
					},
				})
			},
			want: `api operation id "example-show" is mapped by 2 entries`,
		},
		{
			name: "capture",
			mutate: func(_ *Manifest, capture *Capture) {
				capture.RecordAPI(validAPI("example-show", "/examples/{id}/second"))
			},
			want: `captured api operation id "example-show" occurs 2 times`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			manifest, capture := validFixture()
			test.mutate(&manifest, &capture)
			assertViolation(t, Validate(manifest, capture), test.want)
		})
	}
}

// Rationale: an implemented surface that is absent from the manifest is a
// contract orphan and must fail regardless of which frontend introduced it.
func TestValidateRejectsOrphanedCapturedSurfaces(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Capture)
		want   string
	}{
		{
			name: "console",
			mutate: func(capture *Capture) {
				capture.RecordConsole("orphan.action")
			},
			want: `captured console action "orphan.action" is not in the manifest`,
		},
		{
			name: "cli",
			mutate: func(capture *Capture) {
				capture.RecordCLI("orphan", "show")
			},
			want: `captured cli leaf "orphan show" is not in the manifest`,
		},
		{
			name: "api",
			mutate: func(capture *Capture) {
				capture.RecordAPI(validAPI("orphan-show", "/orphans/{id}"))
			},
			want: `captured api endpoint "GET /orphans/{id}" is not in the manifest`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			manifest, capture := validFixture()
			test.mutate(&capture)
			assertViolation(t, Validate(manifest, capture), test.want)
		})
	}
}

// Rationale: collectors preserve duplicates so the validator can reject two
// registrations instead of silently deduplicating evidence.
func TestValidateRejectsCapturedSurfaceMultiplicity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Capture, Operation)
		want   string
	}{
		{
			name: "console",
			mutate: func(capture *Capture, operation Operation) {
				capture.RecordConsole(operation.Console.ID)
			},
			want: `captured console action "example.show" occurs 2 times`,
		},
		{
			name: "cli",
			mutate: func(capture *Capture, operation Operation) {
				capture.RecordCLI(operation.CLI.Path...)
			},
			want: `captured cli leaf "example show" occurs 2 times`,
		},
		{
			name: "api",
			mutate: func(capture *Capture, operation Operation) {
				capture.RecordAPI(operation.API)
			},
			want: `captured api endpoint "GET /examples/{id}" occurs 2 times`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			manifest, capture := validFixture()
			test.mutate(&capture, *manifest.Entries[0].Operation)
			assertViolation(t, Validate(manifest, capture), test.want)
		})
	}
}

// Rationale: request, response, success, operation id, and stable errors are
// executable contract fields rather than documentary metadata.
func TestValidateRejectsDeepAPIContractDrift(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*APIContract)
		want   string
	}{
		{
			name: "operation id",
			mutate: func(api *APIContract) {
				api.OperationID = "different-show"
			},
			want: "operation id",
		},
		{
			name: "request",
			mutate: func(api *APIContract) {
				api.Request = Payload{Kind: PayloadJSON, Schema: "UnexpectedRequest"}
			},
			want: "request payload",
		},
		{
			name: "response",
			mutate: func(api *APIContract) {
				api.Response = Payload{Kind: PayloadJSON, Schema: "UnexpectedResponse"}
			},
			want: "response payload",
		},
		{
			name: "success status",
			mutate: func(api *APIContract) {
				api.SuccessStatus = 201
			},
			want: "success status",
		},
		{
			name: "stable errors",
			mutate: func(api *APIContract) {
				api.Errors = []ErrorContract{{Code: errs.CodeInternal, Status: 500}}
			},
			want: "stable errors",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			manifest, capture := validFixture()
			test.mutate(&capture.API[0])
			assertViolation(t, Validate(manifest, capture), test.want)
		})
	}
}

// Rationale: the MVP human API has a closed method set; accepting an arbitrary
// uppercase token would let an accidental or non-contract route enter generation.
func TestValidateRejectsUnsupportedHTTPMethod(t *testing.T) {
	t.Parallel()

	manifest, capture := validFixture()
	manifest.Entries[0].Operation.API.Method = "FOO"
	capture.API[0].Method = "FOO"
	assertViolation(
		t,
		Validate(manifest, capture),
		`method "FOO" is not in the human API method set`,
	)
}

// Rationale: one canonical route-template grammar prevents equivalent or
// ambiguous paths from acquiring separate operation identities.
func TestValidateRejectsNonCanonicalRouteTemplates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "root", path: "/"},
		{name: "relative", path: "examples"},
		{name: "trailing separator", path: "/examples/"},
		{name: "repeated separator", path: "/examples//show"},
		{name: "current dot segment", path: "/examples/./show"},
		{name: "parent dot segment", path: "/examples/../show"},
		{name: "backslash", path: `/examples\show`},
		{name: "percent encoding", path: "/examples/%7Bid%7D"},
		{name: "query", path: "/examples?limit=1"},
		{name: "fragment", path: "/examples#detail"},
		{name: "control", path: "/examples/\x00"},
		{name: "uppercase literal", path: "/Examples"},
		{name: "snake literal", path: "/release_groups"},
		{name: "partial binding", path: "/examples/prefix-{id}"},
		{name: "uppercase binding", path: "/examples/{ID}"},
		{name: "kebab binding", path: "/examples/{project-id}"},
		{name: "empty binding", path: "/examples/{}"},
		{name: "unclosed binding", path: "/examples/{id"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			manifest, capture := validFixture()
			manifest.Entries[0].Operation.API.Path = test.path
			capture.API[0].Path = test.path
			assertViolation(
				t,
				Validate(manifest, capture),
				`path `+fmt.Sprintf("%q", test.path)+" is not a canonical human API route template",
			)
		})
	}
}

// Rationale: the validator must retain every route shape required by the
// existing flat-resource and action/singleton contracts while rejecting aliases.
func TestValidateAcceptsCanonicalRouteTemplates(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"/examples",
		"/release-groups/{id}",
		"/backing-services/{project_id}/start",
		"/tasks/{task_id}/events",
	} {
		path := path
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			manifest, capture := validFixture()
			manifest.Entries[0].Operation.API.Path = path
			capture.API[0].Path = path
			if err := Validate(manifest, capture); err != nil {
				t.Fatalf("Validate() error = %v, want nil", err)
			}
		})
	}
}

// Rationale: only catalogued code/status pairs are stable public errors; an
// invented code or a known code with the wrong status must not enter a manifest.
func TestValidateRejectsInvalidStableErrorContracts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		errors []ErrorContract
		want   string
	}{
		{
			name:   "missing",
			errors: nil,
			want:   "must declare at least one stable error",
		},
		{
			name:   "unknown code",
			errors: []ErrorContract{{Code: errs.Code("invented.failure"), Status: 400}},
			want:   `error "invented.failure"/400 is not in the stable error catalog`,
		},
		{
			name:   "wrong status",
			errors: []ErrorContract{{Code: errs.CodeServiceNotFound, Status: 409}},
			want:   `error "service.not_found"/409 is not in the stable error catalog`,
		},
		{
			name: "duplicate tuple",
			errors: []ErrorContract{
				{Code: errs.CodeServiceNotFound, Status: 404},
				{Code: errs.CodeServiceNotFound, Status: 404},
			},
			want: `error "service.not_found"/404 is declared 2 times`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			manifest, capture := validFixture()
			manifest.Entries[0].Operation.API.Errors = test.errors
			capture.API[0].Errors = test.errors
			assertViolation(t, Validate(manifest, capture), test.want)
		})
	}
}

// Rationale: parity entries and exemptions are a closed union, preventing an
// entry from bypassing the 1:1 rule by being both or neither.
func TestValidateRejectsInvalidEntryAndExemptionShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		entry Entry
		want  string
	}{
		{
			name:  "neither branch",
			entry: Entry{ID: "empty"},
			want:  "must define exactly one of operation or exemption",
		},
		{
			name: "both branches",
			entry: Entry{
				ID:        "both",
				Operation: &Operation{},
				Exemption: &Exemption{},
			},
			want: "must define exactly one of operation or exemption",
		},
		{
			name: "unknown exemption class",
			entry: Entry{
				ID: "unknown",
				Exemption: &Exemption{
					CLI:    CLILeaf{Path: []string{"local", "show"}},
					Class:  ExemptionClass("convenience"),
					Reason: "not an accepted ADR 0006 class",
				},
			},
			want: `exemption class "convenience" is not accepted`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			manifest, capture := validFixture()
			manifest.Entries = append(manifest.Entries, test.entry)
			assertViolation(t, Validate(manifest, capture), test.want)
		})
	}
}

func validFixture() (Manifest, Capture) {
	operation := Operation{
		Console: ConsoleAction{ID: "example.show"},
		CLI:     CLILeaf{Path: []string{"example", "show"}},
		API:     validAPI("example-show", "/examples/{id}"),
	}
	exemption := Exemption{
		CLI:    CLILeaf{Path: []string{"controller", "inspect"}},
		Class:  ExemptionLocalDiagnostic,
		Reason: "reads same-host state without calling the human API",
	}
	manifest := Manifest{Entries: []Entry{
		{ID: "example.show", Operation: &operation},
		{ID: "local.controller.inspect", Exemption: &exemption},
	}}

	var capture Capture
	capture.RecordConsole(operation.Console.ID)
	capture.RecordCLI(operation.CLI.Path...)
	capture.RecordAPI(operation.API)
	capture.RecordCLI(exemption.CLI.Path...)
	return manifest, capture
}

func validAPI(operationID, path string) APIContract {
	return APIContract{
		OperationID:   operationID,
		Method:        "GET",
		Path:          path,
		Request:       Payload{Kind: PayloadNone},
		Response:      Payload{Kind: PayloadJSON, Schema: "Example"},
		SuccessStatus: 200,
		Errors: []ErrorContract{
			{Code: errs.CodeServiceNotFound, Status: 404},
			{Code: errs.CodeInternal, Status: 500},
		},
	}
}

func assertViolation(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("Validate() error = nil, want containing %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("Validate() error = %q, want containing %q", err, want)
	}
}
