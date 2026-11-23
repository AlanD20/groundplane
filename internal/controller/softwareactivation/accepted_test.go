package softwareactivation

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/AlanD20/groundplane/internal/infra/consolerelease"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestControllerActivationRequiresMatchingConsoleAPIProof(t *testing.T) {
	api := []byte(`{"openapi":"3.1.0"}`)
	store, err := consolerelease.New(filepath.Join(t.TempDir(), "console"), fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("current")},
	}, "console/v0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BindAPIDocument(api); err != nil {
		t.Fatal(err)
	}
	service := &Service{console: store}
	current := consolerelease.APIHash(api)
	candidate := consolerelease.APIHash([]byte(`{"openapi":"3.1.0","paths":{}}`))
	for _, test := range []struct {
		name       string
		selection  preparation.Selection
		controller string
		console    string
		conflict   bool
	}{
		{"Controller missing API proof", preparation.SelectionController, "", "", true},
		{"Both missing API proof", preparation.SelectionBoth, "", "", true},
		{"All missing API proof", preparation.SelectionAll, "", candidate, true},
		{"Controller keeps current API", preparation.SelectionController, current, "", false},
		{"Both changes current API", preparation.SelectionBoth, candidate, "", true},
		{"All with mismatched Console", preparation.SelectionAll, candidate, current, true},
		{"All with matching Console", preparation.SelectionAll, candidate, candidate, false},
		{"Agent keeps current API", preparation.SelectionAgent, "", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := preparation.Result{}
			if test.selection.IncludesController() {
				result.Controller = &preparation.ControllerResult{APISHA256: test.controller}
			}
			if test.console != "" {
				result.Console = &preparation.ConsoleResult{
					Artifact: consoleTestArtifact("console/v0.0.2", "a", test.console),
				}
			}
			prepared := preparationrecord.Record{
				Source:   preparation.ResolvedSource{Selection: test.selection},
				Progress: preparation.Progress{Result: result},
			}
			err := service.validateConsoleCompatibility(prepared)
			if test.conflict {
				if kind, _ := errs.KindOf(err); kind != errs.KindStateConflict {
					t.Fatalf("expected compatibility conflict, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("compatible selection was refused: %v", err)
			}
		})
	}
}
