package controllerrelease

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a staging pointer selects only immutable verified bytes; neither
// missing staging nor unsafe pointers may be reported as an available release.
func TestCandidateSelectionVerifiesPointerAndRelease(t *testing.T) {
	for _, scenario := range []string{"absent", "valid", "writable", "symlink", "extra", "missing-release"} {
		t.Run(scenario, func(t *testing.T) {
			store, release, _ := testReleaseStore(t)
			defer store.Close()
			pointer := filepath.Join(store.root.Name(), "candidate.json")
			if scenario != "absent" {
				if scenario == "missing-release" {
					release = upgrade.Hash([]byte("missing"))
				}
				raw := `{"release":"` + string(release) + `"}`
				if scenario == "extra" {
					raw = `{"path":"/other","release":"` + string(release) + `"}`
				}
				if err := os.WriteFile(pointer, []byte(raw), 0o400); err != nil {
					t.Fatal(err)
				}
				if scenario == "writable" {
					if err := os.Chmod(pointer, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "symlink" {
					if err := os.Rename(pointer, pointer+".original"); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink("candidate.json.original", pointer); err != nil {
						t.Fatal(err)
					}
				}
			}
			got, found, err := store.Candidate(context.Background())
			switch scenario {
			case "absent":
				if err != nil || found {
					t.Fatalf("absent candidate = %#v, %t, %v", got, found, err)
				}
			case "valid":
				if err != nil || !found || got.Release != release {
					t.Fatalf("candidate = %#v, %t, %v", got, found, err)
				}
			default:
				if err == nil || found {
					t.Fatalf("unsafe candidate accepted: %#v, %t, %v", got, found, err)
				}
			}
		})
	}
}

// Rationale: replacing a healthy journal must first retain its selected Agent
// image. A failed second update must not revert desired image to bootstrap.
func TestSelectedReleaseSurvivesLaterUpdateRecovery(t *testing.T) {
	store, journal, _, _ := activationStore(t)
	defer store.Close()
	ctx := context.Background()
	if err := store.Prepare(ctx, journal); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Advance(ctx, journal.TaskID, upgrade.PhasePrepared, upgrade.PhaseActivating); err != nil {
		t.Fatal(err)
	}
	if err := store.Activate(ctx, journal.TaskID); err != nil {
		t.Fatal(err)
	}
	if err := store.Guard(ctx, journal.StartedAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Advance(ctx, journal.TaskID, upgrade.PhaseStarting, upgrade.PhaseHealthy); err != nil {
		t.Fatal(err)
	}
	selected, found, err := store.Selected(ctx)
	if err != nil || !found || selected.Release != journal.Release {
		t.Fatalf("healthy selection = %#v, %t, %v", selected, found, err)
	}

	next := journal
	next.TaskID, next.Phase, next.TrialBootID = ids.New(ids.KindTask), upgrade.PhasePrepared, ""
	next.PreviousController = journal.Manifest.ControllerSHA256
	binary := []byte("second candidate bytes")
	next.Manifest.ControllerSHA256 = upgrade.Hash(binary)
	next.Manifest.ControllerVersion = "0.2.0"
	raw, err := json.Marshal(next.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		t.Fatal(err)
	}
	next.Release = upgrade.Hash(raw)
	newLeaf := filepath.Join(store.root.Name(), "releases", string(next.Release)[7:])
	if err := os.Mkdir(newLeaf, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(newLeaf, "controller"), binary, 0o500); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(newLeaf, "manifest.json"), raw, 0o400); err != nil {
		t.Fatal(err)
	}
	if err := store.Prepare(ctx, next); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Selected(ctx); !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("active selection = %v", err)
	}
	if err := store.Rollback(ctx, next.TaskID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Advance(ctx, next.TaskID, upgrade.PhaseRolledBack, upgrade.PhaseRecovered); err != nil {
		t.Fatal(err)
	}
	selected, found, err = store.Selected(ctx)
	if err != nil || !found || selected.Release != journal.Release || selected.Manifest != journal.Manifest {
		t.Fatalf("retained selection = %#v, %t, %v", selected, found, err)
	}
}
