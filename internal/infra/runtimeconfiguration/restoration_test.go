package runtimeconfiguration

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
)

// SVC-15: restoring a failed write uses the earlier acknowledged source and
// metadata, never the failed file's value or any unrelated acknowledged file.
func TestRestorationSelectsExactPriorFilesAndInitialAbsence(t *testing.T) {
	t.Parallel()
	previous := fixtureSnapshot(1, fixtureRecords())
	changed := taskmaterialization.Clone(previous.Files[:1])
	changed[0].StepID, changed[0].UID = ids.New(ids.KindStep), 1234
	changed[0].Source.EntryValue.ValueGenerationID = ids.New(ids.KindConfig)
	added := removalRecord("config/new-file")
	added.StepID = ids.New(ids.KindStep)
	changes := append(changed, added)
	selected, err := SelectRestorations(previous.EnvironmentID, 2, &previous, changes)
	if err != nil || len(selected) != 2 {
		t.Fatalf("selected files = %d/%v", len(selected), err)
	}
	for _, file := range selected {
		if file.ForwardStepID == changed[0].StepID {
			left, right := file.Prior, previous.Files[0]
			if left.StepID != right.StepID || left.MaterializationID != right.MaterializationID ||
				left.EnvironmentID != right.EnvironmentID || left.Destination != right.Destination ||
				left.ServiceID != right.ServiceID || left.ServiceName != right.ServiceName || left.OutputKind != right.OutputKind ||
				left.UID != right.UID || left.GID != right.GID || left.Mode != right.Mode || left.Length != right.Length ||
				left.SHA256 != right.SHA256 || left.Source.Kind != right.Source.Kind ||
				*left.Source.EntryValue != *right.Source.EntryValue {
				t.Fatal("prior source or metadata changed")
			}
			file.Prior.Source.EntryValue.ValueGenerationID = "caller-mutation"
			if previous.Files[0].Source.EntryValue.ValueGenerationID == "caller-mutation" {
				t.Fatal("source alias escaped")
			}
		} else if file.Prior.Source.Kind != taskmaterialization.SourceRemoval || file.Prior.Length != 0 || file.Prior.SHA256 != digest(nil) {
			t.Fatal("new destination did not retain exact absence")
		}
	}
	initial, err := SelectRestorations(previous.EnvironmentID, 2, nil, changed)
	if err != nil || len(initial) != 1 || initial[0].Prior.OutputKind != taskmaterialization.OutputRemoveSecretFile {
		t.Fatalf("initial absence = %#v/%v", initial, err)
	}
	if _, err := SelectRestorations(previous.EnvironmentID, 2, &previous, append(changes, changed[0])); err == nil {
		t.Fatal("duplicate destination accepted")
	}
	if _, err := SelectRestorations(ids.New(ids.KindEnvironment), 2, &previous, changes); err == nil {
		t.Fatal("foreign acknowledged configuration accepted")
	}
}
