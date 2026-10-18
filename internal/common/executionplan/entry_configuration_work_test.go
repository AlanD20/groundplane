package executionplan

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ENTRY-04/TASK-09: native delivery must reconstruct the exact captured files
// and consumer selection, not select a new runtime or accept a changed capture.
func TestNativeEntryConfigurationPinsFilesAndConsumerAuthority(t *testing.T) {
	for _, removal := range []bool{false, true} {
		attempt, release := nativeTopologyRelease(t)
		candidate := proto.CloneOf(release.Candidate)
		candidate.ArtifactId = ids.New(ids.KindConfig)
		input := &agentpb.EntryConfigurationWorkInput{OperationId: ids.New(ids.KindOperation),
			EnvironmentId: release.EnvironmentId, RenderGeneration: 8, Removal: removal,
			Baseline: release.Candidate, Candidate: candidate,
			Configuration: &agentpb.WorkConfigurationReference{ConfigurationId: ids.New(ids.KindConfig),
				EnvironmentId: release.EnvironmentId, Generation: 8, Sha256: make([]byte, 32)},
			Files: []*agentpb.MaterializeFile{{MaterializationId: ids.New(ids.KindConfig),
				ArtifactId: candidate.ArtifactId, EnvironmentId: release.EnvironmentId,
				Destination: "config/settings.ini", Uid: 1000, Gid: 1001, Mode: 0o444,
				OutputKind: agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE,
				Sha256:     make([]byte, 32)}},
		}
		if !removal {
			input.RunningConsumerIds = []string{release.ServiceId}
		}
		_, raw, reference, err := CaptureEntryConfigurationWorkInput(attempt.Input.ID, input)
		clear(raw)
		if err != nil {
			t.Fatal(err)
		}
		attempt.Input = reference
		first, err := OpenEntryConfigurationWorkInput(attempt, input)
		if err != nil {
			t.Fatal(err)
		}
		second, err := OpenEntryConfigurationWorkInput(attempt, input)
		if err != nil || !proto.Equal(first, second) {
			t.Fatal("same native input did not reconstruct the same procedure", err)
		}
		first.Files[0].Destination = "changed"
		if input.Files[0].Destination != "config/settings.ini" {
			t.Fatal("delivery aliased retained input")
		}
		for _, mutate := range []func(*agentpb.EntryConfigurationWorkInput){
			func(changed *agentpb.EntryConfigurationWorkInput) { changed.Baseline = nil },
			func(changed *agentpb.EntryConfigurationWorkInput) { changed.Files[0].Uid++ },
			func(changed *agentpb.EntryConfigurationWorkInput) { changed.Files[0].Destination = "other.ini" },
			func(changed *agentpb.EntryConfigurationWorkInput) {
				changed.RunningConsumerIds = []string{ids.New(ids.KindService)}
			},
			func(changed *agentpb.EntryConfigurationWorkInput) {
				changed.Candidate.Services[1].ImageReference = "sha256:changed"
			},
		} {
			changed := proto.CloneOf(input)
			mutate(changed)
			if opened, err := OpenEntryConfigurationWorkInput(attempt, changed); err == nil || opened != nil {
				t.Fatal("changed file or runtime authority replayed the accepted input")
			}
		}
		if removal {
			changed := proto.CloneOf(input)
			changed.RunningConsumerIds = []string{release.ServiceId}
			if _, _, _, err := CaptureEntryConfigurationWorkInput(attempt.Input.ID, changed); err == nil {
				t.Fatal("Entry removal acquired workload-restart authority")
			}
		} else {
			stopped := proto.CloneOf(input)
			stopped.RunningConsumerIds = nil
			if _, raw, _, err := CaptureEntryConfigurationWorkInput(attempt.Input.ID, stopped); err != nil {
				t.Fatal("files-only operation required a running workload", err)
			} else {
				clear(raw)
			}
		}
	}
}
