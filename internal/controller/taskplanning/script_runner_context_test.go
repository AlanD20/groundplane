package taskplanning

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// Rationale: explicit setup grants are independent of consumer runtime options,
// including options the inherited Script runner deliberately does not support.
func TestScriptRunnerContextExplicitProjectsOnlyPreparedGrants(t *testing.T) {
	for _, staged := range []bool{false, true} {
		sources := scriptPreparationSources()
		sources.Script.Record.Desired.Execution.User = "1001:1002"
		service, err := NewScriptRunnerPreparationService(
			&ScriptArtifactService{values: &explicitScriptEntryValues{}},
			&scriptPreparationAgent{},
			&scriptPreparationImages{},
		)
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := service.Prepare(context.Background(), sources)
		if err != nil {
			t.Fatal(err)
		}
		input := ManualScriptPlanInput{
			Sources:     sources,
			Preparation: prepared,
			SnapshotID:  "snapshot",
			ExecutionID: "execution",
		}
		if staged {
			input.Candidate = &BlueprintScriptCandidateSources{
				EnvironmentID: sources.Environment.Record.ID, RevisionID: "candidate", RenderGeneration: 1, FixedReadRevision: sources.Revision,
				ProjectionSHA256: [32]byte{1},
			}
		}
		ambient := "never inherit"
		consumer := composetypes.ServiceConfig{
			Image: "sha256:" + strings.Repeat("a", 64), User: "named-consumer", WorkingDir: "/consumer",
			Environment: composetypes.MappingWithEquals{"AMBIENT": &ambient}, Privileged: true,
			Networks: map[string]*composetypes.ServiceNetworkConfig{"ambient": {}}, DNS: []string{"192.0.2.1"},
			Volumes: []composetypes.ServiceVolumeConfig{{Type: "bind", Source: "/host", Target: "/ambient"}},
			CapAdd:  []string{"ALL"}, Runtime: "consumer-runtime", Tmpfs: []string{"/ambient"},
		}
		projection, err := projectScriptRunnerContext(input, consumer, consumer)
		if err != nil {
			t.Fatal(err)
		}
		want := &agentpb.ScriptRunnerProjection{
			SnapshotId: "snapshot", Name: "gp-script-execution", Image: prepared.image.LocalImageID,
			Uid: 1001, Gid: 1002, WorkingDir: "/", Mounts: projection.Mounts,
			EntryBindings: prepared.bindings, Labels: projection.Labels,
			Entrypoint: []string{"/bin/sh"}, Command: []string{"/groundplane-script-body"}, StopGraceSeconds: 10,
		}
		if !proto.Equal(want, projection) {
			t.Fatalf("ambient consumer options entered explicit runner: %v", projection)
		}
		if len(projection.Mounts) != 1 {
			t.Fatal("exact Volume grant was omitted")
		}
		mount := projection.Mounts[0]
		grant := sources.Script.Record.Desired.Execution.Volumes[0]
		if mount.SourceId != grant.VolumeID || !proto.Equal(mount.RenderedMount, &agentpb.ScriptMount{
			Type: "volume", Source: "gp_vol_" + strings.ToLower(grant.VolumeID), Target: grant.Target, ReadOnly: grant.ReadOnly, VolumeNoCopy: true,
		}) {
			t.Fatal("Volume grant gained undeclared mount options")
		}
		if staged {
			if !proto.Equal(
				mount.Source,
				blueprintScriptStagedSourceAuthority(input.Candidate, input.Candidate.ProjectionSHA256),
			) {
				t.Fatal("candidate Volume authority changed")
			}
		} else if !proto.Equal(mount.Source, existingScriptSourceAuthority(sources.DesiredProjection.Revision)) {
			t.Fatal("existing Volume authority changed")
		}
	}
}
