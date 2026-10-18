package executionplan

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// TASK-70: a redeploy must restore exact historical configuration and both
// captured slots, never render the predecessor from the candidate's new intent.
func TestNativeReleasePreservesExactHistoricalConfigurationAndInactiveSlot(t *testing.T) {
	for _, strategy := range []agentpb.WorkReleaseStrategy{
		agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE,
		agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_BLUE_GREEN,
	} {
		t.Run(strategy.String(), func(t *testing.T) {
			attempt, input := nativeTopologyRelease(t)
			retained := candidateRuntimeFixtureArtifact(t, ids.New(ids.KindConfig), candidateRuntimePriorPlanID,
				5, ids.New(ids.KindDeployment), "blue", nil, false)
			input.Predecessor.RetainedArtifact = nativeWitnessBytes(t, retained)
			input.Strategy = strategy
			if strategy == agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE {
				for _, workload := range input.Candidate.Services {
					if workload.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
						continue
					}
					workload.Role, workload.Slot = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON, ""
					workload.ExpectedLabels = candidateRuntimeLabels(
						candidateRuntimePlanID,
						7,
						candidateRuntimeServiceID,
						"singleton",
						candidateRuntimeReleaseID,
						"",
					)
				}
			}
			original, err := OpenReleaseWorkArtifacts(attempt, input)
			if err != nil || original.Prior == nil || original.Prior.Retained == nil {
				t.Fatalf("captured current/inactive pair rejected: %v", err)
			}
			currentBytes := bytes.Clone(input.Predecessor.CurrentArtifact)
			retainedBytes := bytes.Clone(input.Predecessor.RetainedArtifact)
			candidate := input.Candidate
			candidate.CanonicalYaml = bytes.Replace(candidate.CanonicalYaml, []byte("    content: selected"),
				[]byte("    content: candidate-only"), 1)
			candidate.CanonicalYaml = bytes.Replace(candidate.CanonicalYaml, []byte("  api--blue:\n"),
				[]byte("  api--blue:\n    command: [changed-candidate]\n    environment:\n      CONFIG: changed\n"), 1)
			digest := sha256.Sum256(candidate.CanonicalYaml)
			candidate.YamlSha256 = digest[:]
			for range 2 {
				opened, err := OpenReleaseWorkArtifacts(attempt, input)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Contains(opened.Candidate.CanonicalYaml, []byte("candidate-only")) ||
					!bytes.Contains(opened.Candidate.CanonicalYaml, []byte("changed-candidate")) {
					t.Fatal("candidate did not retain its changed configuration")
				}
				for _, pair := range []struct {
					actual *agentpb.ComposeArtifact
					before []byte
				}{{opened.Prior.Artifact, currentBytes}, {opened.Prior.Retained, retainedBytes}} {
					if !bytes.Equal(nativeWitnessBytes(t, pair.actual), pair.before) ||
						bytes.Contains(pair.actual.CanonicalYaml, []byte("candidate-only")) {
						t.Fatal(
							"historical runtime changed with candidate intent or lost its captured artifact identity",
						)
					}
				}
				if !proto.Equal(opened.Prior.Proxy, original.Prior.Proxy) {
					t.Fatal("candidate rendering replaced historical proxy configuration")
				}
				// A decoded consumer owns its bytes; changing it cannot corrupt
				// the retained input used by a later recovery delivery.
				opened.Prior.Artifact.CanonicalYaml[0] ^= 1
				opened.Prior.Retained.CanonicalYaml[0] ^= 1
			}
			for _, test := range []struct {
				name   string
				change func(*agentpb.ReleaseWorkInput)
			}{
				{"different predecessor", func(in *agentpb.ReleaseWorkInput) { in.Predecessor.ReleaseId = ids.New(ids.KindDeployment) }},
				{"different slot", func(in *agentpb.ReleaseWorkInput) { in.Predecessor.Target = "blue" }},
				{"candidate as predecessor", func(in *agentpb.ReleaseWorkInput) { in.Predecessor.ReleaseId = in.ReleaseId }},
				{"history as candidate", func(in *agentpb.ReleaseWorkInput) { in.Candidate = proto.CloneOf(original.Prior.Artifact) }},
			} {
				t.Run(test.name, func(t *testing.T) {
					changed := proto.CloneOf(input)
					test.change(changed)
					if _, err := OpenReleaseWorkArtifacts(attempt, changed); err == nil {
						t.Fatal("changed predecessor/forward authority was accepted")
					}
				})
			}
		})
	}
}
