//go:build linux

package environmentdirectoryhelper

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type volumePathTestCreator struct{}

func (volumePathTestCreator) Create(ctx context.Context, root, directory string) error {
	return createDirectories(ctx, root, directory, uint32(os.Getuid()))
}

func (volumePathTestCreator) RemoveManagedVolume(
	ctx context.Context,
	request ManagedVolumeDirectoryRemoveRequest,
) (ManagedVolumeDirectoryRemoveResult, error) {
	return removeManagedVolume(ctx, request, uint32(os.Getuid()))
}

// Rationale: a real helper replay must recover when its saved cursor names a
// child deleted by a call whose response was lost, without following symlinks.
func TestVolumePathProtocolRecoversDeletedPendingChild(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "vol")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(
		root,
		"tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		testEnvironmentID,
	)
	creator := volumePathTestCreator{}
	if err := creator.Create(ctx, root, directory); err != nil {
		t.Fatal(err)
	}
	leaf := filepath.Join(directory, "app-data")
	if err := os.MkdirAll(filepath.Join(leaf, "a"), 0o700); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 130; index++ {
		for _, parent := range []string{leaf, filepath.Join(leaf, "a")} {
			if err := os.WriteFile(filepath.Join(parent, fmt.Sprintf("z-%03d", index)), []byte("data"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	sentinel := filepath.Join(directory, "retained")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(sentinel, filepath.Join(leaf, "a", "escape")); err != nil {
		t.Fatal(err)
	}
	request := validManagedVolumeRequest(t)
	request.Plan.Operation, request.Plan.TargetId = agentpb.PlanOperation_PLAN_OPERATION_REMOVE, testVolumeID
	request.Plan.Artifacts[0].AuthorizedVolumeDir = directory
	intent := sha256.Sum256([]byte("authorized removal"))
	request.Plan.Steps[0].Payload = &agentpb.ExecutionStep_ManagedVolumeDirectoryRemove{
		ManagedVolumeDirectoryRemove: &agentpb.ManagedVolumeDirectoryRemove{
			ArtifactId: testArtifactID, VolumeId: testVolumeID, ComposeKey: "app-data", IntentSha256: intent[:],
		},
	}
	request.Plan.PlanHash = nil
	var err error
	request.Plan, err = executionplan.Seal(request.Plan)
	if err != nil {
		t.Fatal(err)
	}
	pending := removal.PendingPath{
		OperationID:     request.OperationId,
		VolumeID:        testVolumeID,
		Key:             "app-data",
		IntentSHA256:    intent,
		RequestOrdinal:  1,
		MutationBudget:  128,
		TaskID:          request.TaskId,
		AssignmentID:    request.AssignmentId,
		AgentID:         ids.New(ids.KindAgent),
		AgentGeneration: 1,
		CreatedAt:       time.Now().UTC(),
	}
	pending.RequestSHA256 = removal.PathRequestDigest(pending)
	request.VolumeRemovalPendingPath, err = removal.EncodePendingPath(pending)
	if err != nil {
		t.Fatal(err)
	}
	first, err := Execute(ctx, root, creator, request)
	if err != nil || first.ExitCode != 0 || first.Complete || first.MutationCount != 128 {
		t.Fatalf("first bounded call: %+v %v", first, err)
	}
	pending.Cursor, pending.RequestOrdinal = first.NextCursor, 2
	pending.RequestSHA256 = removal.PathRequestDigest(pending)
	request.VolumeRemovalPendingPath, err = removal.EncodePendingPath(pending)
	if err != nil {
		t.Fatal(err)
	}
	lost, err := Execute(ctx, root, creator, request)
	if err != nil || lost.ExitCode != 0 || lost.Complete || lost.MutationCount > 128 {
		t.Fatalf("lost partial call: %+v %v", lost, err)
	}
	if _, err := os.Stat(filepath.Join(leaf, "a")); !os.IsNotExist(err) {
		t.Fatalf("pending child was not removed: %v", err)
	}
	recovered, err := Execute(ctx, root, creator, request)
	if err != nil || recovered.ExitCode != 0 || !recovered.Complete || recovered.MutationCount > 128 {
		t.Fatalf("exact pending call replay: %+v %v", recovered, err)
	}
	completion, err := removal.DecodeCompletion(recovered.VolumeRemovalCompletion)
	if err != nil || completion.RequestSHA256 != pending.RequestSHA256 || completion.RequestOrdinal != 2 ||
		!completion.DirectoryAbsent {
		t.Fatalf("canonical recovered completion: %+v %v", completion, err)
	}
	if _, err := os.Stat(leaf); !os.IsNotExist(err) {
		t.Fatalf("Volume leaf remains: %v", err)
	}
	absent, err := Execute(ctx, root, creator, request)
	if err != nil || absent.ExitCode != 0 || !absent.Complete || absent.MutationCount != 0 {
		t.Fatalf("already-absent pending leaf replay: %+v %v", absent, err)
	}
	retained, err := os.ReadFile(sentinel)
	if err != nil || string(retained) != "keep" {
		t.Fatalf("symlink target changed: %v", err)
	}
}
