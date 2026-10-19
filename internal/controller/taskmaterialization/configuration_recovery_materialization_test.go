package taskmaterialization

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
)

// TASK-126/SVC-15: native recovery stages retain the old file's source identity
// and bytes after a failed candidate changes its metadata. Missing retained
// content must fail rather than rerendering. This is source selection/delivery,
// not physical restoration or authenticated request admission.
func TestRecoveryMaterializationDeliversOriginalContentForNewExecutionIDs(t *testing.T) {
	t.Parallel()
	expected := []byte("original config")
	digest := sha256.Sum256(expected)
	old := taskmaterialization.Record{StepID: ids.New(ids.KindStep), MaterializationID: ids.New(ids.KindConfig),
		EnvironmentID: resolverEnvironmentID, Destination: "components/caddy/Caddyfile",
		OutputKind: taskmaterialization.OutputPlainFile, UID: 101, GID: 102, Mode: 0o444,
		Length: uint64(len(expected)), SHA256: hex.EncodeToString(digest[:]),
		Source: taskmaterialization.Source{Kind: taskmaterialization.SourceComponentFile,
			ComponentFile: &taskmaterialization.ComponentFileValueReference{RevisionID: ids.New(ids.KindTask),
				ComponentID: ids.New(ids.KindComponent), Path: "components/caddy/Caddyfile"}}}
	prior := runtimeconfiguration.Snapshot{ID: ids.New(ids.KindConfig), EnvironmentID: resolverEnvironmentID,
		Generation: 1, Files: []taskmaterialization.Record{old}}
	changed := old
	changed.StepID, changed.MaterializationID, changed.UID = ids.New(ids.KindStep), ids.New(ids.KindConfig), 201
	selected, err := runtimeconfiguration.SelectRestorations(resolverEnvironmentID, 2, &prior,
		[]taskmaterialization.Record{changed})
	if err != nil || len(selected) != 1 {
		t.Fatalf("select captured predecessor: %v", err)
	}
	resolver := testTaskMaterializationResolver(t, nil)
	content := &resolverRetainedContent{content: expected}
	if err := resolver.EnableComponentMaterializationContent(content); err != nil {
		t.Fatal(err)
	}
	taskID := ids.New(ids.KindTask)
	for range 2 {
		stageID := ids.New(ids.KindStep)
		header, stream, err := resolver.OpenCapturedFile(t.Context(), taskID, stageID, 2, selected[0].Prior)
		if err != nil {
			t.Fatal(err)
		}
		actual, readErr := io.ReadAll(stream)
		closeErr := stream.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(actual, expected) ||
			content.loaded.StepID != old.StepID || content.loaded.MaterializationID != old.MaterializationID ||
			header.UID() != old.UID || header.StepID() != stageID {
			t.Fatalf("wrong retained content or execution identity: %v/%v", readErr, closeErr)
		}
		clear(actual)
		content.content = []byte("missing retained output")
		_, missing, err := resolver.OpenCapturedFile(t.Context(), taskID, stageID, 2, selected[0].Prior)
		if err == nil || missing != nil {
			t.Fatal("missing exact content fell back to another source")
		}
		content.content = expected
	}
}
