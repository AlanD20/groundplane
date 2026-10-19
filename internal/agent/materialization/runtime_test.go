package materialization

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
)

// TASK-91 / ENT-02: the retained physical materializer must stream its private
// file through the hardened frame, not argv, and reject changed bytes. The
// native source owns channel authentication and file selection, not an inbox.
func TestMaterializationRuntimeStreamsCapturedFileAndRejectsCorruption(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		content := []byte("services: {}\n")
		header, err := entrymaterialization.NewHeader(entrymaterialization.HeaderSpec{
			TaskID: materializationTaskID, StepID: materializationStepID,
			EnvironmentID: materializationEnvironmentID, Generation: 7,
			Destination: "blueprints/current/blueprint.yaml", OutputKind: entrymaterialization.OutputPlainFile,
			Mode: entrymaterialization.ModeReadOnly, Length: uint64(len(content)), Digest: sha256.Sum256(content),
		})
		if err != nil {
			t.Fatal(err)
		}
		want := append([]byte(nil), content...)
		if corrupt {
			content[0] ^= 1
		}
		helper := &decodingMaterializationHelper{}
		runtime, err := New(helper, nil)
		if err != nil {
			t.Fatal(err)
		}
		source := OwnContent(content)
		err = runtime.RunConfigRestoreFile(t.Context(), materializationVolumeDir, header, source, false)
		if closeErr := source.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if !bytes.Equal(content, make([]byte, len(content))) {
			t.Fatal("private source was not cleared on close")
		}
		if corrupt {
			if err == nil {
				t.Fatal("changed file bytes crossed the authenticated frame")
			}
		} else if err != nil || helper.volumeDir != materializationVolumeDir ||
			helper.header != header || !bytes.Equal(helper.content, want) {
			t.Fatalf("materializer lost captured file: %v", err)
		}
	}
}
