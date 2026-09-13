package cli

import (
	"bytes"
	"context"
	"testing"

	"github.com/spf13/cobra"

	clicommon "github.com/AlanD20/groundplane/internal/cli/common"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// QA: TASK-01, UI-01; pure TABLE presentation only, not Task acceptance or follow-up retrieval.
// Rationale: asynchronous actions keep the concise follow-up hint for the
// default TABLE format while JSON and YAML remain machine-readable.
func TestRenderDispatchedTaskTableOutput(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	command := &cobra.Command{}
	command.SetOut(&output)
	command.SetContext(context.WithValue(context.Background(), appKey{}, &App{
		Out: clicommon.NewWriter(clicommon.FormatTable, true, &output),
	}))

	err := renderDispatchedTask(command, apiTypes.TaskAccepted{TaskID: "task_1"})
	want := "task task_1 dispatched — `groundplane task show task_1` to follow\n"
	if err != nil || output.String() != want {
		t.Fatalf("render dispatched Task = %q, %v; want %q", output.String(), err, want)
	}
}
