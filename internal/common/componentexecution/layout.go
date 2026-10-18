package componentexecution

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Layout retains only the fixed execution shape in the local journal. It is
// not an executable input: the executor must match it against the captured
// assignment before acting. Compose artifacts and file bodies stay out of
// checkpoint rewrites.
type Layout struct {
	ComponentID string              `json:"component_id"`
	Input       workexecution.Input `json:"input"`
	Remove      bool                `json:"remove"`
	WriteFiles  bool                `json:"write_files"`
	Activate    bool                `json:"activate"`
}

func CaptureLayout(attempt workexecution.Attempt, input *agentpb.ComponentWorkInput) (Layout, error) {
	_, raw, reference, err := executionplan.CaptureComponentWorkInput(attempt.Input.ID, input)
	clear(raw)
	if err != nil || reference != attempt.Input {
		return Layout{}, invalidProgress()
	}
	layout := Layout{ComponentID: input.ComponentId, Input: reference, Remove: input.Remove,
		WriteFiles: len(input.Files) != 0, Activate: input.ConfigurationAction != nil}
	return layout, layout.Validate(attempt)
}

func (layout Layout) Validate(attempt workexecution.Attempt) error {
	if attempt.Validate() != nil || ids.Validate(ids.KindComponent, layout.ComponentID) != nil ||
		layout.Input != attempt.Input || (layout.Remove && (layout.WriteFiles || layout.Activate)) {
		return invalidProgress()
	}
	return nil
}

func (layout Layout) Matches(attempt workexecution.Attempt, input *agentpb.ComponentWorkInput) error {
	captured, err := CaptureLayout(attempt, input)
	if err != nil || captured != layout {
		return invalidProgress()
	}
	return nil
}

func (layout Layout) stages() []Stage {
	if layout.Remove {
		return []Stage{RemoveRuntime, VerifyRuntime}
	}
	var result []Stage
	if layout.WriteFiles {
		result = append(result, WriteConfiguration)
	}
	result = append(result, ReconcileRuntime)
	if layout.Activate {
		result = append(result, ActivateConfiguration)
	}
	return append(result, VerifyRuntime)
}
