package releaseexecution

import (
	"sort"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// releaseHooks selects from the captured set; it never discovers newer Scripts.
// Stable sorting preserves authored order among hooks with the same priority.
func releaseHooks(input Input, phase Phase) []*agentpb.ReleaseWorkHook {
	var hooks []*agentpb.ReleaseWorkHook
	for _, hook := range input.Release.Hooks {
		if phase == PreHooks && (hook.Phase == "pre-deploy" || hook.Phase == "pre-rollback") ||
			phase == PostHooks && (hook.Phase == "post-deploy" || hook.Phase == "post-rollback") {
			hooks = append(hooks, hook)
		}
	}
	sort.SliceStable(hooks, func(left, right int) bool { return hooks[left].Order < hooks[right].Order })
	return hooks
}

// ActiveHook returns the exact captured hook at an acknowledged in-flight
// boundary. Callers cannot choose another hook merely by supplying its name.
func (progress Progress) ActiveHook(input Input) (*agentpb.ReleaseWorkHook, error) {
	if progress.Validate(input) != nil || !progress.InFlight || progress.Phase != PreHooks && progress.Phase != PostHooks {
		return nil, invalidProgress()
	}
	return proto.CloneOf(releaseHooks(input, progress.Phase)[progress.Hook]), nil
}
