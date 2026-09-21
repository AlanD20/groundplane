package releaserender

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumReleaseHookExecutions = 16
	maximumReleaseHookBodyBytes  = 1 << 20
)

const releaseHookStepMemberParamPrefix = "release_hook_member/"

const releaseHookStepExecutionParamPrefix = "release_hook_execution/"

func ReleaseHookStepMemberParam(stepID string) string {
	return releaseHookStepMemberParamPrefix + stepID
}

func ReleaseHookStepExecutionParam(stepID string) string {
	return releaseHookStepExecutionParamPrefix + stepID
}

// ReleaseHookRenderInput is the immutable input for one automatic Script
// execution. Runner bytes are protobuf encodings of the sealed snapshot and
// applied projection; Script body bytes remain in the durable generation.
type ReleaseHookRenderInput struct {
	ScriptID                string          `json:"script_id"`
	ScriptSlug              string          `json:"script_slug"`
	ServiceID               string          `json:"service_id"`
	When                    core.ScriptHook `json:"when"`
	Order                   uint16          `json:"order,omitempty"`
	ScriptGeneration        uint64          `json:"script_generation"`
	ScriptExecutionID       string          `json:"script_execution_id"`
	RunnerSnapshotID        string          `json:"runner_snapshot_id"`
	BodySize                uint32          `json:"body_size"`
	BodySHA256              string          `json:"body_sha256"`
	ServiceDefinitionSHA256 string          `json:"service_definition_sha256"`
	RunnerSnapshot          []byte          `json:"runner_snapshot"`
	RunnerProjection        []byte          `json:"runner_projection"`
}

func cloneReleaseHookRenderInputs(input []ReleaseHookRenderInput) []ReleaseHookRenderInput {
	if len(input) == 0 {
		return nil
	}
	output := make([]ReleaseHookRenderInput, len(input))
	copy(output, input)
	for index := range output {
		output[index].RunnerSnapshot = append([]byte(nil), input[index].RunnerSnapshot...)
		output[index].RunnerProjection = append([]byte(nil), input[index].RunnerProjection...)
	}
	return output
}

func validateReleaseHookRenderInputs(hooks []ReleaseHookRenderInput, serviceID string) error {
	if len(hooks) > maximumReleaseHookExecutions {
		return errs.New(errs.KindValidationFailed, "release selects too many automatic Script hooks")
	}
	bodyBytes := 0
	seenSlugs := make(map[string]struct{}, len(hooks))
	seenExecutions := make(map[string]struct{}, len(hooks))
	for _, hook := range hooks {
		if ids.Validate(ids.KindScript, hook.ScriptID) != nil || hook.ServiceID != serviceID || hook.ScriptSlug == "" ||
			hook.ScriptGeneration == 0 {
			return errs.New(errs.KindValidationFailed, "release hook identity is invalid")
		}
		if _, exists := seenSlugs[hook.ScriptSlug]; exists {
			return errs.New(errs.KindValidationFailed, "release selects a duplicate Script slug")
		}
		seenSlugs[hook.ScriptSlug] = struct{}{}
		if _, err := ulid.ParseStrict(hook.ScriptExecutionID); err != nil {
			return errs.New(errs.KindValidationFailed, "release hook execution identity is invalid")
		}
		if _, exists := seenExecutions[hook.ScriptExecutionID]; exists {
			return errs.New(errs.KindValidationFailed, "release selects a duplicate Script execution")
		}
		seenExecutions[hook.ScriptExecutionID] = struct{}{}
		if _, err := ulid.ParseStrict(hook.RunnerSnapshotID); err != nil || len(hook.RunnerSnapshot) == 0 ||
			len(hook.RunnerProjection) == 0 {
			return errs.New(errs.KindValidationFailed, "release hook runner snapshot is not pinned")
		}
		if len(hook.BodySHA256) != 64 || len(hook.ServiceDefinitionSHA256) != 64 {
			return errs.New(errs.KindValidationFailed, "release hook digest metadata is invalid")
		}
		if _, err := hex.DecodeString(hook.BodySHA256); err != nil {
			return errs.New(errs.KindValidationFailed, "release hook body digest is invalid")
		}
		if _, err := hex.DecodeString(hook.ServiceDefinitionSHA256); err != nil {
			return errs.New(errs.KindValidationFailed, "release hook service digest is invalid")
		}
		if hook.BodySize > 64*1024 {
			return errs.New(errs.KindValidationFailed, "release hook body exceeds the Script limit")
		}
		bodyBytes += int(hook.BodySize)
		if bodyBytes > maximumReleaseHookBodyBytes {
			return errs.New(errs.KindValidationFailed, "release hook bodies exceed the release limit")
		}
		if hook.When != core.ScriptPreDeploy && hook.When != core.ScriptPostDeploy &&
			hook.When != core.ScriptPreRollback &&
			hook.When != core.ScriptPostRollback &&
			hook.When != core.ScriptOnFailure {
			return errs.New(errs.KindValidationFailed, "release hook trigger is invalid")
		}
	}
	return nil
}
