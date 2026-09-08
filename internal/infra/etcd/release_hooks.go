package etcd

import (
	"bytes"
	"context"
	"encoding/hex"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/oklog/ulid/v2"
)

// ListPlanningHookScriptIDs returns the fixed-revision hook selection for one
// release member in deterministic scoped-slug order.
func (ledger *ReleaseLedger) ListPlanningHookScriptIDs(
	ctx context.Context,
	scope ReleasePlanningScope,
	serviceID string,
	operation domain.OperationKind,
) ([]string, error) {
	if ctx == nil || ledger == nil || ledger.store == nil || scope.ReadRevision <= 0 ||
		ids.Validate(ids.KindService, serviceID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "release hook selection is invalid")
	}
	allowed := map[core.ScriptHook]struct{}{core.ScriptOnFailure: {}}
	if operation == domain.OperationDeploy {
		allowed[core.ScriptPreDeploy], allowed[core.ScriptPostDeploy] = struct{}{}, struct{}{}
	} else if operation == domain.OperationRollback {
		allowed[core.ScriptPreRollback], allowed[core.ScriptPostRollback] = struct{}{}, struct{}{}
	} else {
		return nil, errs.New(errs.KindValidationFailed, "release hook operation is invalid")
	}
	type selectedHook struct{ id, slug string }
	selected := make([]selectedHook, 0)
	active, err := readActiveScriptSet(ctx, ledger.store, scope.Environment.Record.ID, scope.ReadRevision)
	if err != nil {
		return nil, err
	}
	prefix, start := scriptSetOwnerPrefix(scope.Environment.Record.ID, active.Record.GenerationID), ""
	for {
		page, err := ledger.store.Range(ctx, RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: 64, Revision: scope.ReadRevision,
		})
		if err != nil {
			return nil, err
		}
		if page == nil || page.ReadRevision != scope.ReadRevision {
			return nil, corruptReleaseRecord()
		}
		for _, value := range page.Values {
			scriptID := strings.TrimPrefix(value.Key, prefix)
			if strings.Contains(scriptID, "/") || ids.Validate(ids.KindScript, scriptID) != nil ||
				!bytes.Equal(value.Value, []byte(scriptID)) {
				return nil, corruptReleaseRecord()
			}
			primary, readErr := scriptExecutionValueAt(ctx, ledger.store, scriptSetScriptKey(
				scope.Environment.Record.ID, active.Record.GenerationID, scriptID,
			), scope.ReadRevision)
			if readErr != nil {
				return nil, readErr
			}
			record, decodeErr := decodeScriptRecord(primary.Value)
			if decodeErr != nil || record.EnvironmentID != scope.Environment.Record.ID ||
				record.ScriptSetGeneration != active.Record.GenerationID {
				return nil, corruptReleaseRecord()
			}
			if record.ServiceID == serviceID {
				if _, exists := allowed[record.Desired.When]; exists {
					selected = append(selected, selectedHook{id: scriptID, slug: record.Desired.Slug})
				}
			}
		}
		if len(page.Values) == 0 || !page.More {
			break
		}
		start = page.Values[len(page.Values)-1].Key
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].slug < selected[j].slug })
	result := make([]string, len(selected))
	for index := range selected {
		result[index] = selected[index].id
	}
	return result, nil
}

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
