package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ComponentFileValidator runs only the compiled recipe selected by the sealed
// action. Failure must occur before the materialization helper receives bytes.
type ComponentFileValidator interface {
	ValidateComponentFile(context.Context, componentsdk.ActionEnvelope, string, []byte) error
}

func (runtime *MaterializationRuntime) preflightComponentFile(
	ctx context.Context, assignment Assignment, step *agentpb.ExecutionStep, payload materializationPayload,
) (materializationPayload, error) {
	var selected *agentpb.ExecutionStep
	for _, candidate := range assignment.Plan.GetSteps() {
		action := candidate.GetComponentApply()
		if action == nil || action.GetArtifactId() != step.GetMaterializeFile().GetMaterializationId() {
			continue
		}
		if selected != nil {
			return materializationPayload{}, closeMaterializationSource(
				payload.Source,
				"agent: Component file action is duplicated",
			)
		}
		selected = candidate
	}
	if selected == nil {
		return payload, nil
	}
	action := selected.GetComponentApply()
	header := payload.Header
	digest := header.Digest()
	if runtime.componentFiles == nil || !componentActionRequiresStep(assignment.Plan, selected, step.GetStepId()) ||
		action.GetManagedConfigContent() || action.GetGeneration() != header.Generation() ||
		header.OutputKind() != entrymaterialization.OutputPlainFile || header.ServiceID() != "" ||
		header.ServiceName() != "" || header.UID() != 0 || header.GID() != 0 ||
		header.Mode() != entrymaterialization.ModeReadOnly ||
		!bytes.Equal(action.GetArtifactDigest(), digest[:]) {
		return materializationPayload{}, closeMaterializationSource(
			payload.Source,
			"agent: Component file preflight authority is invalid",
		)
	}
	envelope, err := DecodeComponentAction(action)
	if err != nil {
		return materializationPayload{}, errs.Wrap(errs.KindValidationFailed, errors.Join(err, payload.Source.Close()))
	}
	content, readErr := io.ReadAll(io.LimitReader(payload.Source, int64(entrymaterialization.MaximumContentBytes)+1))
	if err := errors.Join(readErr, payload.Source.Close()); err != nil {
		clear(content)
		return materializationPayload{}, errs.Wrap(errs.KindInternal, err)
	}
	if uint64(len(content)) != header.Length() || sha256.Sum256(content) != digest {
		clear(content)
		return materializationPayload{}, errs.New(
			errs.KindValidationFailed,
			"agent: Component preflight content differs from the sealed file",
		)
	}
	if err := runtime.componentFiles.ValidateComponentFile(ctx, envelope, header.Destination(), content); err != nil {
		clear(content)
		return materializationPayload{}, err
	}
	if ctx.Err() != nil || sha256.Sum256(content) != digest {
		clear(content)
		return materializationPayload{}, errs.New(
			errs.KindValidationFailed,
			"agent: Component preflight content or execution authority changed",
		)
	}
	payload.Source = &ownedMaterializationSource{content: content, reader: bytes.NewReader(content)}
	return payload, nil
}

func componentActionRequiresStep(plan *agentpb.ExecutionPlan, action *agentpb.ExecutionStep, required string) bool {
	steps := make(map[string]*agentpb.ExecutionStep, len(plan.GetSteps()))
	for _, step := range plan.GetSteps() {
		if step == nil || step.GetStepId() == "" || steps[step.GetStepId()] != nil {
			return false
		}
		steps[step.GetStepId()] = step
	}
	prerequisite := action.GetPrerequisiteStepId()
	for range len(steps) {
		if prerequisite == required {
			return true
		}
		previous := steps[prerequisite]
		if previous == nil {
			return false
		}
		prerequisite = previous.GetPrerequisiteStepId()
	}
	return false
}
