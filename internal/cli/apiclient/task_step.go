package apiclient

import (
	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/slug"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func taskStepFromGenerated(step generated.TaskStep) (apiTypes.TaskStep, error) {
	if !step.Kind.Valid() {
		return apiTypes.TaskStep{}, errs.New(errs.KindInternal, "apiclient: Task response has unknown step kind")
	}
	result := apiTypes.TaskStep{Name: step.Name, Status: apiTypes.TaskStatus(step.Status), Action: step.Action,
		Description: valueOrEmpty(step.Description), Target: valueOrEmpty(step.Target)}
	if step.TimeoutSeconds != nil {
		if *step.TimeoutSeconds <= 0 {
			return apiTypes.TaskStep{}, errs.New(errs.KindInternal, "apiclient: Task step has an invalid timeout")
		}
		result.TimeoutSeconds = uint32(*step.TimeoutSeconds)
	}
	checks, err := taskHealthchecksFromGenerated(step.Healthchecks)
	if err != nil {
		return apiTypes.TaskStep{}, err
	}
	result.Healthchecks = checks
	result.Failure = taskFailureFromGenerated(step.Failure)
	if health := step.HealthWait; health != nil {
		var err error
		result.HealthWait, err = taskReadinessFromGenerated(*health)
		if err != nil {
			return apiTypes.TaskStep{}, err
		}
		if step.HealthWaitReceivedAt == nil || step.HealthWaitReceivedAt.IsZero() {
			return apiTypes.TaskStep{}, errs.New(
				errs.KindInternal,
				"apiclient: Task health observation has no receipt time",
			)
		}
		result.HealthWaitReceivedAt = step.HealthWaitReceivedAt
	}
	switch step.Kind {
	case generated.TaskStepKindOperation:
		if step.ScriptId != nil || step.ScriptSlug != nil {
			return apiTypes.TaskStep{}, errs.New(
				errs.KindInternal, "apiclient: operation Task step has Script identity",
			)
		}
		result.Kind = apiTypes.TaskStepOperation
	case generated.TaskStepKindScript:
		if step.ScriptId == nil || step.ScriptSlug == nil ||
			ids.Validate(ids.KindScript, valueOrEmpty(step.ScriptId)) != nil ||
			!slug.Valid(valueOrEmpty(step.ScriptSlug)) {
			return apiTypes.TaskStep{}, errs.New(
				errs.KindInternal, "apiclient: script Task step has invalid Script identity",
			)
		}
		result.Kind = apiTypes.TaskStepScript
		result.ScriptID = *step.ScriptId
		result.ScriptSlug = *step.ScriptSlug
	}
	return result, nil
}

func taskReadinessFromGenerated(value generated.Readiness) (*taskprogress.Readiness, error) {
	for _, count := range []int32{value.Expected, value.Observed, value.Running, value.Ready, value.Starting, value.Unhealthy, value.Exited} {
		if count < 0 || count > 65536 {
			return nil, errs.New(errs.KindInternal, "apiclient: Task health observation count is invalid")
		}
	}
	p := &taskprogress.Readiness{Expected: uint32(value.Expected), Observed: uint32(value.Observed),
		Running: uint32(value.Running), Ready: uint32(value.Ready), Starting: uint32(value.Starting),
		Unhealthy: uint32(value.Unhealthy), Exited: uint32(value.Exited),
		ElapsedMilliseconds: uint64(value.ElapsedMilliseconds), TimeoutMilliseconds: uint64(value.TimeoutMilliseconds),
		Diagnostic: string(value.Diagnostic)}
	if err := p.Validate(); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return p, nil
}
