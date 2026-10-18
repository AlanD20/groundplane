package blueprintrelease

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// finishSelectedRuntime binds current file recovery to the staged native rollout
// and prepares the one atomic Release publication. Native planning, image
// selection and hook input construction are not repeated in this phase.
func (service *Service) finishSelectedRuntime(
	ctx context.Context,
	input prepareInput,
	captured preparedSelectedRuntime,
	addresses networkreservations.ProxyAddresses,
) (_ Prepared, resultErr error) {
	hooks, manifest := captured.hooks, captured.manifest
	rollout, err := service.ledger.StageServiceRollout(ctx, captured.capture)
	if err != nil {
		return Prepared{}, err
	}
	defer clear(rollout.Record.CandidateArtifact)
	configuration, err := service.prepareWorkConfiguration(ctx, input, rollout.Record, input.Files)
	if err != nil {
		return Prepared{}, err
	}
	defer clear(configuration.Value)
	defer func() {
		if resultErr != nil && !blueprintPublicationOutcomeUnknown(resultErr) {
			resultErr = errors.Join(resultErr,
				service.ledger.AbandonReleaseWorkSecretPins(ctx, input.Work, configuration.Secrets))
		}
	}()
	publishedAt := time.Now().UTC()
	preparedInput, err := service.ledger.PrepareReleaseWorkInput(ctx, input.Work,
		rollout, configuration.Input, configuration.Secrets, publishedAt)
	if err != nil {
		return Prepared{}, err
	}
	defer keyvalue.ZeroMutationBytes(preparedInput.Change.Mutations)
	hookWork := etcd.ReleaseHookWorkPublication{
		Task: input.Work.Task.Task, Item: preparedInput.Item, OperationID: input.OperationID,
		ReadRevision: manifest.ReadRevision,
	}
	defer func() {
		for index := range hookWork.Hooks {
			hookWork.Hooks[index].Execution.Clear()
		}
	}()
	operation, err := releaserender.ReleaseOperationKind(input.Work.Task.Task.Action)
	if err != nil {
		return Prepared{}, err
	}
	phases, err := releaserender.OrderedReleaseHooks(operation,
		captured.capture.Render.ServiceID, captured.capture.Render.Hooks)
	if err != nil {
		return Prepared{}, err
	}
	defer phases.Clear()
	for _, group := range [][]releaserender.ReleaseHookRenderInput{phases.Before, phases.After, phases.Failure} {
		for _, hook := range group {
			execution, err := releaserender.CaptureHookExecution(hookWork.Task, hookWork.Item,
				input.OperationID, hook, input.CreatedAt)
			if err != nil {
				return Prepared{}, err
			}
			sources, exists := hooks.sources[execution.ID]
			if !exists {
				execution.Clear()
				return Prepared{}, errs.New(errs.KindInternal, "Blueprint post-deploy execution source is missing")
			}
			hookWork.Hooks = append(hookWork.Hooks, etcd.ReleaseHookExecutionPublication{Sources: sources, Execution: execution})
		}
	}
	if len(hookWork.Hooks) != hooks.executions {
		return Prepared{}, errs.New(errs.KindInternal, "Blueprint post-deploy execution authority is incomplete")
	}
	hookPrepared, err := service.ledger.PrepareReleaseWorkHooks(ctx, input.Work, hookWork)
	if err != nil {
		return Prepared{}, err
	}
	for index := range hookWork.Hooks {
		hookWork.Hooks[index].SnapshotRevision = hookPrepared.SnapshotRevision(
			hookWork.Hooks[index].Execution.SnapshotID,
		)
	}
	sourceMembers, err := service.ledger.ReleaseHookSourceMembers(ctx, manifest, hookWork.Hooks)
	if err != nil {
		return Prepared{}, err
	}
	var sourcePrepared scriptsourcepublication.PreparedSourceSet
	if len(sourceMembers) != 0 {
		sourcePrepared, err = service.sources.Prepare(ctx, input.OperationID, sourceMembers)
		if err != nil {
			if blueprintPublicationOutcomeUnknown(err) {
				return Prepared{}, err
			}
			if cleanupErr := service.sources.Abandon(ctx, input.OperationID, sourceMembers); cleanupErr != nil {
				return Prepared{}, errors.Join(err, cleanupErr)
			}
			return Prepared{}, err
		}
	}
	for index := range hookWork.Hooks {
		hookWork.Hooks[index].Execution.SourceMembershipCount = sourcePrepared.MembershipCount()
		hookWork.Hooks[index].Execution.SourceMembershipSHA256 = sourcePrepared.MembershipSHA256()
	}
	publication, err := service.ledger.PrepareReleaseWorkPublication(
		ctx,
		service.sources,
		etcd.ReleaseWorkPublicationEvidence{
			RenderingInput:     input.Projection,
			ServiceRollout:     &rollout,
			ProxyAddresses:     addresses,
			NativePredecessors: captured.predecessors,
			StorageConditions:  captured.storageConditions,
			Manifest:           manifest, EnvironmentID: input.Environment.Record.ID, PublishedAt: publishedAt,
			Work: input.Work, PreparedInput: preparedInput,
			HookWork: hookWork, HookPrepared: hookPrepared, SourcePrepared: sourcePrepared, SourceMembers: sourceMembers,
		},
	)
	if err != nil {
		if blueprintPublicationOutcomeUnknown(err) {
			return Prepared{}, err
		}
		if len(sourceMembers) != 0 {
			if cleanupErr := service.sources.Abandon(ctx, input.OperationID, sourceMembers); cleanupErr != nil {
				return Prepared{}, errors.Join(err, cleanupErr)
			}
		}
		return Prepared{}, err
	}
	return Prepared{Item: preparedInput.Item, Input: preparedInput.Input, Publication: publication,
		ledger: service.ledger, work: input.Work, secrets: configuration.Secrets}, nil
}

func blueprintPublicationOutcomeUnknown(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
