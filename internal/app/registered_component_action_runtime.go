package app

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"io"
	"runtime"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane-component-sdk/component"
	registeredcatalog "github.com/AlanD20/groundplane-registered-components/catalog"
	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/common/managedconfig"
	"github.com/AlanD20/groundplane/internal/infra/docker/dnsresolverobserver"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type registeredComponentActionRuntime struct {
	catalog       registeredActionCatalog
	managedHelper managedConfigExecutor
	composeHelper agent.ComposeHelper
	observer      dnsResolverObserver
}

type dnsResolverObserver interface {
	Observe(context.Context, dnsresolverobserver.Request) (*agentpb.DNSResolverObservationEvidence, error)
}

type managedConfigExecutor interface {
	Validate(context.Context, string, []string, []byte) error
	Execute(context.Context, *agentpb.ManagedConfigHelperRequest) (*agentpb.ManagedConfigHelperResponse, error)
}

func newRegisteredComponentActionRuntime(
	catalog registeredActionCatalog,
	managedHelper managedConfigExecutor,
	composeHelper agent.ComposeHelper,
	observer dnsResolverObserver,
) (*registeredComponentActionRuntime, error) {
	if managedHelper == nil || composeHelper == nil || observer == nil {
		return nil, errs.New(errs.KindInternal, "registered Component action helper is required")
	}
	return &registeredComponentActionRuntime{
		catalog: catalog, managedHelper: managedHelper, composeHelper: composeHelper, observer: observer,
	}, nil
}

func (runtime *registeredComponentActionRuntime) ExecuteComponentAction(
	ctx context.Context,
	assignment agent.Assignment,
	step *agentpb.ExecutionStep,
	payload agent.ManagedConfigPayload,
) (result *agent.ComponentActionResult, resultErr error) {
	if runtime == nil || runtime.managedHelper == nil || runtime.composeHelper == nil || ctx == nil {
		return nil, closeComponentArtifact(payload.Source, errs.New(
			errs.KindInternal,
			"agent: registered Component action runtime is not configured",
		))
	}
	action := step.GetComponentApply()
	envelope, err := agent.DecodeComponentAction(action)
	if err != nil {
		return nil, closeComponentArtifact(payload.Source, err)
	}
	if assignment.Plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY {
		if payload.Source != nil {
			return nil, closeComponentArtifact(payload.Source, errs.New(
				errs.KindInternal, "agent: container Component action received managed content",
			))
		}
		_, _, _, resolveErr := runtime.catalog.ResolveContainerConfigActionEnvelope(envelope)
		if resolveErr != nil {
			return nil, resolveErr
		}
		response, executeErr := runtime.composeHelper.Execute(ctx, &agentpb.ComposeHelperRequest{
			Schema:         1,
			AssignmentId:   assignment.AssignmentID,
			TaskId:         assignment.TaskID,
			OperationId:    assignment.OperationID,
			Plan:           assignment.Plan,
			StepId:         step.GetStepId(),
			TimeoutSeconds: step.GetTimeoutSeconds(),
		})
		if executeErr != nil {
			return nil, executeErr
		}
		if response == nil || response.GetSchema() != 1 ||
			response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
			response.GetExitCode() != 0 ||
			response.GetDiagnostic() != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE {
			return nil, errs.New(errs.KindRequestFailed, "agent: Component container action failed")
		}
		return nil, nil
	}
	_, _, observationRecipe, observationErr := runtime.catalog.ResolveDNSResolverObservationActionEnvelope(envelope)
	if observationErr == nil {
		if payload.Source != nil {
			return nil, closeComponentArtifact(payload.Source, errs.New(
				errs.KindValidationFailed,
				"agent: DNS resolver observation received managed content",
			))
		}
		request, requestErr := dnsResolverObservationRequest(assignment, action, observationRecipe)
		if requestErr != nil {
			return nil, requestErr
		}
		evidence, observeErr := runtime.observer.Observe(ctx, request)
		if observeErr != nil {
			return nil, observeErr
		}
		return &agent.ComponentActionResult{DNSResolverObservation: evidence}, nil
	}
	if payload.Source == nil {
		return nil, errs.New(errs.KindInternal, "agent: managed Component action content is missing")
	}
	defer func() {
		if closeErr := payload.Source.Close(); closeErr != nil {
			resultErr = errs.Wrap(errs.KindInternal, errors.Join(resultErr, closeErr))
		}
	}()
	artifact := envelope.Artifact()
	digest := artifact.Digest()
	if payload.Header.ArtifactID != artifact.ID().String() ||
		payload.Header.MediaType != managedconfig.MediaTypeTextUTF8 ||
		payload.Header.Length == 0 || payload.Header.Length > managedconfig.MaximumArtifactBytes ||
		subtle.ConstantTimeCompare(payload.Header.Digest[:], digest[:]) != 1 {
		return nil, errs.New(errs.KindValidationFailed, "agent: managed-config payload does not match Component action")
	}
	content, err := io.ReadAll(io.LimitReader(payload.Source, managedconfig.MaximumArtifactBytes+1))
	if err != nil {
		clear(content)
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(content)
	contentDigest := sha256.Sum256(content)
	if uint64(len(content)) != payload.Header.Length ||
		subtle.ConstantTimeCompare(contentDigest[:], digest[:]) != 1 {
		return nil, errs.New(errs.KindValidationFailed, "agent: managed-config source changed after transfer")
	}
	_, _, recipe, err := runtime.catalog.ResolveManagedConfigActionEnvelope(envelope)
	if err != nil {
		return nil, err
	}
	if err := runtime.managedHelper.Validate(
		ctx, selectedImageReference(recipe.Image()), recipe.ValidateArgs(), content,
	); err != nil {
		return nil, err
	}
	request := managedConfigRequest(
		assignment,
		action,
		recipe.RelativePath(),
		agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_PUBLISH,
	)
	request.Content = content
	result = &agent.ComponentActionResult{}
	state, err := runtime.executeManagedConfig(ctx, request)
	if err != nil {
		return result, err
	}
	result.ManagedConfig = &state
	return result, nil
}

func (runtime *registeredComponentActionRuntime) FinalizeManagedConfig(
	ctx context.Context,
	assignment agent.Assignment,
	step *agentpb.ExecutionStep,
	commit bool,
) (agent.ManagedConfigTransactionState, error) {
	if runtime == nil || runtime.managedHelper == nil || ctx == nil || step == nil {
		return agent.ManagedConfigTransactionState{}, errs.New(
			errs.KindInternal,
			"agent: managed-config finalization runtime is not configured",
		)
	}
	action := step.GetComponentApply()
	envelope, err := agent.DecodeComponentAction(action)
	if err != nil {
		return agent.ManagedConfigTransactionState{}, err
	}
	_, _, recipe, err := runtime.catalog.ResolveManagedConfigActionEnvelope(envelope)
	if err != nil {
		return agent.ManagedConfigTransactionState{}, err
	}
	operation := agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK
	if commit {
		operation = agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_COMMIT
	}
	return runtime.executeManagedConfig(
		ctx,
		managedConfigRequest(assignment, action, recipe.RelativePath(), operation),
	)
}

func (runtime *registeredComponentActionRuntime) executeManagedConfig(
	ctx context.Context,
	request *agentpb.ManagedConfigHelperRequest,
) (agent.ManagedConfigTransactionState, error) {
	response, firstErr := runtime.managedHelper.Execute(ctx, request)
	state, stateErr := managedConfigTransactionState(request, response)
	if firstErr == nil && stateErr == nil {
		return state, nil
	}
	firstErr = errors.Join(firstErr, stateErr)
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	response, recoveryErr := runtime.managedHelper.Execute(recoveryCtx, request)
	if recoveryErr == nil {
		state, recoveryErr = managedConfigTransactionState(request, response)
	}
	if recoveryErr != nil {
		return agent.ManagedConfigTransactionState{}, errs.Wrap(
			errs.KindRequestFailed,
			errors.Join(firstErr, recoveryErr),
		)
	}
	return state, nil
}

func managedConfigTransactionState(
	request *agentpb.ManagedConfigHelperRequest,
	response *agentpb.ManagedConfigHelperResponse,
) (agent.ManagedConfigTransactionState, error) {
	if request == nil || response == nil || response.GetSchema() != managedconfighelper.SchemaVersion ||
		response.GetTransactionId() != request.GetTransactionId() ||
		response.GetOperation() != request.GetOperation() ||
		response.GetDisposition() == agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_UNSPECIFIED {
		return agent.ManagedConfigTransactionState{}, errs.New(
			errs.KindStateConflict,
			"agent: managed-config helper response does not match its transaction",
		)
	}
	live, err := managedConfigFileState(response.GetLiveSha256())
	if err != nil {
		return agent.ManagedConfigTransactionState{}, err
	}
	previous, err := managedConfigFileState(response.GetPreviousSha256())
	if err != nil {
		return agent.ManagedConfigTransactionState{}, err
	}
	state := agent.ManagedConfigTransactionState{Live: live, Previous: previous}
	if !managedConfigStateMatches(previous, request.GetExpectedPreviousSha256()) {
		return agent.ManagedConfigTransactionState{}, errs.New(
			errs.KindStateConflict,
			"agent: managed-config helper predecessor proof changed",
		)
	}
	expectedLive := request.GetSha256()
	if request.GetOperation() == agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK {
		expectedLive = request.GetExpectedPreviousSha256()
	}
	if !managedConfigStateMatches(live, expectedLive) {
		return agent.ManagedConfigTransactionState{}, errs.New(
			errs.KindStateConflict,
			"agent: managed-config helper live proof changed",
		)
	}
	return state, nil
}

func managedConfigFileState(digest []byte) (agent.ManagedConfigFileState, error) {
	if len(digest) == 0 {
		return agent.ManagedConfigFileState{}, nil
	}
	if len(digest) != sha256.Size {
		return agent.ManagedConfigFileState{}, errs.New(
			errs.KindStateConflict,
			"agent: managed-config helper digest proof is invalid",
		)
	}
	state := agent.ManagedConfigFileState{Present: true}
	copy(state.SHA256[:], digest)
	return state, nil
}

func managedConfigStateMatches(state agent.ManagedConfigFileState, digest []byte) bool {
	if len(digest) == 0 {
		return !state.Present
	}
	return len(digest) == sha256.Size && state.Present &&
		subtle.ConstantTimeCompare(state.SHA256[:], digest) == 1
}

func managedConfigRequest(
	assignment agent.Assignment,
	action *agentpb.ComponentApply,
	relativePath string,
	operation agentpb.ManagedConfigOperation,
) *agentpb.ManagedConfigHelperRequest {
	digest := sha256.New()
	for _, value := range []string{
		assignment.OperationID, assignment.TaskID, action.GetComponentId(), action.GetArtifactId(),
		strconv.FormatUint(action.GetGeneration(), 10),
	} {
		_, _ = digest.Write([]byte(value))
		_, _ = digest.Write([]byte{0})
	}
	return &agentpb.ManagedConfigHelperRequest{
		Schema: managedconfighelper.SchemaVersion, ArtifactId: action.GetArtifactId(),
		RelativePath: relativePath, Sha256: append([]byte(nil), action.GetArtifactDigest()...),
		ExpectedPreviousSha256: append([]byte(nil), action.GetExpectedPreviousArtifactDigest()...),
		Operation:              operation, TransactionId: "mct_" + hex.EncodeToString(digest.Sum(nil)),
		Generation: action.GetGeneration(),
	}
}

func dnsResolverObservationRequest(
	assignment agent.Assignment,
	action *agentpb.ComponentApply,
	recipe registeredcatalog.DNSResolverObservationRecipe,
) (dnsresolverobserver.Request, error) {
	artifact := componentObservationComposeArtifact(assignment.Plan)
	if artifact == nil {
		return dnsresolverobserver.Request{}, errs.New(
			errs.KindValidationFailed,
			"agent: DNS resolver observation artifact is invalid",
		)
	}
	if artifact.GetProjectName() == "" || len(artifact.GetServices()) != 1 {
		return dnsresolverobserver.Request{}, errs.New(
			errs.KindValidationFailed,
			"agent: DNS resolver observation target is invalid",
		)
	}
	service := artifact.GetServices()[0]
	if service.GetComposeName() != recipe.ServiceName() || service.GetServiceId() == "" {
		return dnsresolverobserver.Request{}, errs.New(
			errs.KindValidationFailed,
			"agent: DNS resolver observation Service is invalid",
		)
	}
	labels := make(map[string]string, len(service.GetExpectedLabels()))
	for _, label := range service.GetExpectedLabels() {
		if label == nil || label.GetKey() == "" {
			return dnsresolverobserver.Request{}, errs.New(
				errs.KindValidationFailed,
				"agent: DNS resolver ownership label is invalid",
			)
		}
		labels[label.GetKey()] = label.GetValue()
	}
	var digest [sha256.Size]byte
	copy(digest[:], action.GetArtifactDigest())
	var imageIndexDigest [sha256.Size]byte
	copy(imageIndexDigest[:], service.GetImageIndexDigest())
	return dnsresolverobserver.Request{
		ComponentID: action.GetComponentId(), ServiceID: service.GetServiceId(), ArtifactID: action.GetArtifactId(),
		ArtifactSHA256: digest, RenderGeneration: action.GetGeneration(), ProjectName: artifact.GetProjectName(),
		ServiceName: recipe.ServiceName(), ArtifactTarget: recipe.ArtifactTarget(),
		ImageReference: selectedImageReference(recipe.Image()), ListenEndpoint: recipe.ListenEndpoint(),
		ImageRepository: service.GetImageRepository(), ImageIndexDigest: imageIndexDigest,
		ImageOS: service.GetImageOs(), ImageArchitecture: service.GetImageArchitecture(),
		ImageVariant: service.GetImageVariant(),
		MetricsURL:   recipe.MetricsURL(), ReloadMetric: recipe.ReloadMetric(), ExpectedLabels: labels,
	}, nil
}

func componentObservationComposeArtifact(plan *agentpb.ExecutionPlan) *agentpb.ComposeArtifact {
	if plan == nil {
		return nil
	}
	if len(plan.GetArtifacts()) == 1 {
		return plan.GetArtifacts()[0]
	}
	if len(plan.GetArtifacts()) != 2 {
		return nil
	}
	for _, step := range plan.GetSteps() {
		if apply := step.GetComposeApply(); apply != nil {
			for _, artifact := range plan.GetArtifacts() {
				if artifact.GetArtifactId() == apply.GetArtifactId() {
					return artifact
				}
			}
		}
	}
	return nil
}

func selectedImageReference(image component.OCIImage) string {
	variant := ""
	if runtime.GOARCH == "arm64" {
		variant = "v8"
	}
	_, reference, _ := image.Select(runtime.GOOS, runtime.GOARCH, variant)
	return reference
}

func closeComponentArtifact(source io.ReadCloser, operationErr error) error {
	if source == nil {
		return operationErr
	}
	return errs.Wrap(errs.KindInternal, errors.Join(operationErr, source.Close()))
}
