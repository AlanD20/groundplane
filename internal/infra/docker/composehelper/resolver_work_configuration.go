package composehelper

import (
	"bytes"
	"context"
	"encoding/hex"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelper"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelpercontainer"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func resolverFileRequest(
	input resolverexecution.HelperInput,
	catalog ResolverCatalog,
	operation agentpb.ManagedConfigOperation,
) (*agentpb.ManagedConfigHelperRequest, registeredcatalog.ManagedConfigActionRecipe, error) {
	configuration := input.Assignment.Input.Candidate
	if configuration == nil {
		return nil, registeredcatalog.ManagedConfigActionRecipe{}, errs.New(
			errs.KindValidationFailed,
			"CoreDNS file operation has no captured candidate",
		)
	}
	recipe, err := catalog.ResolveManagedResolverAction(configuration.Apply)
	if err != nil {
		return nil, registeredcatalog.ManagedConfigActionRecipe{}, err
	}
	forward := input.Assignment.Attempt
	if input.Assignment.Recovery != nil {
		forward = input.Assignment.Recovery.Forward
	}
	transactionID, err := resolverexecution.FileTransactionID(forward, *configuration)
	if err != nil {
		return nil, registeredcatalog.ManagedConfigActionRecipe{}, err
	}
	request := &agentpb.ManagedConfigHelperRequest{
		Schema:                 managedconfighelper.SchemaVersion,
		ArtifactId:             configuration.Apply.ArtifactId,
		RelativePath:           recipe.RelativePath(),
		Sha256:                 append([]byte(nil), configuration.Apply.ArtifactDigest...),
		ExpectedPreviousSha256: append([]byte(nil), configuration.Apply.ExpectedPreviousArtifactDigest...),
		Operation:              operation,
		TransactionId:          transactionID,
		Generation:             configuration.Generation,
	}
	return request, recipe, nil
}

func writeResolverConfiguration(
	ctx context.Context,
	input resolverexecution.HelperInput,
	catalog ResolverCatalog,
) error {
	request, recipe, err := resolverFileRequest(
		input,
		catalog,
		agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_PUBLISH,
	)
	if err != nil {
		return err
	}
	configuration := input.Assignment.Input.Candidate
	service := configuration.Runtime.Services[0]
	platform, reference, found := recipe.Image().Select(service.ImageOs, service.ImageArchitecture)
	if !found || reference != service.ImageReference ||
		hex.EncodeToString(service.ImageConfigDigest) != platform.ConfigDigest ||
		hex.EncodeToString(service.ImageChildDigest) != platform.ChildDigest {
		return errs.New(errs.KindValidationFailed, "CoreDNS file validator does not match the captured runtime image")
	}
	validator, err := managedconfighelpercontainer.New(reference)
	if err != nil {
		return err
	}
	defer validator.Close()
	if err := validator.Validate(ctx, managedconfighelpercontainer.ValidatorImage{Reference: reference, Platform: platform}, recipe.ValidateArgs(), configuration.Corefile); err != nil {
		return err
	}
	request.Content = append([]byte(nil), configuration.Corefile...)
	defer clear(request.Content)
	response, err := managedconfighelper.ApplyNative(ctx, request, input.Assignment.Input.PreviousFileOwner)
	if err != nil {
		return err
	}
	return validateResolverFileResponse(request, response, request.Sha256)
}

func finalizeResolverConfiguration(
	ctx context.Context,
	input resolverexecution.HelperInput,
	catalog ResolverCatalog,
) error {
	request, _, err := resolverFileRequest(
		input,
		catalog,
		agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_COMMIT,
	)
	if err != nil {
		return err
	}
	response, err := managedconfighelper.ApplyNative(ctx, request, input.Assignment.Input.PreviousFileOwner)
	if err != nil {
		return err
	}
	return validateResolverFileResponse(request, response, request.Sha256)
}

func resolverConfigurationCommitted(
	ctx context.Context,
	input resolverexecution.HelperInput,
	catalog ResolverCatalog,
) (bool, error) {
	request, _, err := resolverFileRequest(
		input,
		catalog,
		agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_COMMIT,
	)
	if err != nil {
		return false, err
	}
	return managedconfighelper.InspectNativeCommit(ctx, request)
}

func restoreResolverConfiguration(
	ctx context.Context,
	input resolverexecution.HelperInput,
	catalog ResolverCatalog,
) error {
	configuration := input.Assignment.Input
	if configuration.Candidate != nil {
		request, _, err := resolverFileRequest(
			input,
			catalog,
			agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK,
		)
		if err != nil {
			return err
		}
		response, err := managedconfighelper.RestoreNative(ctx, request, configuration.PreviousFileOwner)
		if err != nil {
			return err
		}
		if err := validateResolverFileResponse(request, response, request.ExpectedPreviousSha256); err != nil {
			return err
		}
	}
	if previous := configuration.Previous; previous != nil {
		recipe, err := catalog.ResolveManagedResolverAction(previous.Apply)
		if err != nil {
			return err
		}
		state, err := managedconfighelper.InspectNative(ctx, recipe.RelativePath())
		if err != nil {
			return err
		}
		if !state.Present || state.Owner != configuration.PreviousFileOwner ||
			!bytes.Equal(state.SHA256[:], previous.Apply.ArtifactDigest) {
			return errs.New(
				errs.KindStateConflict,
				"CoreDNS restoration did not restore the captured file and its owner",
			)
		}
	}
	return nil
}

func validateResolverFileResponse(
	request *agentpb.ManagedConfigHelperRequest,
	response *agentpb.ManagedConfigHelperResponse,
	expectedLive []byte,
) error {
	if response == nil || response.Schema != managedconfighelper.SchemaVersion ||
		response.TransactionId != request.TransactionId ||
		response.Operation != request.Operation ||
		response.Disposition == agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_UNSPECIFIED ||
		!bytes.Equal(response.LiveSha256, expectedLive) ||
		!bytes.Equal(response.PreviousSha256, request.ExpectedPreviousSha256) {
		return errs.New(
			errs.KindStateConflict,
			"CoreDNS managed-file operation did not return its exact captured file proof",
		)
	}
	return nil
}
