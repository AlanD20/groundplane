package softwarepreparation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/infra/registryimages"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Prepare builds or imports the already-resolved source and publishes immutable
// artifacts to the managed registry. Checkpoints are reported immediately after
// each registry readback so a restarted durable Task can skip finished outputs.
// It never stages or activates either component.
func (preparer *Preparer) Prepare(
	ctx context.Context,
	input Input,
	checkpoint Result,
	reporter Reporter,
) (result Result, returnErr error) {
	if reporter == nil {
		return Result{}, errs.New(errs.KindInternal, "software preparation progress reporter is required")
	}
	if err := ValidateInput(input); err != nil {
		return Result{}, err
	}
	if err := ValidateResult(checkpoint, input.Source, true); err != nil {
		return Result{}, err
	}
	if checkpoint.Source.Selection == "" {
		checkpoint.Source = input.Source
	}
	result = checkpoint
	if err := preparer.verifyCheckpoint(ctx, result); err != nil {
		return result, err
	}
	if err := reporter(ctx, Progress{Phase: PhasePreparing, Result: result}); err != nil {
		return result, err
	}
	if err := checkWorkspaceCapacity(preparer.workspaceRoot); err != nil {
		return result, err
	}
	workspace := filepath.Join(preparer.workspaceRoot, ".software-preparation-"+input.OperationID)
	if info, statErr := os.Lstat(workspace); statErr == nil {
		status, statusOK := info.Sys().(*syscall.Stat_t)
		if !info.IsDir() || info.Mode().Perm() != 0o700 || !statusOK || status.Uid != uint32(os.Geteuid()) {
			return result, errs.New(errs.KindStateConflict, "stale software preparation workspace is unsafe")
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		cleanupErr := staleDockerOperation(preparer, workspace, input.OperationID, input.Source).cleanup(cleanupCtx)
		cancel()
		if cleanupErr != nil {
			return result, errs.Wrap(errs.KindRequestUnavailable, cleanupErr)
		}
		if err := os.RemoveAll(workspace); err != nil {
			return result, errs.Wrap(errs.KindRequestUnavailable, err)
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return result, errs.Wrap(errs.KindInternal, statErr)
	}
	if err := os.Mkdir(workspace, 0o700); err != nil {
		return result, errs.Wrap(errs.KindInternal, err)
	}
	operation, err := newDockerOperation(preparer, workspace, input.OperationID)
	if err != nil {
		cleanupErr := os.RemoveAll(workspace)
		if cleanupErr != nil {
			return result, errs.WrapJoined(errs.KindRequestUnavailable, cleanupErr, err)
		}
		return result, err
	}
	cleanupAttempted := false
	cleanup := func() error {
		cleanupAttempted = true
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		if err := operation.cleanup(cleanupCtx); err != nil {
			return err
		}
		if err := os.RemoveAll(workspace); err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		return nil
	}
	defer func() {
		if !cleanupAttempted {
			if cleanupErr := cleanup(); cleanupErr != nil {
				returnErr = errs.WrapJoined(errs.KindRequestUnavailable, cleanupErr, returnErr)
			}
		}
	}()

	var agentTag, agentUpstream, controllerTag string
	var controllerIdentity controllerDigests
	if input.Source.SourceKind == SourceRef {
		agentTag, controllerTag, controllerIdentity, err = operation.prepareSource(
			ctx, input,
			input.Source.Selection.includesAgent() && result.Agent == nil,
			input.Source.Selection.includesController() && result.Controller == nil,
		)
	} else {
		agentTag, agentUpstream, controllerTag, controllerIdentity, err = operation.prepareRelease(
			ctx, input,
			input.Source.Selection.includesAgent() && result.Agent == nil,
			input.Source.Selection.includesController() && result.Controller == nil,
		)
	}
	if err != nil {
		return result, err
	}

	if input.Source.Selection.includesAgent() && result.Agent == nil {
		artifact, err := operation.publish(ctx, agentTag, agentRepository, input.Source, SelectionAgent)
		if err != nil {
			return result, err
		}
		result.Agent = &AgentResult{Artifact: artifact, UpstreamReference: agentUpstream}
		if err := reporter(ctx, Progress{Phase: PhaseAgentPublished, Result: result}); err != nil {
			return result, err
		}
	}
	if input.Source.Selection.includesController() && result.Controller == nil {
		artifact, err := operation.publish(
			ctx, controllerTag, controllerRepository, input.Source, SelectionController,
		)
		if err != nil {
			return result, err
		}
		result.Controller = &ControllerResult{
			Artifact: artifact, BinarySHA256: controllerIdentity.binary,
			MetadataSHA256: controllerIdentity.metadata, CLISHA256: controllerIdentity.cli,
		}
		if err := reporter(ctx, Progress{Phase: PhaseControllerPublished, Result: result}); err != nil {
			return result, err
		}
	}
	if err := ValidateResult(result, input.Source, false); err != nil {
		return result, err
	}
	if err := cleanup(); err != nil {
		return result, errs.Wrap(errs.KindRequestUnavailable, err)
	}
	if err := preparer.verifyCheckpoint(ctx, result); err != nil {
		return result, err
	}
	if err := reporter(ctx, Progress{Phase: PhaseVerified, Result: result}); err != nil {
		return result, err
	}
	return result, nil
}

func ValidateInput(input Input) error {
	if !operationPattern.MatchString(input.OperationID) {
		return errs.New(errs.KindValidationFailed, "software preparation operation identity is invalid")
	}
	if err := input.Source.validate(); err != nil {
		return err
	}
	needsCatalog := input.Source.SourceKind == SourceRef && input.Source.Selection.includesController()
	if !needsCatalog {
		if len(input.ControllerToolCatalog) != 0 || input.ControllerCatalogSHA256 != "" {
			return errs.New(errs.KindValidationFailed, "software preparation input has an inapplicable tool catalog")
		}
	} else {
		if len(input.ControllerToolCatalog) == 0 || len(input.ControllerToolCatalog) > maximumCatalogBytes ||
			input.ControllerCatalogSHA256 != sha256Value(input.ControllerToolCatalog) {
			return errs.New(errs.KindValidationFailed, "controller tool catalog identity is invalid")
		}
		if _, err := postgres16protocol.DecodeManagedReleaseIndex(input.ControllerToolCatalog); err != nil {
			return errs.New(errs.KindValidationFailed, "controller tool catalog is invalid")
		}
	}
	if input.Source.SourceKind == Release {
		for component, release := range map[Selection]*ResolvedRelease{
			SelectionController: input.Source.ControllerRelease,
			SelectionAgent:      input.Source.AgentRelease,
		} {
			if release == nil {
				continue
			}
			required := requiredReleaseAssets(component, release.OriginalRef, input.Source.Platform)
			if len(release.Assets) != len(required) {
				return errs.New(errs.KindValidationFailed, "published release asset authority is incomplete")
			}
			seen := make(map[string]struct{}, len(release.Assets))
			for _, asset := range release.Assets {
				maximum, ok := required[asset.Name]
				if !ok || asset.Size > maximum {
					return errs.New(errs.KindValidationFailed, "published release asset authority is unexpected")
				}
				if _, duplicate := seen[asset.Name]; duplicate {
					return errs.New(errs.KindValidationFailed, "published release asset authority is duplicated")
				}
				seen[asset.Name] = struct{}{}
			}
		}
	}
	return nil
}

func ValidateResult(result Result, source ResolvedSource, partial bool) error {
	if result.Source.Selection == "" {
		if result.Controller != nil || result.Agent != nil || !partial {
			return errs.New(errs.KindValidationFailed, "software preparation result source is missing")
		}
		return nil
	}
	if !result.Source.Equal(source) {
		return errs.New(errs.KindValidationFailed, "software preparation result source changed")
	}
	if !source.Selection.includesController() && result.Controller != nil ||
		!source.Selection.includesAgent() && result.Agent != nil {
		return errs.New(errs.KindValidationFailed, "software preparation result contains another component")
	}
	if result.Controller != nil {
		if err := validateArtifact(result.Controller.Artifact, controllerRepository); err != nil {
			return err
		}
		if !digestPattern.MatchString(result.Controller.BinarySHA256) ||
			!digestPattern.MatchString(result.Controller.MetadataSHA256) ||
			!digestPattern.MatchString(result.Controller.CLISHA256) {
			return errs.New(errs.KindValidationFailed, "prepared Controller bundle identities are invalid")
		}
	}
	if result.Agent != nil {
		if err := validateArtifact(result.Agent.Artifact, agentRepository); err != nil {
			return err
		}
		if source.SourceKind == SourceRef && result.Agent.UpstreamReference != "" {
			return errs.New(errs.KindValidationFailed, "source-built Agent cannot name an upstream image")
		}
		if source.SourceKind == Release &&
			(!strings.HasPrefix(result.Agent.UpstreamReference, "ghcr.io/aland20/groundplane-agent@sha256:") ||
				!digestPattern.MatchString(result.Agent.UpstreamReference[strings.LastIndexByte(result.Agent.UpstreamReference, '@')+1:])) {
			return errs.New(errs.KindValidationFailed, "published Agent result identity is invalid")
		}
	}
	if !partial && (source.Selection.includesController() != (result.Controller != nil) ||
		source.Selection.includesAgent() != (result.Agent != nil)) {
		return errs.New(errs.KindStateConflict, "software preparation result is incomplete")
	}
	return nil
}

func ValidateProgress(progress Progress, source ResolvedSource) error {
	partial := progress.Phase != PhaseVerified
	if err := ValidateResult(progress.Result, source, partial); err != nil {
		return err
	}
	failed := progress.Phase == PhaseFailed
	if progress.Result.Source.Selection == "" ||
		failed && (progress.ErrorCode == "" || progress.ErrorDetail == "") ||
		!failed && (progress.ErrorCode != "" || progress.ErrorDetail != "") ||
		len(progress.ErrorCode) > 128 || len(progress.ErrorDetail) > 1024 {
		return errs.New(errs.KindValidationFailed, "software preparation progress is invalid")
	}
	switch progress.Phase {
	case PhaseAccepted:
		if progress.Result.Agent != nil || progress.Result.Controller != nil {
			return errs.New(errs.KindValidationFailed, "initial software preparation progress has outputs")
		}
	case PhasePreparing:
	case PhaseAgentPublished:
		if progress.Result.Agent == nil || progress.Result.Controller != nil {
			return errs.New(errs.KindValidationFailed, "agent publication checkpoint is invalid")
		}
	case PhaseControllerPublished:
		if progress.Result.Controller == nil ||
			(source.Selection == SelectionBoth && progress.Result.Agent == nil) {
			return errs.New(errs.KindValidationFailed, "controller publication checkpoint is invalid")
		}
	case PhaseVerified:
	case PhaseFailed:
	default:
		return errs.New(errs.KindValidationFailed, "software preparation phase is invalid")
	}
	return nil
}

func validateArtifact(artifact Artifact, repository string) error {
	prefix := imagefetch.RegistryAuthority + "/" + repository + "@"
	if artifact.Platform != HostPlatform() || !strings.HasPrefix(artifact.Reference, prefix) ||
		!digestPattern.MatchString(artifact.ManifestDigest) || !digestPattern.MatchString(artifact.ConfigDigest) ||
		artifact.Reference != prefix+artifact.ManifestDigest {
		return errs.New(errs.KindValidationFailed, "prepared registry artifact identity is invalid")
	}
	return nil
}

func (preparer *Preparer) verifyCheckpoint(ctx context.Context, result Result) error {
	artifacts := make([]Artifact, 0, 2)
	if result.Agent != nil {
		artifacts = append(artifacts, result.Agent.Artifact)
	}
	if result.Controller != nil {
		artifacts = append(artifacts, result.Controller.Artifact)
	}
	for _, artifact := range artifacts {
		if artifact.Reference == "" {
			continue
		}
		plan, err := (registryimages.Local{}).Resolve(ctx, artifact.Reference)
		if err != nil {
			return err
		}
		if plan.Reference() != artifact.Reference || plan.ManifestDigest != artifact.ManifestDigest ||
			plan.ConfigDigest != artifact.ConfigDigest || plan.Architecture != artifact.Platform.Architecture {
			return errs.New(errs.KindStateConflict, "prepared registry checkpoint readback differs")
		}
	}
	return nil
}

func checkWorkspaceCapacity(path string) error {
	var status syscall.Statfs_t
	if err := syscall.Statfs(path, &status); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	available := uint64(status.Bavail) * uint64(status.Bsize)
	if available < minimumFreeBytes {
		return errs.New(
			errs.KindStateConflict,
			"software preparation requires at least 10 GiB of free workspace capacity",
		)
	}
	return nil
}

func (operation *dockerOperation) prepareSource(
	ctx context.Context,
	input Input,
	wantAgent bool,
	wantController bool,
) (agentTag string, controllerTag string, identity controllerDigests, err error) {
	if !wantAgent && !wantController {
		return "", "", controllerDigests{}, nil
	}
	if err := operation.preparer.downloadSource(ctx, input.Source, operation.directory); err != nil {
		return "", "", controllerDigests{}, err
	}
	sourceDirectory := filepath.Join(operation.directory, "source")
	versionBytes, err := boundedFile(filepath.Join(sourceDirectory, "VERSION"), 128)
	if err != nil {
		return "", "", controllerDigests{}, err
	}
	baseVersion := strings.TrimSpace(string(versionBytes))
	if !versionPattern.MatchString(baseVersion) {
		return "", "", controllerDigests{}, errs.New(errs.KindStateConflict, "selected source VERSION is invalid")
	}
	version := baseVersion + "-ref." + input.Source.ResolvedSHA
	if wantController {
		controllerTag, identity, err = operation.buildController(
			ctx, sourceDirectory, input.Source, version, input.ControllerToolCatalog,
		)
		if err != nil {
			return "", "", controllerDigests{}, err
		}
	}
	if wantAgent {
		agentTag, err = operation.buildAgent(ctx, sourceDirectory, input.Source, version)
		if err != nil {
			return "", "", controllerDigests{}, err
		}
	}
	return agentTag, controllerTag, identity, nil
}

func (operation *dockerOperation) prepareRelease(
	ctx context.Context,
	input Input,
	wantAgent bool,
	wantController bool,
) (agentTag string, agentUpstream string, controllerTag string, identity controllerDigests, err error) {
	assetPaths := make(map[string]string, 3)
	if wantController {
		for _, asset := range input.Source.ControllerRelease.Assets {
			path := filepath.Join(operation.directory, asset.Name)
			if err := operation.preparer.downloadAsset(
				ctx, input.Source.ControllerRelease.OriginalRef, asset, path,
			); err != nil {
				return "", "", "", controllerDigests{}, err
			}
			assetPaths[asset.Name] = path
		}
	}
	if wantAgent {
		for _, asset := range input.Source.AgentRelease.Assets {
			path := filepath.Join(operation.directory, asset.Name)
			if err := operation.preparer.downloadAsset(
				ctx, input.Source.AgentRelease.OriginalRef, asset, path,
			); err != nil {
				return "", "", "", controllerDigests{}, err
			}
			assetPaths[asset.Name] = path
		}
	}
	if wantController {
		version := releaseVersion(input.Source.ControllerRelease.OriginalRef)
		bundleName := fmt.Sprintf("groundplane-%s-linux-%s.tar.gz", version, input.Source.Platform.Architecture)
		if err := verifyReleaseChecksum(assetPaths[bundleName], assetPaths[bundleName+".sha256"], bundleName); err != nil {
			return "", "", "", controllerDigests{}, err
		}
		payload := filepath.Join(operation.directory, "controller-payload")
		if err := os.Mkdir(payload, 0o700); err != nil {
			return "", "", "", controllerDigests{}, errs.Wrap(errs.KindInternal, err)
		}
		identity, err = extractControllerBundle(assetPaths[bundleName], payload, version, input.Source.Platform)
		if err != nil {
			return "", "", "", controllerDigests{}, err
		}
		controllerTag, err = operation.packageController(
			ctx, payload, input.Source, "controller/v"+version, identity,
		)
		if err != nil {
			return "", "", "", controllerDigests{}, err
		}
	}
	if wantAgent {
		version := releaseVersion(input.Source.AgentRelease.OriginalRef)
		agentUpstream, err = readAgentManifest(assetPaths["agent.json"], "agent/v"+version)
		if err != nil {
			return "", "", "", controllerDigests{}, err
		}
		agentTag, err = operation.pullAgent(ctx, agentUpstream, input.Source, "agent/v"+version)
		if err != nil {
			return "", "", "", controllerDigests{}, err
		}
	}
	return agentTag, agentUpstream, controllerTag, identity, nil
}

func verifyReleaseChecksum(bundlePath, checksumPath, bundleName string) error {
	checksum, err := boundedFile(checksumPath, 1024)
	if err != nil {
		return err
	}
	fields := strings.Fields(string(checksum))
	if len(fields) != 2 || fields[1] != bundleName || len(fields[0]) != sha256.Size*2 {
		return errs.New(errs.KindStateConflict, "controller release checksum is invalid")
	}
	expected, err := hex.DecodeString(fields[0])
	if err != nil {
		return errs.New(errs.KindStateConflict, "controller release checksum is invalid")
	}
	bundle, err := os.Open(bundlePath)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer bundle.Close()
	hasher := sha256.New()
	read, err := io.Copy(hasher, io.LimitReader(bundle, maximumBundleBytes+1))
	if err != nil || read <= 0 || read > maximumBundleBytes || !bytes.Equal(expected, hasher.Sum(nil)) {
		return errs.New(errs.KindStateConflict, "controller release checksum differs from bundle")
	}
	return nil
}

func readAgentManifest(path, expectedTag string) (string, error) {
	value, err := boundedFile(path, 64<<10)
	if err != nil {
		return "", err
	}
	var manifest struct {
		Tag   string `json:"tag"`
		Image string `json:"image"`
	}
	if err := decodeJSON(value, &manifest); err != nil || manifest.Tag != expectedTag ||
		!strings.HasPrefix(manifest.Image, "ghcr.io/aland20/groundplane-agent@sha256:") ||
		!digestPattern.MatchString(manifest.Image[strings.LastIndexByte(manifest.Image, '@')+1:]) {
		return "", errs.New(errs.KindStateConflict, "published Agent metadata is invalid")
	}
	return manifest.Image, nil
}
