package taskplanning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	"github.com/AlanD20/groundplane/internal/common/envoyrouter"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// PrepareComponentWorkAction binds the provider operation to its captured
// configuration and retained predecessor.
func (resolver *ComponentConfigurationCapture) PrepareComponentWorkAction(ctx context.Context,
	input EnvironmentManagedConfigApplyInput, capture runtimeconfiguration.WorkCapture,
	prior runtimeconfiguration.Snapshot, predecessorKind core.ComponentKind,
) (*agentpb.ComponentApply, error) {
	if resolver == nil || ctx == nil || capture.Validate() != nil || capture.Generation != input.RenderGeneration ||
		capture.Current.EnvironmentID != input.Artifact.GetOwnerId() {
		return nil, errs.New(errs.KindStateConflict, "Component action has no matching captured configuration")
	}
	resolved, found, err := ResolveEnvironmentManagedConfigApply(input)
	if err != nil || !found {
		return nil, err
	}
	action := proto.Clone(resolved.action).(*agentpb.ComponentApply)
	activation, destination, implementation, err := containerActivation(resolver.componentCatalog, action)
	if err != nil {
		return nil, err
	}
	if activation == registeredcatalog.ContainerConfigActivationCommand {
		return action, nil
	}
	if activation != registeredcatalog.ContainerConfigActivationHTTPDiscovery {
		return nil, errs.New(errs.KindStateConflict, "Component configuration activation is unsupported")
	}
	var forward *taskmaterialization.Record
	for index := range input.Materializations {
		file := &input.Materializations[index]
		if file.MaterializationID != action.ArtifactId {
			continue
		}
		if forward != nil || !componentWorkFileOwner(*file, capture.Current.EnvironmentID, action.ComponentId, destination) {
			return nil, errs.New(errs.KindStateConflict, "Component activation file ownership is ambiguous")
		}
		forward = file
	}
	if forward == nil {
		return nil, errs.New(errs.KindStateConflict, "Component activation file was not captured")
	}
	desired, err := resolver.contents.Load(ctx, *forward, input.RenderGeneration)
	if err != nil {
		return nil, err
	}
	defer clear(desired)
	desiredDigest := sha256.Sum256(desired)
	if !bytes.Equal(desiredDigest[:], action.ArtifactDigest) {
		return nil, errs.New(errs.KindStateConflict, "Component activation content differs from its digest")
	}
	if _, err := envoyrouter.Read(desired); err != nil {
		return nil, err
	}
	transition := &agentpb.ContainerConfigTransition{DesiredSource: append([]byte(nil), desired...)}
	if predecessorKind != implementation {
		action.ContainerConfigTransition = transition
		return action, nil
	}
	if capture.Prior == nil || prior.ID != capture.Prior.ID || prior.EnvironmentID != capture.Prior.EnvironmentID || prior.Generation != capture.Prior.Generation {
		return nil, errs.New(errs.KindStateConflict, "Serving Component has no captured previous configuration")
	}
	for _, file := range prior.Files {
		if file.Destination != destination {
			continue
		}
		if !componentWorkFileOwner(file, capture.Current.EnvironmentID, action.ComponentId, destination) {
			return nil, errs.New(errs.KindStateConflict, "Previous Component configuration belongs to another owner")
		}
		content, err := resolver.contents.Load(ctx, file, prior.Generation)
		if err != nil {
			return nil, err
		}
		defer clear(content)
		digest := sha256.Sum256(content)
		if hex.EncodeToString(digest[:]) != file.SHA256 {
			return nil, errs.New(errs.KindStateConflict, "Previous Component content differs from captured file")
		}
		if _, err := envoyrouter.PrepareActivation(content, desired); err != nil {
			return nil, err
		}
		transition.PreviousSource, transition.PreviousSourceSha256 = append([]byte(nil), content...), append([]byte(nil), digest[:]...)
		action.ContainerConfigTransition = transition
		return action, nil
	}
	return nil, errs.New(errs.KindStateConflict, "Captured previous Component configuration is missing")
}

func componentWorkFileOwner(file taskmaterialization.Record, environmentID, componentID, destination string) bool {
	return file.EnvironmentID == environmentID && file.Destination == destination &&
		file.Source.Kind == taskmaterialization.SourceComponentFile && file.Source.ComponentFile != nil &&
		file.Source.ComponentFile.ComponentID == componentID
}
