package taskplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	"github.com/AlanD20/groundplane/internal/common/envoyrouter"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareComponentWorkPredecessorConfiguration captures the serving provider
// source even when the forward action disables or replaces that provider.
func (resolver *ComponentConfigurationCapture) PrepareComponentWorkPredecessorConfiguration(ctx context.Context,
	componentID string, kind core.ComponentKind, artifact *agentpb.ComposeArtifact,
	capture runtimeconfiguration.WorkCapture, prior runtimeconfiguration.Snapshot,
) (*agentpb.ComponentWorkRetainedConfiguration, error) {
	if resolver == nil || ctx == nil || artifact == nil || capture.Validate() != nil ||
		artifact.OwnerId != capture.Current.EnvironmentID {
		return nil, errs.New(
			errs.KindStateConflict,
			"Component recovery requires its captured runtime and configuration",
		)
	}
	for _, registration := range resolver.componentCatalog {
		managed := registration.ManagedConfiguration
		if registration.Kind != kind || managed == nil {
			continue
		}
		if capture.Prior == nil || prior.ID != capture.Prior.ID || prior.EnvironmentID != capture.Prior.EnvironmentID ||
			prior.Generation != capture.Prior.Generation {
			return nil, errs.New(
				errs.KindStateConflict,
				"Serving discovery Component has no retained configuration snapshot",
			)
		}
		for _, file := range prior.Files {
			if file.Destination != managed.SourcePath {
				continue
			}
			if !componentWorkFileOwner(file, prior.EnvironmentID, componentID, managed.SourcePath) {
				return nil, errs.New(errs.KindStateConflict, "Component recovery source belongs to another owner")
			}
			content, err := resolver.contents.Load(ctx, file, prior.Generation)
			if err != nil {
				return nil, err
			}
			defer clear(content)
			digest := sha256.Sum256(content)
			if hex.EncodeToString(digest[:]) != file.SHA256 {
				return nil, errs.New(errs.KindStateConflict, "Component recovery source differs from retained bytes")
			}
			if managed.Activation == registeredcatalog.ContainerConfigActivationHTTPDiscovery {
				if _, err := envoyrouter.Read(content); err != nil {
					return nil, err
				}
			}
			definition := registration.Definition.Digest()
			return &agentpb.ComponentWorkRetainedConfiguration{
				File: &agentpb.MaterializeFile{
					ArtifactId:        artifact.ArtifactId,
					MaterializationId: file.MaterializationID,
					EnvironmentId:     file.EnvironmentID,
					Destination:       file.Destination,
					ServiceId:         file.ServiceID,
					ServiceName:       file.ServiceName,
					OutputKind:        agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE,
					Uid:               file.UID,
					Gid:               file.GID,
					Mode:              file.Mode,
					Length:            file.Length,
					Sha256:            append([]byte(nil), digest[:]...),
				},
				DefinitionDigest: append(
					[]byte(nil),
					definition[:]...), CatalogDigest: append([]byte(nil), registration.CatalogDigest[:]...),
				ActionId: string(
					managed.ActionID,
				), Source: append([]byte(nil), content...), Generation: prior.Generation,
			}, nil
		}
		return nil, errs.New(
			errs.KindStateConflict,
			"Component recovery configuration is missing from its retained snapshot",
		)
	}
	return nil, nil
}
