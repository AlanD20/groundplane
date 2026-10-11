package taskplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	"github.com/AlanD20/groundplane/internal/common/envoyrouter"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/controller/componentrender"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A predecessor without a forward activation uses its exact captured file:
// either a real restoration step or a read-only, untouched snapshot member.
func (resolver *TaskPlanResolver) bindRetainedComponentConfiguration(
	ctx context.Context, task etcd.TaskRecord, procedure *agentpb.ComponentRecoveryProcedure,
	prior *agentpb.ManagedComponentService,
) error {
	for _, registration := range resolver.componentCatalog {
		managed := registration.ManagedConfiguration
		if registration.Kind != core.ComponentKind(prior.GetComponentKind()) || managed == nil ||
			managed.Activation != registeredcatalog.ContainerConfigActivationHTTPDiscovery {
			continue
		}
		prepared, err := resolver.configurationRecovery.Prepare(ctx, task)
		if err != nil {
			return err
		}
		var selected *agentpb.ComponentRetainedConfiguration
		for _, file := range prepared.Files {
			original := file.Original
			if original.Destination != managed.SourcePath {
				continue
			}
			if selected != nil || original.EnvironmentID != task.Owner.EnvironmentID ||
				original.Source.Kind != taskmaterialization.SourceComponentFile || original.Source.ComponentFile == nil ||
				original.Source.ComponentFile.ComponentID != procedure.GetComponentId() {
				return errs.New(errs.KindStateConflict, "router recovery file ownership is missing or ambiguous")
			}
			selected, err = resolver.retainedComponentConfiguration(ctx, task, original, registration)
			if err != nil {
				return err
			}
			selected.FileSource = &agentpb.ComponentRetainedConfiguration_MaterializeStepId{
				MaterializeStepId: file.Compensate.StepID,
			}
		}
		if selected == nil {
			original, err := resolver.configurationRecovery.UnchangedComponentFile(ctx, task,
				procedure.GetComponentId(), managed.SourcePath)
			if err != nil {
				return err
			}
			selected, err = resolver.retainedComponentConfiguration(ctx, task, original, registration)
			if err != nil {
				return err
			}
			digest, err := hex.DecodeString(original.SHA256)
			if err != nil {
				return errs.New(errs.KindStateConflict, "retained Component file digest is invalid")
			}
			snapshotDigest, err := hex.DecodeString(task.Configuration.Prior.SHA256)
			if err != nil {
				return errs.New(errs.KindStateConflict, "retained Component snapshot digest is invalid")
			}
			selected.FileSource = &agentpb.ComponentRetainedConfiguration_UnchangedFile{
				UnchangedFile: &agentpb.ComponentUnchangedConfigurationFile{
					MaterializationId: original.MaterializationID, EnvironmentId: original.EnvironmentID,
					Destination: original.Destination, Sha256: digest,
					PriorSnapshotId: task.Configuration.Prior.ID, PriorSnapshotSha256: snapshotDigest,
					PriorSnapshotGeneration: task.Configuration.Prior.Generation,
				},
			}
		}
		procedure.ConfigurationSource = &agentpb.ComponentRecoveryProcedure_RetainedConfiguration{
			RetainedConfiguration: selected,
		}
		return nil
	}
	return nil
}

func (resolver *TaskPlanResolver) retainedComponentConfiguration(ctx context.Context, task etcd.TaskRecord,
	original taskmaterialization.Record, registration componentrender.EnvironmentComponentRegistration,
) (*agentpb.ComponentRetainedConfiguration, error) {
	content, err := resolver.loadComponentMaterialization(ctx, original, uint64(task.RenderGeneration))
	if err != nil {
		return nil, err
	}
	if _, err := envoyrouter.Read(content); err != nil {
		clear(content)
		return nil, err
	}
	digest := sha256.Sum256(content)
	if original.SHA256 != hex.EncodeToString(digest[:]) {
		clear(content)
		return nil, errs.New(errs.KindStateConflict, "router recovery bytes differ from the retained file")
	}
	definition := registration.Definition.Digest()
	return &agentpb.ComponentRetainedConfiguration{
		DefinitionDigest: append([]byte(nil), definition[:]...),
		CatalogDigest:    append([]byte(nil), registration.CatalogDigest[:]...),
		ActionId:         string(registration.ManagedConfiguration.ActionID), Source: content,
		Generation: uint64(task.RenderGeneration),
	}, nil
}
