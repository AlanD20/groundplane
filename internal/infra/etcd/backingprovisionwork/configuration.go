package backingprovisionwork

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// LoadConfiguration follows the immutable file-source snapshot, never desired
// Entries or latest Secret values. The caller must first authorize this exact
// attempt's acknowledged WriteConfiguration boundary before delivering content.
func (repository *Repository) LoadConfiguration(
	ctx context.Context,
	view workqueue.View,
) ([]taskmaterialization.Record, error) {
	input, _, err := repository.Load(ctx, view)
	if err != nil {
		return nil, err
	}
	defer clear(input.Wire)
	procedure, err := input.Procedure()
	if err != nil {
		return nil, err
	}
	configuration := procedure.Configuration
	reference := runtimeconfiguration.Reference{
		ID:            configuration.ConfigurationId,
		EnvironmentID: configuration.EnvironmentId,
		Generation:    configuration.Generation,
		SHA256:        hex.EncodeToString(configuration.Sha256),
	}
	sources, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return nil, err
	}
	snapshot, err := sources.Load(ctx, reference, view.ReadRevision)
	if err != nil {
		return nil, err
	}
	if err := validateConfigurationSources(procedure, snapshot.Files); err != nil {
		return nil, err
	}
	return taskmaterialization.Clone(snapshot.Files), nil
}

func validateConfigurationSources(procedure *agentpb.BackingProvisionInput, files []taskmaterialization.Record) error {
	if len(files) != len(procedure.Files) || len(files) > 1 {
		return invalid("backing creation file sources differ from its captured manifest")
	}
	stageID, err := backingprovision.StageID(procedure.WorkItemId, backingprovision.WriteConfiguration)
	if err != nil {
		return err
	}
	for index, file := range files {
		metadata := procedure.Files[index]
		if metadata == nil || taskmaterialization.ValidateRecord(file, procedure.RenderGeneration) != nil ||
			file.StepID != stageID ||
			file.MaterializationID != metadata.MaterializationId ||
			file.EnvironmentID != metadata.EnvironmentId ||
			file.ServiceID != metadata.ServiceId ||
			file.ServiceName != metadata.ServiceName ||
			file.Destination != metadata.Destination ||
			file.UID != metadata.Uid ||
			file.GID != metadata.Gid ||
			file.Mode != metadata.Mode ||
			file.Length != metadata.Length ||
			file.SHA256 != hex.EncodeToString(metadata.Sha256) ||
			file.OutputKind != taskmaterialization.OutputGeneratedEnvironment ||
			file.Source.Kind != taskmaterialization.SourceGeneratedEnvironment {
			return invalid("backing creation file source does not match its acknowledged output metadata")
		}
	}
	return nil
}
