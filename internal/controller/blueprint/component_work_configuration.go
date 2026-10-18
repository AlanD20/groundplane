package blueprint

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (service *Service) captureComponentWorkConfiguration(ctx context.Context, work workqueue.View,
	placement componentWorkPlacement, files []taskmaterialization.Record,
) (runtimeconfiguration.WorkCapture, error) {
	configurationID := ids.DeriveAt(ids.KindConfig, work.Item.Item.CreatedAt, work.Item.Item.ID, "component-configuration")
	return service.repository.PrepareComponentWorkConfiguration(ctx, work, configurationID,
		placement.Selection.Source.desired.Record.RenderGeneration, files)
}

func componentWorkConfigurationReference(reference runtimeconfiguration.Reference) (*agentpb.WorkConfigurationReference, error) {
	if err := runtimeconfiguration.ValidateReference(reference); err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.WorkConfigurationReference{ConfigurationId: reference.ID,
		EnvironmentId: reference.EnvironmentID, Generation: reference.Generation, Sha256: digest}, nil
}
