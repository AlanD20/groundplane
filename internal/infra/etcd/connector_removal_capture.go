package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/connectormutations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *ConnectorRepository) CaptureConnectorRemoval(ctx context.Context,
	environment keyvalue.Versioned[hierarchy.EnvironmentRecord], project keyvalue.Versioned[hierarchy.ProjectRecord],
	current keyvalue.Versioned[connectors.Record], taskID, workID, planID string, at time.Time,
) (connectorremoval.Input, error) {
	if err := connectormutations.ValidateConnectorHierarchy(ctx, environment, project, current.Record); err != nil {
		return connectorremoval.Input{}, err
	}
	if err := connectors.ValidateConnectorVersion(current); err != nil {
		return connectorremoval.Input{}, err
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{connectors.CredentialValueKey(current.Record.Connector.ID)}, Revision: current.ReadRevision})
	if err != nil {
		return connectorremoval.Input{}, err
	}
	if read == nil {
		return connectorremoval.Input{}, errs.New(errs.KindInternal, "Connector credential capture is missing")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != current.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return connectorremoval.Input{}, errs.New(
			errs.KindStateConflict,
			"Connector credentials are unavailable for removal",
		)
	}
	credentials, err := connectors.DecodeEncryptedCredentials(read.Values[0].Value)
	defer clear(credentials.Ciphertext)
	if err != nil {
		return connectorremoval.Input{}, err
	}
	if credentials.ConnectorID != current.Record.Connector.ID {
		return connectorremoval.Input{}, errs.New(
			errs.KindStateConflict,
			"Connector credentials belong to another resource",
		)
	}
	input := connectorremoval.Input{TaskID: taskID, WorkItemID: workID, PlanID: planID,
		TenantID: project.Record.TenantID, ProjectID: project.Record.ID, EnvironmentID: environment.Record.ID,
		ConnectorID: current.Record.Connector.ID, ConnectorRevision: current.Revision,
		CredentialRevision: read.Values[0].ModRevision, CreatedAt: at}
	if err := input.Validate(); err != nil {
		return connectorremoval.Input{}, err
	}
	return input, nil
}
