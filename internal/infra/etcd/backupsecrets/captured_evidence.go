package backupsecrets

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// resolveCapturedEvidence reads only the selected resource snapshot and typed
// source authorities. Authentication and execution ownership precede this read.
func (reader *Reader) resolveCapturedEvidence(ctx context.Context, evidence Evidence,
	scope *agentpb.BackupPlanScope, sources []*agentpb.BackupStepAuthority, stepIndex int,
) (Evidence, error) {
	dynamic := newBackupSecretDynamicRead()
	environmentID, connectorIDs, sourceIDs, err := reader.planDynamicKeys(
		ctx, dynamic, evidence, scope, sources, stepIndex,
	)
	if err != nil {
		return Evidence{}, err
	}
	firstDynamic, err := reader.readFixed(ctx, dynamic.keys, evidence.ReadRevision)
	if err != nil {
		return Evidence{}, err
	}
	defer keyvalue.ClearValues(firstDynamic.Values)
	selectedConnectorID := ""
	if evidence.Run != nil {
		selectedConnectorID = evidence.Run.ConnectorID
	} else if evidence.Restore != nil {
		selectedConnectorID = evidence.Restore.Point.ConnectorID
	} else if evidence.Dispatch != nil {
		selectedConnectorID = sources[stepIndex].GetPrune().Objects[0].Object.Connector.ConnectorId
	}
	if err := reader.decodeCommonDynamicEvidence(
		firstDynamic, dynamic, &evidence, environmentID, connectorIDs, selectedConnectorID,
	); err != nil {
		evidence.Clear()
		return Evidence{}, err
	}
	if err := reader.decodeSourceDynamicEvidence(
		firstDynamic, dynamic, &evidence, sources[stepIndex], stepIndex, sourceIDs,
	); err != nil {
		evidence.Clear()
		return Evidence{}, err
	}
	if evidence.Dispatch != nil {
		if err := reader.decodePruneDynamicEvidence(
			ctx, firstDynamic, dynamic, &evidence, scope, sources, stepIndex,
		); err != nil {
			evidence.Clear()
			return Evidence{}, err
		}
	}
	if err := decodeCurrentAgeIdentityEvidence(firstDynamic, dynamic, &evidence, sources[stepIndex]); err != nil {
		evidence.Clear()
		return Evidence{}, err
	}

	secondDynamic, err := reader.readFixed(ctx, dynamic.keys, evidence.ReadRevision)
	if err != nil {
		evidence.Clear()
		return Evidence{}, err
	}
	defer keyvalue.ClearValues(secondDynamic.Values)
	if err := reader.resolveEncryptedCredentialValues(
		ctx, evidence.ReadRevision, dynamic, &evidence, secondDynamic,
	); err != nil {
		evidence.Clear()
		return Evidence{}, err
	}
	return evidence, nil
}
