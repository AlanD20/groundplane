package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	localagentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *LocalAgentRepository) GetSingleton(
	ctx context.Context,
) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	evidence, err := repository.readSingleton(ctx)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
		Record: evidence.record, Revision: evidence.primaryRevision,
		ReadRevision: evidence.readRevision,
	}, nil
}

func (repository *LocalAgentRepository) readSingleton(ctx context.Context) (localAgentEvidence, error) {
	pointer, err := repository.store.Get(ctx, localagentrecord.LocalAgentSingletonKey)
	if err != nil {
		return localAgentEvidence{}, err
	}
	if pointer == nil || pointer.Entry == nil {
		return localAgentEvidence{}, errs.New(errs.KindAgentNotFound, "local Agent was not found")
	}
	agentID, err := localagentrecord.DecodeLocalAgentReference(pointer.Entry.Value)
	if err != nil {
		return localAgentEvidence{}, err
	}
	values, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			localagentrecord.LocalAgentPrimaryKey(agentID), localagentrecord.LocalAgentOwnerKey(agentID),
			localagentrecord.LocalAgentConfigKey(agentID), localagentrecord.LocalAgentTokenKey(agentID),
		},
		Revision: pointer.ReadRevision,
	})
	if err != nil {
		return localAgentEvidence{}, err
	}
	if len(values.Values) != 4 {
		return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent aggregate read is incomplete")
	}
	for _, value := range values.Values {
		if value == nil {
			return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent aggregate is incomplete")
		}
	}
	primary, err := localagentrecord.DecodeLocalAgentPrimary(values.Values[0].Value)
	if err != nil {
		return localAgentEvidence{}, err
	}
	ownerAgentID, err := localagentrecord.DecodeLocalAgentReference(values.Values[1].Value)
	if err != nil || ownerAgentID != agentID {
		return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent owner index does not match primary")
	}
	config, err := localagentrecord.DecodeLocalAgentConfig(values.Values[2].Value)
	if err != nil {
		return localAgentEvidence{}, err
	}
	token, err := localagentrecord.DecodeLocalAgentToken(values.Values[3].Value)
	if err != nil {
		return localAgentEvidence{}, err
	}
	if primary.ID != agentID || config.AgentID != agentID || token.AgentID != agentID ||
		primary.Generation != config.Generation || primary.Generation != token.Generation {
		return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent aggregate identities do not match")
	}
	digests, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: localagentrecord.LocalAgentDigestPrefix, Limit: 2, Revision: pointer.ReadRevision,
	})
	if err != nil {
		return localAgentEvidence{}, err
	}
	if digests.More || len(digests.Values) > 1 {
		return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent has multiple credential indexes")
	}
	var digestValue *etcdstore.KeyValue
	digestText := ""
	if len(digests.Values) == 1 {
		value := digests.Values[0]
		indexedAgentID, decodeErr := localagentrecord.DecodeLocalAgentReference(value.Value)
		parsedDigest, parseErr := localagentrecord.LocalAgentDigestFromKey(value.Key)
		if decodeErr != nil || parseErr != nil || indexedAgentID != agentID {
			return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent credential index is corrupt")
		}
		digestText = parsedDigest
		digestValue = &value
	}
	if primary.Phase == localagentrecord.LocalAgentPhaseDeleting {
		if digestValue != nil {
			return localAgentEvidence{}, errs.New(
				errs.KindInternal,
				"deleting local Agent still has credential authority",
			)
		}
	} else if digestValue == nil {
		return localAgentEvidence{}, errs.New(errs.KindInternal, "active local Agent is missing credential authority")
	}
	record := localagentrecord.LocalAgentRecord{
		ID: primary.ID, EnrollmentTaskID: primary.EnrollmentTaskID,
		Image: primary.Image, Generation: primary.Generation, Phase: primary.Phase,
		Config: config.Config, EncryptedToken: token.EncryptedToken,
		TokenDigest: digestText, CreatedAt: primary.CreatedAt, ReadyAt: primary.ReadyAt,
		TokenUpdatedAt: token.UpdatedAt,
	}
	if err := localagentrecord.ValidateLocalAgentRecord(record); err != nil {
		return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent aggregate is corrupt")
	}
	return localAgentEvidence{
		record: record, primaryRevision: values.Values[0].ModRevision,
		readRevision: pointer.ReadRevision, singleton: pointer.Entry,
		primary: values.Values[0], owner: values.Values[1], config: values.Values[2],
		token: values.Values[3], digest: digestValue,
	}, nil
}
