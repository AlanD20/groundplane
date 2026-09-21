package agentregistration

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	localagentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) ResolveAgentChannel(
	ctx context.Context,
	presentedAgentID string,
	token [agentprotocol.RawTokenBytes]byte,
) (localagentrecord.LocalAgentChannelAuthorization, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return localagentrecord.LocalAgentChannelAuthorization{}, err
	}
	if err := ids.Validate(ids.KindAgent, presentedAgentID); err != nil {
		return localagentrecord.LocalAgentChannelAuthorization{}, agentCredentialNotFound()
	}
	digest := sha256.Sum256(token[:])
	digestText := base64.RawURLEncoding.EncodeToString(digest[:])
	lookup, err := repository.store.Get(ctx, localagentrecord.LocalAgentDigestKey(digestText))
	if err != nil {
		return localagentrecord.LocalAgentChannelAuthorization{}, err
	}
	if lookup == nil || lookup.Entry == nil {
		return localagentrecord.LocalAgentChannelAuthorization{}, agentCredentialNotFound()
	}
	resolvedAgentID, err := localagentrecord.DecodeLocalAgentReference(lookup.Entry.Value)
	if err != nil || resolvedAgentID != presentedAgentID {
		return localagentrecord.LocalAgentChannelAuthorization{}, agentCredentialNotFound()
	}
	values, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			localagentrecord.LocalAgentSingletonKey,
			localagentrecord.LocalAgentPrimaryKey(resolvedAgentID),
			localagentrecord.LocalAgentConfigKey(resolvedAgentID),
		},
		Revision: lookup.ReadRevision,
	})
	if err != nil {
		return localagentrecord.LocalAgentChannelAuthorization{}, err
	}
	if len(values.Values) != 3 || values.Values[0] == nil || values.Values[1] == nil || values.Values[2] == nil {
		return localagentrecord.LocalAgentChannelAuthorization{}, errs.New(
			errs.KindInternal,
			"Agent credential references incomplete durable state",
		)
	}
	singletonAgentID, err := localagentrecord.DecodeLocalAgentReference(values.Values[0].Value)
	if err != nil || singletonAgentID != resolvedAgentID {
		return localagentrecord.LocalAgentChannelAuthorization{}, errs.New(
			errs.KindInternal,
			"Agent singleton does not match credential lookup",
		)
	}
	primary, err := localagentrecord.DecodeLocalAgentPrimary(values.Values[1].Value)
	if err != nil {
		return localagentrecord.LocalAgentChannelAuthorization{}, err
	}
	config, err := localagentrecord.DecodeLocalAgentConfig(values.Values[2].Value)
	if err != nil {
		return localagentrecord.LocalAgentChannelAuthorization{}, err
	}
	if primary.ID != resolvedAgentID || config.AgentID != resolvedAgentID ||
		primary.Generation != config.Generation || primary.Phase == localagentrecord.LocalAgentPhaseDeleting {
		return localagentrecord.LocalAgentChannelAuthorization{}, agentCredentialNotFound()
	}
	return localagentrecord.LocalAgentChannelAuthorization{
		AgentID: resolvedAgentID, Generation: primary.Generation,
		Config: localagentrecord.CloneLocalAgentConfig(config.Config),
	}, nil
}
