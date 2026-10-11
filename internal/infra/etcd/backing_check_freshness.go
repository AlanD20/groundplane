package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BackingCheckSourcesChanged compares stored revisions only. It never decrypts
// credentials or queries a provider just to display historical evidence.
func (repository *AttachRepository) BackingCheckSourcesChanged(
	ctx context.Context,
	source backingchecks.Source,
) (bool, error) {
	if err := source.Validate(); err != nil {
		return false, err
	}
	parameters := source.Parameters
	keys := []string{attachments.AttachKey(parameters.AttachID), attachments.AttachKey(parameters.CredentialAttachID),
		attachments.AttachFactsKey(parameters.CredentialAttachID), serviceruntimerecord.Key(parameters.ServiceID),
		localagents.LocalAgentPrimaryKey(
			source.AgentID,
		), blueprints.EnvironmentBlueprintHeadKey(parameters.EnvironmentID),
		services.ServiceRuntimeKey(
			parameters.ServiceID,
		), blueprints.EnvironmentBlueprintHeadKey(source.BackingEnvironmentID),
		services.ServiceRuntimeKey(parameters.BackingServiceID)}
	revisions := []int64{
		source.AttachRevision,
		source.CredentialRevision,
		source.FactsRevision,
		source.RuntimeRevision,
		source.AgentRevision,
		source.ConsumerDesiredRevision,
		source.ConsumerIntentRevision,
		source.BackingDesiredRevision,
		source.BackingRuntimeRevision,
	}
	if source.GrantID != "" {
		keys = append(keys, attachments.AttachKey(source.GrantID))
		revisions = append(revisions, source.GrantRevision)
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return false, errs.New(errs.KindInternal, "access check freshness evidence is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, revision := range revisions {
		if keyvalue.RevisionOf(read.Values[index]) != revision {
			return true, nil
		}
	}
	return false, nil
}
