package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareResolverDNSMetadata(ctx context.Context,
	environmentID string, ack resolverAcknowledgement, revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	owned, found, err := blueprints.ReadCurrentOwnedIdentities(ctx, repository.store, environmentID, revision)
	if err != nil || !found {
		return nil, nil, err
	}
	rootKey := blueprints.EnvironmentBlueprintRootKey(environmentID, owned.Record.RevisionID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{rootKey}, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, nil, errs.New(errs.KindInternal, "DNS metadata cleanup source is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(read.Values[0].Value)
	if err != nil {
		return nil, nil, err
	}
	if seal.EnvironmentID != environmentID || seal.RevisionID != owned.Record.RevisionID {
		return nil, nil, errs.New(errs.KindInternal, "DNS metadata cleanup source identity changed")
	}
	if seal.SourceKind != blueprints.EnvironmentBlueprintSourceMutation {
		return nil, nil, nil
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return nil, nil, err
	}
	snapshot, err := ledger.LoadAtRevision(ctx, environmentID, revision)
	if err != nil {
		return nil, nil, err
	}
	if snapshot.HeadTaskID != owned.Record.RevisionID || snapshot.HeadRevision != owned.Revision {
		return nil, nil, errs.New(errs.KindInternal, "DNS metadata cleanup head is inconsistent")
	}
	if snapshot.Desired != nil && snapshot.Desired.Record.ParentTaskID == snapshot.HeadTaskID {
		return nil, nil, nil
	}
	guards := []keyvalue.Condition{
		{Key: rootKey, ModRevision: read.Values[0].ModRevision},
		{Key: blueprints.EnvironmentBlueprintHeadKey(environmentID), ModRevision: snapshot.HeadRevision},
		{Key: blueprintunits.EpochKey(environmentID), ModRevision: snapshot.EpochRevision},
	}
	desired, found, err := blueprints.ReadCurrentDesiredInput(ctx, repository.store, environmentID, revision)
	if err != nil {
		return nil, nil, err
	}
	if !found {
		return nil, nil, errs.New(errs.KindInternal, "DNS metadata cleanup authored input is missing")
	}
	records, err := dnsrecords.AuthoredEnvironmentRecords(desired.Record, owned.Record)
	if err != nil {
		return nil, nil, err
	}
	var hosts []platformcomponents.PlatformDNSHost
	proof := blueprintunits.MetadataDNSReceipt{
		AuthoredRevisionID: snapshot.HeadTaskID, AuthoredRevision: snapshot.HeadRevision,
		ComponentID: ack.component.Desired.ID, HostID: ack.state.HostID,
		AppliedRevision: ack.state.AppliedRevision, NeverPublished: ack.state.Applied == nil,
	}
	if applied := ack.state.Applied; applied != nil {
		proof.ResolverEnabled = applied.Desired.Enabled
		proof.CorefileSHA256 = applied.ConfigurationSHA256()
		if applied.Desired.Enabled {
			hosts = applied.Hosts
		}
	}
	reader := dnsrecords.NewReader(repository.store)
	for _, record := range records {
		if metadataDNSExecutionHeld(snapshot, record.ID) {
			continue
		}
		conditions, mutations, _, err := reader.PrepareHostnameRetirement(
			ctx,
			environmentID,
			record.ID,
			record.Hostname(),
			hosts,
			revision,
		)
		if err != nil {
			return nil, nil, err
		}
		if len(mutations) != 0 {
			return append(guards, conditions...), mutations, nil
		}
	}
	for _, omission := range owned.Record.PendingOmissions {
		if omission.Kind != ids.KindDNSRecord || metadataDNSExecutionHeld(snapshot, omission.ID) {
			continue
		}
		target := blueprintunits.ResourceKey{Kind: ids.KindDNSRecord, ID: omission.ID}
		settled := false
		for _, applied := range snapshot.Applied {
			if applied.Record.Target == target && applied.Record.State == blueprintunits.Absent &&
				applied.Record.HasEffectSource() {
				settled = true
				break
			}
		}
		if settled {
			continue
		}
		if applied := ack.state.Applied; applied != nil && applied.Desired.Enabled {
			if slices.ContainsFunc(applied.EnvironmentDNS, func(capture platformcomponents.EnvironmentDNSCapture) bool {
				return capture.Binding.Record.EnvironmentID == environmentID && capture.Binding.Record.ID == omission.ID
			}) {
				continue
			}
		}
		conditions, mutations, more, err := reader.PrepareHostnameRetirement(
			ctx,
			environmentID,
			omission.ID,
			"",
			hosts,
			revision,
		)
		if err != nil {
			return nil, nil, err
		}
		if len(mutations) != 0 {
			return append(guards, conditions...), mutations, nil
		}
		if more {
			continue
		}
		plan, err := blueprintunits.PrepareMetadataDNSAbsence(snapshot, target, proof)
		if err != nil {
			return nil, nil, err
		}
		return append(guards, plan.Conditions()...), plan.Mutations(), nil
	}
	return nil, nil, nil
}

func metadataDNSExecutionHeld(snapshot blueprintunits.Snapshot, recordID string) bool {
	target := blueprintunits.ResourceKey{Kind: ids.KindDNSRecord, ID: recordID}
	for _, execution := range snapshot.Executions {
		if slices.Contains(execution.Record.Unit.Reads, target) ||
			slices.Contains(execution.Record.Unit.Writes, target) {
			return true
		}
	}
	return false
}
