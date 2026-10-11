package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BackingCheckSource owns one MVCC capture. Its comparisons cannot be replaced
// by a caller constructing public Source metadata. Host selection is bound to
// this acknowledged workload and installed Agent before publication.
type BackingCheckSource struct {
	facts                      attachments.AccessFactSource
	environment                keyvalue.Versioned[hierarchy.EnvironmentRecord]
	project                    keyvalue.Versioned[hierarchy.ProjectRecord]
	backingProject             keyvalue.Versioned[hierarchy.ProjectRecord]
	consumerService            keyvalue.Versioned[services.ServiceRecord]
	backingService             keyvalue.Versioned[services.ServiceRecord]
	fence, backingFence        environmentfence.Evidence
	conditions                 []keyvalue.Condition
	agent                      keyvalue.Versioned[localagents.LocalAgentRecord]
	runtimeRevision            int64
	target                     *agentpb.ServiceObservationTarget
	replicas                   uint32
	helperImageID, containerID string
}

func (source *BackingCheckSource) Clear() { source.facts.Clear() }

func (source BackingCheckSource) FactSource() attachments.AccessFactSource {
	copyOfSource := source.facts
	copyOfSource.Consumer.Record = attachments.CloneAttachRecord(source.facts.Consumer.Record)
	copyOfSource.Owner.Record = attachments.CloneAttachRecord(source.facts.Owner.Record)
	if source.facts.Grant != nil {
		grant := *source.facts.Grant
		grant.Record = attachments.CloneAttachRecord(grant.Record)
		copyOfSource.Grant = &grant
	}
	copyOfSource.Facts.Record.Ciphertext = slices.Clone(source.facts.Facts.Record.Ciphertext)
	return copyOfSource
}

func (source BackingCheckSource) Scope() (keyvalue.Versioned[hierarchy.EnvironmentRecord], keyvalue.Versioned[hierarchy.ProjectRecord]) {
	return source.environment, source.project
}

func (source BackingCheckSource) AdapterKey() string {
	return source.backingService.Record.Desired.Adapter
}
func (source BackingCheckSource) AgentID() string     { return source.agent.Record.ID }
func (source BackingCheckSource) HelperImage() string { return source.agent.Record.Image }

func (repository *AttachRepository) LoadBackingCheckSource(ctx context.Context,
	environmentID, attachID string, revision int64, grantID string,
) (_ BackingCheckSource, returnErr error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindAttach, attachID) != nil ||
		revision <= 0 || grantID != "" && ids.Validate(ids.KindAttach, grantID) != nil {
		return BackingCheckSource{}, errs.New(
			errs.KindValidationFailed,
			"access check requires an Environment and current Attach revision",
		)
	}
	consumer, err := repository.GetAttach(ctx, attachID)
	if err != nil {
		return BackingCheckSource{}, err
	}
	if consumer.Revision != revision || consumer.Record.EnvironmentID != environmentID {
		return BackingCheckSource{}, errs.New(
			errs.KindStateConflict,
			"Attach changed or belongs to another Environment; refresh before checking access",
		)
	}
	facts, err := repository.CaptureAccessFacts(ctx, consumer, grantID)
	if err != nil {
		return BackingCheckSource{}, err
	}
	source := BackingCheckSource{facts: facts}
	defer func() {
		if returnErr != nil {
			source.Clear()
		}
	}()
	if consumer.Record.Status != core.AttachReady || facts.Owner.Record.Status != core.AttachReady ||
		consumer.Record.HookBundle {
		return BackingCheckSource{}, errs.New(errs.KindStateConflict, "access check requires a ready built-in Attach")
	}
	source.conditions = facts.Conditions()
	if err := repository.captureBackingCheckHierarchy(ctx, &source); err != nil {
		return BackingCheckSource{}, err
	}
	consumerService, err := repository.backingCheckServiceAt(
		ctx,
		environmentID,
		consumer.Record.ServiceID,
		consumer.ReadRevision,
	)
	if err != nil {
		return BackingCheckSource{}, err
	}
	backingService, err := repository.backingCheckServiceAt(
		ctx,
		consumer.Record.BackingEnvironmentID,
		consumer.Record.BackingServiceID,
		consumer.ReadRevision,
	)
	if err != nil {
		return BackingCheckSource{}, err
	}
	if consumerService.Record.BackingNetworkID != "" ||
		consumerService.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning ||
		backingService.Record.BackingNetworkID != consumer.Record.BackingNetworkID ||
		backingService.Record.Desired.Adapter == "custom" {
		return BackingCheckSource{}, errs.New(
			errs.KindStateConflict,
			"access check requires a running consumer and its built-in Backing Service",
		)
	}
	source.consumerService, source.backingService = consumerService, backingService
	source.conditions = append(
		source.conditions,
		services.ServiceDesiredCondition(consumerService),
		services.ServiceRuntimeCondition(consumerService),
		services.ServiceDesiredCondition(backingService),
		services.ServiceRuntimeCondition(backingService),
	)
	keys := []string{serviceruntimerecord.Key(consumer.Record.ServiceID), localagents.LocalAgentSingletonKey,
		services.ServiceLifecycleActiveKey(
			consumer.Record.ServiceID,
		), services.ServiceLifecycleActiveKey(consumer.Record.BackingServiceID),
		deletions.TombstoneKey(string(deletions.DeletionTargetService), consumer.Record.ServiceID),
		deletions.TombstoneKey(string(deletions.DeletionTargetService), consumer.Record.BackingServiceID),
		backingchecks.LatestKey(attachID, grantID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: consumer.ReadRevision})
	if err != nil {
		return BackingCheckSource{}, err
	}
	if read == nil || read.ReadRevision != consumer.ReadRevision || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return BackingCheckSource{}, errs.New(errs.KindInternal, "access check runtime capture is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil {
		return BackingCheckSource{}, errs.New(
			errs.KindStateConflict,
			"Deploy the consumer and wait for the Agent to be ready before checking access",
		)
	}
	for index := 2; index <= 5; index++ {
		if read.Values[index] != nil {
			return BackingCheckSource{}, errs.New(
				errs.KindResourceInUse,
				"consumer or Backing Service has an active lifecycle operation; wait for it before checking access",
			)
		}
	}
	runtime, err := decodeAcknowledgedServiceRuntime(read.Values[0].Value, environmentID, consumer.Record.ServiceID)
	if err != nil {
		return BackingCheckSource{}, err
	}
	source.target, source.replicas, err = serviceruntimerecord.WorkloadObservation(runtime)
	if err != nil {
		return BackingCheckSource{}, err
	}
	source.runtimeRevision = read.Values[0].ModRevision
	agentID, err := localagents.DecodeLocalAgentReference(read.Values[1].Value)
	if err != nil {
		return BackingCheckSource{}, err
	}
	if agentID != runtime.Source.AgentID {
		return BackingCheckSource{}, errs.New(errs.KindStateConflict, "consumer runtime belongs to another Agent")
	}
	agentKey := localagents.LocalAgentPrimaryKey(agentID)
	agentRead, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{agentKey}, Revision: consumer.ReadRevision},
	)
	if err != nil {
		return BackingCheckSource{}, err
	}
	if agentRead == nil || agentRead.ReadRevision != consumer.ReadRevision || len(agentRead.Values) != 1 ||
		agentRead.Values[0] == nil {
		if agentRead != nil {
			keyvalue.ClearValues(agentRead.Values)
		}
		return BackingCheckSource{}, errs.New(errs.KindStateConflict, "installed Agent is unavailable")
	}
	defer keyvalue.ClearValues(agentRead.Values)
	agent, err := localagents.DecodeLocalAgentPrimary(agentRead.Values[0].Value)
	if err != nil {
		return BackingCheckSource{}, err
	}
	if agent.ID != agentID || agent.Phase != localagents.LocalAgentPhaseReady {
		return BackingCheckSource{}, errs.New(
			errs.KindStateConflict,
			"wait for Agent installation or update to finish before checking access",
		)
	}
	source.agent = keyvalue.Versioned[localagents.LocalAgentRecord]{
		Record:       agent,
		Revision:     agentRead.Values[0].ModRevision,
		ReadRevision: consumer.ReadRevision,
	}
	source.conditions = append(source.conditions, keyvalue.Condition{Key: agentKey, ModRevision: source.agent.Revision})
	for index, key := range keys {
		source.conditions = append(
			source.conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])},
		)
	}
	return source, nil
}

func (repository *AttachRepository) backingCheckServiceAt(
	ctx context.Context,
	environmentID, serviceID string,
	revision int64,
) (keyvalue.Versioned[services.ServiceRecord], error) {
	projection, found, err := blueprints.ReadCurrentProjection(ctx, repository.store, environmentID, revision)
	if err != nil {
		return keyvalue.Versioned[services.ServiceRecord]{}, err
	}
	if !found || projection.ReadRevision != revision {
		return keyvalue.Versioned[services.ServiceRecord]{}, errs.New(
			errs.KindStateConflict,
			"access check Service configuration is unavailable",
		)
	}
	return services.ReadJoined(ctx, repository.store, services.DesiredSelection{
		Services: environmentqueries.OrdinaryServices(
			projection.Record,
		), Revision: projection.Revision, ReadRevision: revision,
	}, serviceID, blueprints.EnvironmentBlueprintHeadKey(environmentID))
}
