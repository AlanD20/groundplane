package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintAuthoredPublication accepts immutable operator input and one
// visible reconciliation parent, without claiming that fact-dependent runtime
// files or host effects already exist. Captured work membership is published
// after this head, Task and immutable graph become atomically visible.
type BlueprintAuthoredPublication struct {
	Project                  keyvalue.Versioned[hierarchy.ProjectRecord]
	Environment              keyvalue.Versioned[hierarchy.EnvironmentRecord]
	ExpectedHeadRevision     int64
	Claim                    blueprints.EnvironmentBlueprintStageClaim
	DesiredInput             projectionrecord.EnvironmentDesiredInput
	OwnedIdentities          projectionrecord.EnvironmentOwnedIdentities
	ZoneReservations         []zones.Record
	PrerequisiteAuthority    BlueprintPrerequisiteAuthority
	AttachInputGenerations   []BlueprintAttachInputGenerationPublication
	RetainedAttachConditions []keyvalue.Condition
	Parent                   workexecution.Task
	Graph                    blueprintunits.CapturedGraph
	Baseline                 blueprintunits.Snapshot
	Marker                   idempotency.IdempotencyMarker
	ScriptPublication        BlueprintScriptPublication
}

func (repository *EnvironmentBlueprintRepository) PublishEnvironmentBlueprintAuthoredRevision(
	ctx context.Context, input BlueprintAuthoredPublication,
) (keyvalue.TransactionResult, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.TransactionResult{}, err
	}
	project, environment, parent, claim := input.Project, input.Environment, input.Parent, input.Claim
	if err := hierarchy.ValidateProject(project.Record); err != nil {
		return keyvalue.TransactionResult{}, err
	}
	if err := hierarchy.ValidateEnvironment(environment.Record); err != nil {
		return keyvalue.TransactionResult{}, err
	}
	if len(input.DesiredInput.Input.DNS) != 0 && project.Record.Kind != hierarchy.ProjectKindTenant {
		return keyvalue.TransactionResult{}, errs.New(errs.KindValidationFailed,
			"tenantless Backing Environments cannot own scoped DNS records; use platform DNS settings")
	}
	if project.Revision <= 0 || environment.Revision <= 0 ||
		input.Baseline.EnvironmentID != environment.Record.ID || input.Baseline.ReadRevision <= 0 ||
		input.Baseline.HeadRevision != input.ExpectedHeadRevision || input.Baseline.EpochRevision < 0 ||
		environment.Record.ProjectID != project.Record.ID ||
		environment.Record.ProvisioningState != hierarchy.EnvironmentProvisioningReady ||
		input.ExpectedHeadRevision < 0 || claim.BaselineHeadRevision != input.ExpectedHeadRevision ||
		claim.SourceKind != blueprints.EnvironmentBlueprintSourceApply ||
		claim.EnvironmentID != environment.Record.ID || claim.RevisionID != parent.ID || claim.TaskID != parent.ID ||
		parent.Action != workexecution.ActionApply || parent.Actor != workexecution.OperatorActor ||
		parent.TargetID != environment.Record.ID || parent.Scope.EnvironmentID != environment.Record.ID ||
		parent.Scope.ProjectID != project.Record.ID || parent.Scope.TenantID != project.Record.TenantID ||
		input.DesiredInput.EnvironmentID != claim.EnvironmentID ||
		input.DesiredInput.RevisionID != claim.RevisionID ||
		input.DesiredInput.RenderGeneration != claim.RenderGeneration ||
		input.OwnedIdentities.EnvironmentID != claim.EnvironmentID ||
		input.OwnedIdentities.RevisionID != claim.RevisionID ||
		input.OwnedIdentities.RenderGeneration != claim.RenderGeneration ||
		input.Marker.TaskID != parent.ID || input.Marker.Locator != claim.Locator ||
		!input.Marker.CreatedAt.Equal(parent.CreatedAt) ||
		!input.Marker.UpdatedAt.Equal(parent.CreatedAt) {
		return keyvalue.TransactionResult{}, errs.New(
			errs.KindValidationFailed, "Blueprint authored publication identity is invalid",
		)
	}
	if err := parent.Validate(); err != nil {
		return keyvalue.TransactionResult{}, err
	}
	if err := input.ScriptPublication.validate(environment.Record.ID, claim.SourceKind); err != nil {
		return keyvalue.TransactionResult{}, err
	}
	if input.ScriptPublication.IsZero() {
		if len(input.OwnedIdentities.Scripts) != 0 {
			return keyvalue.TransactionResult{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Script set has not been prepared",
			)
		}
	} else {
		set, err := scriptrecord.DecodeScriptSetGeneration(input.ScriptPublication.mutations[0].Value)
		if err != nil || set.GenerationID != parent.ID {
			return keyvalue.TransactionResult{}, errs.New(errs.KindValidationFailed, "Blueprint Script set belongs to another revision")
		}
	}
	if err := idempotency.ValidateIdempotencyMarker(input.Marker); err != nil {
		return keyvalue.TransactionResult{}, err
	}
	var birthConditions []keyvalue.Condition
	if input.ExpectedHeadRevision > 0 && len(input.OwnedIdentities.Components) > 0 {
		previous, found, err := blueprints.ReadCurrentOwnedIdentities(ctx, repository.store, claim.EnvironmentID, 0)
		if err != nil {
			return keyvalue.TransactionResult{}, err
		}
		if !found || previous.Revision != input.ExpectedHeadRevision {
			return keyvalue.TransactionResult{}, errs.New(
				errs.KindStateConflict,
				"Environment Component identity predecessor changed",
			)
		}
		input.OwnedIdentities, birthConditions, err = restoreOmittedComponentBirths(
			ctx, repository.store, input.OwnedIdentities, previous.ReadRevision,
		)
		if err != nil {
			return keyvalue.TransactionResult{}, err
		}
	}
	ownedIdentityValue, err := projectionrecord.EncodeEnvironmentOwnedIdentities(input.OwnedIdentities)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	defer clear(ownedIdentityValue)
	initialAbsence, err := blueprintunits.PrepareInitialAbsencePublication(
		claim.EnvironmentID,
		claim.RevisionID,
		newbornBlueprintUnitTargets(input.OwnedIdentities, claim.RevisionID),
	)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	defer initialAbsence.Clear()
	desiredValue, err := projectionrecord.EncodeEnvironmentDesiredInputStorage(input.DesiredInput)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	defer clear(desiredValue)
	desiredDigest := sha256.Sum256(desiredValue)
	if hex.EncodeToString(desiredDigest[:]) != parent.Intent.SHA256 {
		return keyvalue.TransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Task input differs from the sealed desired revision",
		)
	}
	if len(input.ZoneReservations) != len(input.OwnedIdentities.Networks) {
		return keyvalue.TransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Zone reservations are incomplete",
		)
	}
	zoneIdentities := make(map[string]string, len(input.OwnedIdentities.Networks))
	for _, owned := range input.OwnedIdentities.Networks {
		zoneIdentities[owned.ID] = owned.Name
	}
	for _, zone := range input.ZoneReservations {
		if zones.ValidateRecord(zone) != nil || zone.EnvironmentID != environment.Record.ID ||
			zoneIdentities[zone.Desired.ID] != zone.Desired.Name {
			return keyvalue.TransactionResult{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Zone reservation identity changed",
			)
		}
		delete(zoneIdentities, zone.Desired.ID)
	}
	effectiveEnvironment := environment.Record
	effectiveEnvironment.NetworkPool = input.DesiredInput.Input.NetworkPool
	zonePool, err := networkreservations.NewPlanner(repository.store).PrepareZonePoolAtRevision(
		ctx, effectiveEnvironment, input.ZoneReservations, 0,
	)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	defer clear(zonePool.Value())
	nameConditions, nameMutations, err := dnsrecords.NewReader(repository.store).PrepareAuthoredDNSPublication(
		ctx, input.DesiredInput, input.OwnedIdentities, environment.ReadRevision,
	)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(nameMutations)
	prerequisites, err := prepareBlueprintPrerequisiteAuthorityPublication(
		input.PrerequisiteAuthority, input.DesiredInput,
	)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	defer prerequisites.clear()
	revision := blueprints.EnvironmentDesiredRevisionIdentity{
		EnvironmentID: environment.Record.ID, RevisionID: claim.RevisionID,
	}
	publication, err := repository.prepareEnvironmentBlueprintPublicationWithDigest(
		ctx, claim, revision, desiredDigest, input.Marker,
		input.ExpectedHeadRevision,
	)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	defer clear(publication.publishedDescriptor)
	work, err := workqueue.PrepareTask(parent, input.Marker)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(work.Mutations)
	markerConditions, markerMutations, err := idempotency.PreparePublication(input.Marker)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(markerMutations)
	reference, err := idempotency.EncodeTaskReference(parent.ID)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	defer clear(reference)
	conditions := []keyvalue.Condition{
		{Key: blueprintunits.EpochKey(environment.Record.ID), ModRevision: input.Baseline.EpochRevision},
		{
			Key:         blueprints.EnvironmentBlueprintRootKey(revision.EnvironmentID, revision.RevisionID),
			ModRevision: publication.rootRevision,
		},
		{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
		{Key: blueprints.EnvironmentBlueprintHeadKey(revision.EnvironmentID), ModRevision: input.ExpectedHeadRevision},
		{Key: blueprints.EnvironmentBlueprintOwnedIdentitiesKey(revision.EnvironmentID, revision.RevisionID)},
		{Key: removalrecord.EnvironmentLockKey(environment.Record.ID)},
	}
	mutations := []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: publication.descriptorKey, Value: publication.publishedDescriptor},
		{Type: keyvalue.MutationDelete, Key: publication.locatorKey},
		{
			Type:  keyvalue.MutationPut,
			Key:   blueprints.EnvironmentBlueprintHeadKey(revision.EnvironmentID),
			Value: reference,
		},
		{
			Type:  keyvalue.MutationPut,
			Key:   blueprints.EnvironmentBlueprintOwnedIdentitiesKey(revision.EnvironmentID, revision.RevisionID),
			Value: ownedIdentityValue,
		},
		prerequisites.mutation,
	}
	conditions = append(conditions, prerequisites.conditions...)
	placementConditions, err := repository.blueprintWorkPlacementConditions(ctx, input.Graph)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	conditions = append(conditions, placementConditions...)
	conditions = append(conditions, nameConditions...)
	mutations = append(mutations, nameMutations...)
	zonePoolKey := networkreservations.ZonePoolRegistryKey(environment.Record.ID)
	conditions = append(conditions, keyvalue.Condition{Key: zonePoolKey, ModRevision: zonePool.CurrentRevision()})
	mutations = append(
		mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: zonePoolKey, Value: zonePool.Value()},
	)
	conditions = append(conditions, initialAbsence.Conditions()...)
	conditions = append(conditions, birthConditions...)
	mutations = append(mutations, initialAbsence.Mutations()...)
	conditions = append(conditions, input.ScriptPublication.conditions...)
	mutations = append(mutations, input.ScriptPublication.mutations...)
	tenant, err := loadConnectorTaskInitiationTenantAtRevision(
		ctx, repository.store, project, project.ReadRevision,
	)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(tenant, project, environment, taskjournal.TaskActorOperator)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	attachInputs, err := repository.prepareBlueprintAttachInputGenerations(
		ctx, parent, input.AttachInputGenerations,
	)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	defer attachInputs.clear()
	if err := repository.stageBlueprintAttachInputs(ctx, parent,
		keyvalue.Condition{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		[]keyvalue.Condition{
			{Key: blueprints.EnvironmentBlueprintHeadKey(environment.Record.ID), ModRevision: input.ExpectedHeadRevision},
			{Key: blueprintunits.EpochKey(environment.Record.ID), ModRevision: input.Baseline.EpochRevision},
		},
		&attachInputs); err != nil {
		return attachInputs.finish(ctx, repository.store, parent.ID, keyvalue.TransactionResult{}, err)
	}
	captured := input.Graph
	for _, generation := range attachInputs.generations {
		if _, err := blueprintattachinputs.WorkInput(generation); err != nil {
			return attachInputs.finish(ctx, repository.store, parent.ID, keyvalue.TransactionResult{}, err)
		}
		matched := false
		for index := range captured.Items {
			item := &captured.Items[index]
			if item.ID != generation.WorkItemID {
				continue
			}
			if item.TargetID != generation.AttachID || item.Operation != workexecution.Attach ||
				item.Input != nil || item.Phase != workexecution.Waiting ||
				item.Wait == nil || item.Wait.Reason != workexecution.WaitInput {
				break
			}
			// Capturing encrypted input is not execution readiness. Preparation
			// publishes its pending binding and exact input reference atomically.
			matched = true
			break
		}
		if !matched {
			return attachInputs.finish(ctx, repository.store, parent.ID, keyvalue.TransactionResult{},
				errs.New(errs.KindValidationFailed, "Attach work does not reference its captured input generation"))
		}
	}
	graph, err := blueprintunits.PrepareCapturedGraph(parent, captured)
	if err != nil {
		return attachInputs.finish(ctx, repository.store, parent.ID, keyvalue.TransactionResult{}, err)
	}
	defer keyvalue.ClearMutationValues(graph.Mutations)
	desiredPlan, err := blueprintunits.PrepareAuthoredPlanPublication(input.Baseline, blueprintunits.DesiredPlan{
		EnvironmentID: parent.Scope.EnvironmentID, ParentTaskID: parent.ID,
		Complete: true, Units: captured.Units,
	})
	if err != nil {
		return attachInputs.finish(ctx, repository.store, parent.ID, keyvalue.TransactionResult{}, err)
	}
	defer desiredPlan.Clear()
	conditions = append(conditions, desiredPlan.Conditions()...)
	mutations = append(mutations, desiredPlan.Mutations()...)
	conditions = append(conditions, attachInputs.conditions...)
	mutations = append(mutations, attachInputs.mutations...)
	baseCount := len(conditions)
	classifier := func(_ int64, values []*keyvalue.KeyValue) error {
		if len(values) < baseCount {
			return errs.New(errs.KindInternal, "Blueprint authored publication compare evidence is incomplete")
		}
		return errs.New(errs.KindStateConflict, "Blueprint authored publication raced")
	}
	conditions, _, err = prepareTaskInitiationFences(initiation, conditions, classifier)
	if err != nil {
		return attachInputs.finish(ctx, repository.store, parent.ID, keyvalue.TransactionResult{}, err)
	}
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return attachInputs.finish(ctx, repository.store, parent.ID, keyvalue.TransactionResult{}, err)
	}
	conditions = append(conditions, input.RetainedAttachConditions...)
	result, publicationErr := queue.Commit(ctx, work, graph,
		workqueue.Change{Conditions: markerConditions, Mutations: markerMutations},
		workqueue.Change{Conditions: conditions, Mutations: mutations})
	return attachInputs.finish(ctx, repository.store, parent.ID, result, publicationErr)
}

func newbornBlueprintUnitTargets(
	identities projectionrecord.EnvironmentOwnedIdentities, birthRevisionID string,
) []blueprintunits.ResourceKey {
	targets := make([]blueprintunits.ResourceKey, 0,
		len(identities.Services)+len(identities.Networks)+len(identities.Volumes)+
			len(identities.Entries)+len(identities.Routes)+len(identities.Attaches)+
			len(identities.Components)+len(identities.Scripts)+len(identities.DNSRecords))
	for _, identity := range identities.Services {
		if identity.BirthRevisionID == birthRevisionID {
			targets = append(targets, blueprintunits.ResourceKey{Kind: ids.KindService, ID: identity.ID})
		}
	}
	for _, identity := range identities.Networks {
		if identity.BirthRevisionID == birthRevisionID {
			targets = append(targets, blueprintunits.ResourceKey{Kind: ids.KindNetwork, ID: identity.ID})
		}
	}
	for _, identity := range identities.Volumes {
		if identity.BirthRevisionID == birthRevisionID {
			targets = append(targets, blueprintunits.ResourceKey{Kind: ids.KindVolume, ID: identity.ID})
		}
	}
	for _, identity := range identities.Entries {
		if identity.BirthRevisionID == birthRevisionID {
			targets = append(targets, blueprintunits.ResourceKey{Kind: ids.KindEnvEntry, ID: identity.ID})
		}
	}
	for _, identity := range identities.Routes {
		if identity.BirthRevisionID == birthRevisionID {
			targets = append(targets, blueprintunits.ResourceKey{Kind: ids.KindRoute, ID: identity.ID})
		}
	}
	for _, identity := range identities.DNSRecords {
		if identity.BirthRevisionID == birthRevisionID {
			targets = append(targets, blueprintunits.ResourceKey{Kind: ids.KindDNSRecord, ID: identity.ID})
		}
	}
	for _, identity := range identities.Attaches {
		if identity.BirthRevisionID == birthRevisionID {
			targets = append(targets, blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: identity.ID})
		}
	}
	for _, identity := range identities.Components {
		if identity.BirthRevisionID == birthRevisionID {
			targets = append(targets, blueprintunits.ResourceKey{Kind: ids.KindComponent, ID: identity.ID})
		}
	}
	for _, identity := range identities.Scripts {
		if identity.BirthRevisionID == birthRevisionID {
			targets = append(targets, blueprintunits.ResourceKey{Kind: ids.KindScript, ID: identity.ID})
		}
	}
	return targets
}
