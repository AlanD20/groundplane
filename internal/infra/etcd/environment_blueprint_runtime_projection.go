package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	componentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BlueprintRuntimeComponent is one Component singleton and its acknowledged
// runtime at the same fixed ledger snapshot. Runtime absence is accepted only
// with proven disabled singleton state.
type BlueprintRuntimeComponent struct {
	Component      etcdstore.Versioned[componentrecord.Record]
	Runtime        etcdstore.Versioned[componentplanning.Runtime]
	RuntimePresent bool
}

// BlueprintRuntimeComponentReceipt binds an explicitly authored Component to
// the exact desired unit and acknowledged effect selected by the ledger.
type BlueprintRuntimeComponentReceipt struct {
	Identity projectionrecord.OwnedIdentity
	Unit     blueprintunits.Unit
	Applied  etcdstore.Versioned[blueprintunits.AppliedRecord]
}

// BlueprintRuntimeProjectionSeal is the complete authority for publishing one
// derived runtime projection. The desired input and owned identities remain
// immutable revision records; this seal never advances applied runtime state.
type BlueprintRuntimeProjectionSeal struct {
	Parent                  workexecution.Task
	SnapshotReadRevision    int64
	ExpectedHeadRevision    int64
	ExpectedEpochRevision   int64
	DesiredRootRevision     int64
	OwnedIdentitiesRevision int64
	Tenant                  etcdstore.Versioned[hierarchyrecord.TenantRecord]
	Project                 etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	Environment             etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	Projection              projectionrecord.EnvironmentComposeProjection
	Attaches                []etcdstore.Versioned[attachrecord.Record]
	Components              []BlueprintRuntimeComponent
	ComponentReceipts       []BlueprintRuntimeComponentReceipt
	BackupPreparation       blueprintplanning.BlueprintBackupPolicyPreparation
}

// ReadEnvironmentBlueprintRuntimeComponents captures the closed pair of
// Environment Component singletons and each runtime record at one MVCC
// revision. Whether an absent runtime is an allowed default is decided by the
// authored ledger projection, not inferred by this storage reader.
func (repository *EnvironmentBlueprintRepository) ReadEnvironmentBlueprintRuntimeComponents(
	ctx context.Context,
	environmentID string,
	revision int64,
) ([]BlueprintRuntimeComponent, error) {
	if repository == nil || repository.HierarchyRepository == nil {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint runtime Component lookup is invalid")
	}
	return readEnvironmentBlueprintRuntimeComponents(ctx, repository.store, environmentID, revision)
}

func readEnvironmentBlueprintRuntimeComponents(
	ctx context.Context, store hierarchyStore, environmentID string, revision int64,
) ([]BlueprintRuntimeComponent, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, err
	}
	if store == nil || ids.Validate(ids.KindEnvironment, environmentID) != nil || revision <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint runtime Component lookup is invalid")
	}
	page, err := recordquery.ListIndexAtRevision(
		ctx,
		store,
		"components",
		"environment",
		environmentID,
		componentrecord.EnvironmentOwnerPrefix(environmentID),
		componentrecord.RecordKey,
		ids.KindComponent,
		etcdstore.PageRequest{Limit: etcdstore.MaximumPageLimit, Revision: revision},
		componentrecord.DecodeRecord,
		func(record componentrecord.Record) string { return record.Desired.ID },
		func(record componentrecord.Record) bool {
			return record.Desired.Owner == core.ComponentOwnerEnvironment &&
				record.Desired.OwnerID == environmentID
		},
		revision,
	)
	if err != nil {
		return nil, err
	}
	if page.Revision != revision || page.NextCursor != "" || len(page.Items) != 2 {
		return nil, errs.New(errs.KindStateConflict, "Environment Component singleton set changed")
	}
	result := make([]BlueprintRuntimeComponent, len(page.Items))
	for index, component := range page.Items {
		runtime, found, err := componentplanning.ReadRuntime(
			ctx, store, component.Record.Desired.ID, revision,
		)
		if err != nil {
			return nil, err
		}
		if component.ReadRevision != revision || runtime.ReadRevision != revision {
			return nil, errs.New(errs.KindStateConflict, "Blueprint runtime Component snapshot changed")
		}
		if found {
			acknowledged := runtime.Record.Component
			acknowledged.Runtime.Healthy = component.Record.Runtime.Healthy
			if runtime.Record.EnvironmentID != environmentID ||
				!componentrecord.EqualRecord(acknowledged, component.Record) {
				return nil, errs.New(errs.KindStateConflict, "acknowledged Component runtime changed")
			}
		}
		result[index] = BlueprintRuntimeComponent{
			Component: component, Runtime: runtime, RuntimePresent: found,
		}
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].Component.Record.Desired.Kind < result[right].Component.Record.Desired.Kind
	})
	capabilities := make(map[core.ComponentCapability]bool, len(result))
	for _, singleton := range result {
		capability, supported := singleton.Component.Record.Desired.Kind.EnvironmentCapability()
		if !supported || capabilities[capability] {
			return nil, errs.New(errs.KindStateConflict, "Environment Component singleton capabilities changed")
		}
		capabilities[capability] = true
	}
	if !capabilities[core.ComponentCapabilityHTTPRouter] || !capabilities[core.ComponentCapabilityEdgeTunnel] {
		return nil, errs.New(errs.KindStateConflict, "Environment Component singleton kinds changed")
	}
	return result, nil
}

// SealEnvironmentBlueprintRuntimeProjection publishes one complete immutable
// artifact only while the same desired head, coordinator claim, planning epoch,
// source revision and hierarchy are still current.
func (repository *EnvironmentBlueprintRepository) SealEnvironmentBlueprintRuntimeProjection(
	ctx context.Context,
	input BlueprintRuntimeProjectionSeal,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	parent := input.Parent
	projection := input.Projection
	if parent.Validate() != nil || parent.Action != workexecution.ActionApply ||
		parent.Stop != nil || parent.FinishedAt != nil || parent.CollectionStartedAt != nil ||
		parent.Scope.EnvironmentID == "" || parent.TargetID != parent.Scope.EnvironmentID ||
		projection.EnvironmentID != parent.Scope.EnvironmentID ||
		projection.RevisionID != parent.ID ||
		input.SnapshotReadRevision <= 0 ||
		input.ExpectedHeadRevision <= 0 || input.ExpectedEpochRevision < 0 ||
		input.DesiredRootRevision <= 0 || input.OwnedIdentitiesRevision <= 0 ||
		input.Project.Revision <= 0 || input.Environment.Revision <= 0 {
		return errs.New(errs.KindValidationFailed, "Blueprint runtime projection seal authority is invalid")
	}
	if err := hierarchyrecord.ValidateProject(input.Project.Record); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateEnvironment(input.Environment.Record); err != nil {
		return err
	}
	if err := validateBlueprintRuntimeComponentSeal(input); err != nil {
		return err
	}
	if input.BackupPreparation.IsZero() || projection.Backup == nil {
		return errs.New(errs.KindValidationFailed, "Blueprint runtime Backup policy authority is missing")
	}
	backup, err := blueprintplanning.PrepareBlueprintBackupPolicyPublication(
		blueprintplanning.TaskIdentity{ID: parent.ID, Target: parent.Scope.EnvironmentID},
		projection, input.BackupPreparation,
	)
	if err != nil {
		return err
	}
	defer blueprintplanning.ClearPreparedBlueprintBackupPolicyPublication(backup)
	if parent.Scope.EnvironmentID != input.Environment.Record.ID || parent.Scope.ProjectID != input.Project.Record.ID ||
		input.Environment.Record.ProjectID != input.Project.Record.ID || parent.Scope.TenantID != input.Project.Record.TenantID {
		return errs.New(errs.KindStateConflict, "Blueprint runtime projection hierarchy changed")
	}
	desired, found, err := blueprints.ReadDesiredInputRevision(ctx, repository.store,
		projection.EnvironmentID, parent.ID, input.SnapshotReadRevision)
	if err != nil {
		return err
	}
	if !found || desired.Revision != input.DesiredRootRevision || desired.Record.EnvironmentID != projection.EnvironmentID ||
		desired.Record.RevisionID != parent.ID || desired.Record.RenderGeneration != projection.RenderGeneration {
		return errs.New(errs.KindStateConflict, "Blueprint runtime captured input changed")
	}
	captured, err := projectionrecord.EncodeEnvironmentDesiredInputStorage(desired.Record)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(captured)
	clear(captured)
	if hex.EncodeToString(digest[:]) != parent.Intent.SHA256 {
		return errs.New(errs.KindStateConflict, "Blueprint runtime captured input hash changed")
	}
	identitiesKey := blueprints.EnvironmentBlueprintOwnedIdentitiesKey(projection.EnvironmentID, parent.ID)
	identitiesRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{identitiesKey},
	})
	if err != nil {
		return err
	}
	if identitiesRead == nil || len(identitiesRead.Values) != 1 || identitiesRead.Values[0] == nil ||
		identitiesRead.Values[0].ModRevision != input.OwnedIdentitiesRevision {
		return errs.New(errs.KindStateConflict, "Blueprint runtime owned identities changed")
	}
	defer etcdstore.ClearValues(identitiesRead.Values)
	identities, err := projectionrecord.DecodeEnvironmentOwnedIdentities(identitiesRead.Values[0].Value)
	if err != nil || identities.EnvironmentID != projection.EnvironmentID ||
		identities.RenderGeneration != projection.RenderGeneration ||
		identities.RevisionID != parent.ID || len(identities.Attaches) != len(input.Attaches) ||
		len(identities.Components) != len(input.ComponentReceipts) {
		return errs.New(errs.KindStateConflict, "Blueprint runtime Attach identities changed")
	}
	selected := make(map[string]string, len(identities.Attaches))
	for _, identity := range identities.Attaches {
		selected[identity.ID] = identity.Name
	}
	for _, attach := range input.Attaches {
		name, found := selected[attach.Record.ID]
		if !found || name != attach.Record.Name || attachrecord.ValidateAttachRecord(attach.Record) != nil {
			return errs.New(errs.KindStateConflict, "Blueprint runtime Attach selection changed")
		}
		delete(selected, attach.Record.ID)
	}
	selectedComponents := make(map[string]projectionrecord.OwnedIdentity, len(identities.Components))
	for _, identity := range identities.Components {
		selectedComponents[identity.ID] = identity
	}
	for _, receipt := range input.ComponentReceipts {
		identity, found := selectedComponents[receipt.Identity.ID]
		if !found || identity != receipt.Identity {
			return errs.New(errs.KindStateConflict, "Blueprint runtime Component identities changed")
		}
		delete(selectedComponents, receipt.Identity.ID)
	}
	if len(selectedComponents) != 0 {
		return errs.New(errs.KindStateConflict, "Blueprint runtime Component selection changed")
	}
	switch input.Project.Record.Kind {
	case hierarchyrecord.ProjectKindTenant:
		if input.Tenant.Revision <= 0 || hierarchyrecord.ValidateTenant(input.Tenant.Record) != nil ||
			input.Project.Record.TenantID != input.Tenant.Record.ID {
			return errs.New(errs.KindStateConflict, "Blueprint runtime projection Tenant changed")
		}
	case hierarchyrecord.ProjectKindBacking:
		if input.Tenant.Revision != 0 || input.Tenant.Record.ID != "" || input.Project.Record.TenantID != "" {
			return errs.New(errs.KindStateConflict, "Blueprint runtime projection backing owner changed")
		}
	default:
		return errs.New(errs.KindStateConflict, "Blueprint runtime projection Project kind changed")
	}
	value, err := projectionrecord.EncodePreparedEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		return err
	}
	defer clear(value)
	parentKey := workqueue.TaskKey(parent.ID)
	headKey := blueprints.EnvironmentBlueprintHeadKey(projection.EnvironmentID)
	epochKey := blueprintunits.EpochKey(projection.EnvironmentID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{parentKey}})
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "Blueprint runtime projection Task is unavailable")
	}
	defer etcdstore.ClearValues(read.Values)
	storedParent, parentErr := workqueue.DecodeTask(read.Values[0].Value)
	if parentErr != nil || storedParent.Action != workexecution.ActionApply ||
		storedParent.Stop != nil || storedParent.FinishedAt != nil || storedParent.CollectionStartedAt != nil ||
		!time.Now().UTC().Before(storedParent.Deadline) ||
		storedParent.Scope != parent.Scope || storedParent.ID != parent.ID ||
		storedParent.TargetID != parent.TargetID || storedParent.Intent != parent.Intent {
		return errs.New(errs.KindStateConflict, "Blueprint runtime projection Task authority changed")
	}
	conditions := []etcdstore.Condition{
		{Key: blueprints.EnvironmentBlueprintEffectiveProjectionKey(projection.EnvironmentID, parent.ID)},
		{Key: headKey, ModRevision: input.ExpectedHeadRevision},
		{Key: epochKey, ModRevision: input.ExpectedEpochRevision},
		{Key: parentKey, ModRevision: read.Values[0].ModRevision},
		{
			Key:         blueprints.EnvironmentBlueprintRootKey(projection.EnvironmentID, parent.ID),
			ModRevision: input.DesiredRootRevision,
		},
		{Key: identitiesKey, ModRevision: input.OwnedIdentitiesRevision},
		{Key: hierarchyrecord.ProjectKey(input.Project.Record.ID), ModRevision: input.Project.Revision},
		{Key: hierarchyrecord.EnvironmentKey(input.Environment.Record.ID), ModRevision: input.Environment.Revision},
	}
	if input.Project.Record.Kind == hierarchyrecord.ProjectKindTenant {
		conditions = append(conditions, etcdstore.Condition{
			Key: hierarchyrecord.TenantKey(input.Tenant.Record.ID), ModRevision: input.Tenant.Revision,
		})
	}
	for _, attach := range input.Attaches {
		if attach.Revision <= 0 || attach.ReadRevision != input.SnapshotReadRevision ||
			attach.Record.EnvironmentID != projection.EnvironmentID ||
			attach.Record.Status != core.AttachReady ||
			attach.Record.Operation != attachrecord.AttachOperationProvision {
			return errs.New(errs.KindValidationFailed, "Blueprint runtime Attach authority is invalid")
		}
		conditions = append(conditions, etcdstore.Condition{
			Key: attachrecord.AttachKey(attach.Record.ID), ModRevision: attach.Revision,
		})
		authority, err := repository.blueprintRuntimeAttachConditions(
			ctx,
			projection.EnvironmentID,
			attach,
			input.SnapshotReadRevision,
		)
		if err != nil {
			return err
		}
		conditions = append(conditions, authority...)
	}
	for _, component := range input.Components {
		conditions = append(conditions, etcdstore.Condition{
			Key:         componentrecord.RecordKey(component.Component.Record.Desired.ID),
			ModRevision: component.Component.Revision,
		})
		runtimeCondition := etcdstore.Condition{
			Key: componentplanning.RuntimeKey(component.Component.Record.Desired.ID),
		}
		if component.RuntimePresent {
			runtimeCondition.ModRevision = component.Runtime.Revision
		}
		conditions = append(conditions, runtimeCondition)
	}
	for _, receipt := range input.ComponentReceipts {
		candidateInput, err := componentplanning.ValidateBlueprintProxyCandidateProjection(ctx, repository.store,
			receipt.Applied.Record, receipt.Applied.Revision, input.SnapshotReadRevision, projection)
		if err != nil {
			return err
		}
		if candidateInput != nil {
			conditions = append(conditions, *candidateInput)
		}
		conditions = append(conditions, etcdstore.Condition{
			Key:         blueprintunits.AppliedKey(projection.EnvironmentID, receipt.Applied.Record.Target),
			ModRevision: receipt.Applied.Revision,
		})
	}
	mutations := []etcdstore.Mutation{{
		Type:  etcdstore.MutationPut,
		Key:   blueprints.EnvironmentBlueprintEffectiveProjectionKey(projection.EnvironmentID, parent.ID),
		Value: value,
	}}
	conditions = append(conditions, backup.Conditions()...)
	mutations = append(mutations, backup.Mutations()...)
	result, err := executeEnvironmentBlueprintTransaction(ctx, repository.transactions, conditions, mutations)
	etcdstore.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Blueprint runtime projection seal raced")
	}
	return nil
}

func validateBlueprintRuntimeComponentSeal(input BlueprintRuntimeProjectionSeal) error {
	projection := input.Projection
	if len(input.Components) != len(projection.Components) {
		return errs.New(errs.KindValidationFailed, "Blueprint runtime Component seal is incomplete")
	}
	byID := make(map[string]BlueprintRuntimeComponent, len(input.Components))
	expectedSources := make([]projectionrecord.ManagedComponentRuntimeSource, 0, len(input.Components))
	for index, authority := range input.Components {
		component := authority.Component
		configuration := componentrecord.CloneRecord(component.Record)
		configuration.Runtime.Healthy = false
		if component.Revision <= 0 || component.ReadRevision != input.SnapshotReadRevision ||
			component.Record.Desired.Owner != core.ComponentOwnerEnvironment ||
			component.Record.Desired.OwnerID != projection.EnvironmentID ||
			componentrecord.ValidateRecord(component.Record) != nil ||
			index >= len(projection.Components) ||
			!componentrecord.EqualRecord(configuration, projection.Components[index]) {
			return errs.New(errs.KindValidationFailed, "Blueprint runtime Component authority is invalid")
		}
		if _, duplicate := byID[component.Record.Desired.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Blueprint runtime Component authority is duplicated")
		}
		byID[component.Record.Desired.ID] = authority
		if !authority.RuntimePresent {
			if component.Record.Desired.Enabled || len(component.Record.Runtime.GeneratedServices) != 0 ||
				component.Record.Runtime.PinnedIPv4 != "" {
				return errs.New(errs.KindStateConflict, "enabled Component acknowledged runtime is missing")
			}
			continue
		}
		if authority.Runtime.Revision <= 0 || authority.Runtime.ReadRevision != input.SnapshotReadRevision ||
			componentplanning.ValidateRuntime(authority.Runtime.Record) != nil {
			return errs.New(errs.KindValidationFailed, "Blueprint runtime Component record is invalid")
		}
		acknowledged := authority.Runtime.Record.Component
		acknowledged.Runtime.Healthy = component.Record.Runtime.Healthy
		if authority.Runtime.Record.EnvironmentID != projection.EnvironmentID ||
			!componentrecord.EqualRecord(acknowledged, component.Record) {
			return errs.New(errs.KindStateConflict, "Blueprint runtime Component record changed")
		}
		if !component.Record.Desired.Enabled {
			continue
		}
		artifact := &agentpb.ComposeArtifact{}
		if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
			authority.Runtime.Record.Artifact, artifact,
		); err != nil {
			return errs.New(errs.KindStateConflict, "Blueprint managed Component runtime artifact changed")
		}
		source, err := projectionrecord.PrepareManagedComponentRuntimeSource(
			projectionrecord.EnvironmentComposeProjection{
				EnvironmentID:    projection.EnvironmentID,
				RevisionID:       authority.Runtime.Record.RevisionID,
				RenderGeneration: authority.Runtime.Record.RenderGeneration,
				ComposeArtifact:  authority.Runtime.Record.Artifact,
			},
			authority.Runtime.Record.Component,
			artifact,
		)
		if err != nil {
			return err
		}
		expectedSources = append(expectedSources, source)
	}
	sort.Slice(expectedSources, func(left, right int) bool {
		return expectedSources[left].ComponentKind < expectedSources[right].ComponentKind
	})
	if len(expectedSources) != len(projection.ManagedComponentRuntimeSources) {
		return errs.New(errs.KindStateConflict, "Blueprint managed Component runtime sources changed")
	}
	for index := range expectedSources {
		if expectedSources[index] != projection.ManagedComponentRuntimeSources[index] {
			return errs.New(errs.KindStateConflict, "Blueprint managed Component runtime source changed")
		}
	}
	seenReceipts := make(map[string]struct{}, len(input.ComponentReceipts))
	for _, selected := range input.ComponentReceipts {
		authority, found := byID[selected.Identity.ID]
		receipt := selected.Applied
		if !found || selected.Identity.Name == "" || selected.Unit.Target.Kind != ids.KindComponent ||
			selected.Unit.Target.ID != selected.Identity.ID || selected.Unit.Removal ||
			selected.Unit.Fingerprint == "" || receipt.Revision <= 0 ||
			receipt.ReadRevision != input.SnapshotReadRevision ||
			receipt.Record.EnvironmentID != projection.EnvironmentID ||
			receipt.Record.Target != selected.Unit.Target || receipt.Record.State != blueprintunits.Applied ||
			receipt.Record.Fingerprint != selected.Unit.Fingerprint ||
			!blueprintRuntimeComponentReceiptMatches(receipt, authority) {
			return errs.New(errs.KindStateConflict, "Blueprint Component receipt authority changed")
		}
		if _, duplicate := seenReceipts[selected.Identity.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Blueprint Component receipt is duplicated")
		}
		seenReceipts[selected.Identity.ID] = struct{}{}
	}
	return nil
}

func blueprintRuntimeComponentReceiptMatches(
	applied etcdstore.Versioned[blueprintunits.AppliedRecord],
	authority BlueprintRuntimeComponent,
) bool {
	receipt := applied.Record
	if receipt.Execution != nil {
		return authority.RuntimePresent &&
			*receipt.Execution == authority.Runtime.Record.Execution &&
			receipt.ParentTaskID == authority.Runtime.Record.RevisionID
	}
	component := authority.Component
	if component.Record.Desired.Enabled || len(component.Record.Runtime.GeneratedServices) != 0 ||
		component.Record.Runtime.PinnedIPv4 != "" || component.Record.Runtime.Healthy {
		return false
	}
	runtimeRevision := int64(0)
	if authority.RuntimePresent {
		runtimeRevision = authority.Runtime.Revision
	}
	return blueprintunits.ControllerDisabledComponentPublicationMatches(applied, component.Revision, runtimeRevision)
}
