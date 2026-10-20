package etcd

import (
	"context"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ServiceRemovalPublication struct {
	Tenant      keyvalue.Versioned[hierarchy.TenantRecord]
	Project     keyvalue.Versioned[hierarchy.ProjectRecord]
	Environment keyvalue.Versioned[hierarchy.EnvironmentRecord]
	Service     keyvalue.Versioned[services.ServiceRecord]
	Baseline    keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]
	Candidate   environmentprojection.EnvironmentComposeProjection
	Claim       blueprints.EnvironmentBlueprintStageClaim
	Input       serviceremovalwork.Input
	Deadline    time.Time
	Marker      idempotency.IdempotencyMarker
}

// PublishServiceRemoval protects the sealed desired revision and captured
// runtime while publishing native work. The visible Service/head remain until
// execution proves the workload absent and finalization commits atomically.
func (repository *ServiceRepository) PublishServiceRemoval(
	ctx context.Context,
	request ServiceRemovalPublication,
) (_ IdempotencyTransactionResult, resultErr error) {
	input, marker, claim := request.Input, request.Marker, request.Claim
	input, configurationGuards, err := repository.captureRemovalConfiguration(ctx, input, request.Baseline.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateServiceLifecycleHierarchy(&request.Tenant, request.Project, request.Environment, request.Service); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := serviceremovalwork.ValidateCandidate(input, request.Baseline.Record, request.Candidate); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if input.Scope != (workexecution.Scope{TenantID: request.Tenant.Record.ID, ProjectID: request.Project.Record.ID, EnvironmentID: request.Environment.Record.ID}) ||
		request.Baseline.Revision != input.HeadRevision ||
		claim.TaskID != input.TaskID ||
		claim.RevisionID != input.DesiredRevisionID ||
		!claim.CreatedAt.Equal(input.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Service removal sources do not describe one resource snapshot",
		)
	}
	want := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetService, ID: input.ServiceID}
	if idempotency.ValidateIdempotencyMarker(marker) != nil || marker.Kind != idempotency.IdempotencyMarkerTask ||
		marker.State != idempotency.IdempotencyMarkerPending || marker.TaskID != input.TaskID || marker.Locator != claim.Locator ||
		marker.Locator.Method != http.MethodDelete || marker.Locator.Route != "/services/{id}" ||
		marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment || marker.Locator.ScopeID != input.Scope.EnvironmentID ||
		marker.ReplayTarget == nil || *marker.ReplayTarget != want || !idempotency.ValidTaskResponse(marker.Response, input.TaskID) ||
		!blueprints.SameBlueprintProtectedIntent(
			marker.Intent,
			claim.Intent,
		) || !marker.CreatedAt.Equal(input.CreatedAt) || !marker.UpdatedAt.Equal(input.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Service removal replay marker differs from its action",
		)
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	fence, err := environmentfence.LoadOrdinary(
		ctx,
		repository.store,
		input.Scope.EnvironmentID,
		request.Baseline.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	references, err := repository.validateServiceRemovalReferences(ctx, request.Service, request.Baseline)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	dns, err := dnsrecords.NewReader(repository.store).
		PrepareServiceWithdrawal(ctx, input.ServiceID, request.Baseline.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	scripts, err := scriptsourceevidence.PrepareServiceScriptAbsence(
		ctx,
		repository.store,
		input.ServiceID,
		request.Baseline.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	publication, err := composeHierarchyRepository(repository.store).prepareEnvironmentDirectPublication(ctx, claim,
		blueprints.EnvironmentDesiredRevisionIdentity{
			EnvironmentID: input.Scope.EnvironmentID,
			RevisionID:    input.DesiredRevisionID,
		},
		request.Candidate, marker, input.HeadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(publication.publishedDescriptor)
	if hex.EncodeToString(publication.seal.ProjectionSHA256[:]) != input.DesiredSHA256 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"Service removal sealed desired bytes changed",
		)
	}
	// Persist the prepared revision without selecting it as the current head.
	// Finalization can then publish precisely this candidate, even after restart.
	identities, err := prepareEnvironmentDesiredIdentityPublication(ctx, repository.store, request.Candidate,
		claim.SourceKind, input.HeadRevision, request.Baseline.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer identities.clear()
	defer func() {
		if resultErr != nil {
			resultErr = repository.abandonRemovalSecretPreparation(ctx, input, resultErr)
		}
	}()
	input, pinChange, err := repository.prepareRemovalSecretPins(
		ctx,
		input,
		request.Baseline.ReadRevision,
		append(
			configurationGuards,
			keyvalue.Condition{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
			keyvalue.Condition{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
			keyvalue.Condition{Key: workqueue.TaskKey(input.TaskID)},
			keyvalue.Condition{Key: workqueue.ItemKey(input.WorkItemID)},
			keyvalue.Condition{Key: serviceremovalwork.InputKey(input.WorkItemID)},
		),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(pinChange.Mutations)
	_, change, err := serviceremovalwork.PrepareAdmission(input, request.Service, request.Deadline, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epoch.Value)
	conditions := append(change.Conditions, references.Conditions()...)
	conditions = append(conditions, configurationGuards...)
	conditions = append(conditions, pinChange.Conditions...)
	conditions = append(conditions, dns...)
	conditions = append(conditions, scripts...)
	conditions = append(conditions, identities.conditions...)
	conditions = append(conditions, fence.TransactionConditions()...)
	conditions = append(
		conditions,
		keyvalue.Condition{
			Key:         hierarchy.EnvironmentKey(request.Environment.Record.ID),
			ModRevision: request.Environment.Revision,
		},
		keyvalue.Condition{Key: hierarchy.ProjectKey(request.Project.Record.ID), ModRevision: request.Project.Revision},
		keyvalue.Condition{
			Key:         blueprints.EnvironmentBlueprintHeadKey(input.Scope.EnvironmentID),
			ModRevision: input.HeadRevision,
		},
		keyvalue.Condition{
			Key:         blueprints.EnvironmentBlueprintRootKey(input.Scope.EnvironmentID, input.DesiredRevisionID),
			ModRevision: publication.rootRevision,
		},
		keyvalue.Condition{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		keyvalue.Condition{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
	)
	mutations := append(
		change.Mutations,
		epoch,
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   publication.descriptorKey,
			Value: publication.publishedDescriptor,
		},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: publication.locatorKey},
	)
	mutations = append(mutations, identities.mutations...)
	mutations = append(mutations, pinChange.Mutations...)
	conditions, err = mergeServiceRemovalConditions(conditions)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(
		&request.Tenant,
		request.Project,
		request.Environment,
		taskjournal.TaskActorOperator,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	classify := idempotencyPlanClassifier(func(_ int64, _ []*keyvalue.KeyValue) error {
		return errs.New(errs.KindStateConflict, "Service or its references changed before removal; retry the action")
	})
	conditions, classify, err = prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask, conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	receipts, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return receipts.Apply(ctx, marker, plan)
}
