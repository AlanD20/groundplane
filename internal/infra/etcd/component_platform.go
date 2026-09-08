package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const platformComponentOwnerPrefix = "/v1/indexes/components/by-owner/platform/"
const componentObservationPrefix = "/v1/observed/platform/components/"

func platformComponentOwnerKey(id string) string { return platformComponentOwnerPrefix + id }
func platformComponentKindKey(kind core.ComponentKind) string {
	return "/v1/indexes/components/by-kind/platform/" + encodeDynamicSegment(string(kind))
}
func componentObservationKey(id string) string { return componentObservationPrefix + id }

// CreatePlatformComponent atomically publishes the platform component and indexes.
func (repository *ComponentRepository) CreatePlatformComponent(
	ctx context.Context,
	record ComponentRecord,
) (Versioned[ComponentRecord], error) {
	return repository.createPlatformComponent(ctx, record, false)
}

func (repository *ComponentRepository) createPlatformComponent(
	ctx context.Context,
	record ComponentRecord,
	bootstrap bool,
) (Versioned[ComponentRecord], error) {
	if err := validatePlatformComponentRecord(record); err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	if bootstrap && (record.Desired.ID != platformCoreDNSComponentID ||
		record.Desired.Kind != core.ComponentKindCoreDNS || !record.Desired.Enabled ||
		len(record.Runtime.GeneratedServices) != 0 || record.Runtime.PinnedIPv4 != "" || record.Runtime.Healthy) {
		return Versioned[ComponentRecord]{}, errs.New(
			errs.KindValidationFailed,
			"platform Component bootstrap record is invalid",
		)
	}
	value, err := encodeComponentRecord(record)
	if err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	defer clear(value)
	conditions := []Condition{
		{Key: componentKey(record.Desired.ID)},
		{Key: platformComponentOwnerKey(record.Desired.ID)},
		{
			Key: platformComponentKindKey(record.Desired.Kind),
		},
		{Key: deletionTombstoneKey("component", record.Desired.ID)},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: componentKey(record.Desired.ID), Value: value},
		{Type: MutationPut, Key: platformComponentOwnerKey(record.Desired.ID), Value: []byte(record.Desired.ID)},
		{Type: MutationPut, Key: platformComponentKindKey(record.Desired.Kind), Value: []byte(record.Desired.ID)},
		componentWriteFenceMutation(record.Desired.ID),
	}
	if bootstrap {
		conditions = append(conditions, Condition{Key: platformComponentBootstrapKey(record.Desired.ID)})
		mutations = append(mutations, Mutation{
			Type: MutationPut, Key: platformComponentBootstrapKey(record.Desired.ID),
			Value: []byte(record.Desired.ID),
		})
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[ComponentRecord]{}, errs.New(
			errs.KindStateConflict,
			"platform Component stable identity is already in use",
		)
	}
	return Versioned[ComponentRecord]{Record: record, Revision: result.Revision, ReadRevision: result.Revision}, nil
}

func (repository *ComponentRepository) ListPlatformComponents(
	ctx context.Context,
	request PageRequest,
) (Page[ComponentRecord], error) {
	return listIndexPage(ctx, repository.store, "components", "platform", "", platformComponentOwnerPrefix,
		componentKey, ids.KindComponent, request, decodeComponentRecord,
		func(record ComponentRecord) string { return record.Desired.ID },
		func(record ComponentRecord) bool {
			return record.Desired.Owner == core.ComponentOwnerPlatform && record.Desired.OwnerID == ""
		})
}

func (repository *ComponentRepository) ReplacePlatformDesired(
	ctx context.Context,
	current Versioned[ComponentRecord],
	desired core.Component,
) (Versioned[ComponentRecord], error) {
	replacement, err := ReplaceComponentDesired(current.Record, desired)
	if err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	return repository.replacePlatform(ctx, current, replacement)
}

func (repository *ComponentRepository) ReplacePlatformRuntime(
	ctx context.Context,
	current Versioned[ComponentRecord],
	generatedServices []string,
	pinnedIPv4 string,
	healthy bool,
) (Versioned[ComponentRecord], error) {
	replacement, err := SetComponentRuntime(current.Record, generatedServices, pinnedIPv4, healthy)
	if err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	return repository.replacePlatform(ctx, current, replacement)
}

func (repository *ComponentRepository) replacePlatform(
	ctx context.Context,
	current Versioned[ComponentRecord],
	replacement ComponentRecord,
) (Versioned[ComponentRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	if err := validatePlatformComponentRecord(current.Record); err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	if err := validatePlatformComponentRecord(replacement); err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision ||
		current.Record.Desired.ID != replacement.Desired.ID {
		return Versioned[ComponentRecord]{}, errs.New(
			errs.KindValidationFailed,
			"platform Component version metadata is invalid",
		)
	}
	indexes, err := repository.store.GetMany(
		ctx,
		GetManyRequest{
			Keys: []string{
				platformComponentOwnerKey(current.Record.Desired.ID),
				platformComponentKindKey(current.Record.Desired.Kind),
			},
			Revision: current.ReadRevision,
		},
	)
	if err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != current.Record.Desired.ID ||
		string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return Versioned[ComponentRecord]{}, errs.New(
			errs.KindInternal,
			"platform Component indexes are missing or corrupt",
		)
	}
	value, err := encodeComponentRecord(replacement)
	if err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	defer clear(value)
	result, err := repository.store.Transact(ctx, []Condition{
		{Key: componentKey(current.Record.Desired.ID), ModRevision: current.Revision},
		{Key: platformComponentOwnerKey(current.Record.Desired.ID), ModRevision: indexes.Values[0].ModRevision},
		{Key: platformComponentKindKey(current.Record.Desired.Kind), ModRevision: indexes.Values[1].ModRevision},
		{Key: deletionTombstoneKey("component", current.Record.Desired.ID)},
	}, []Mutation{
		{Type: MutationPut, Key: componentKey(replacement.Desired.ID), Value: value},
		componentWriteFenceMutation(replacement.Desired.ID),
	})
	if err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[ComponentRecord]{}, stateConflict("platform component", current.Record.Desired.ID)
	}
	return Versioned[ComponentRecord]{
		Record:       replacement,
		Revision:     result.Revision,
		ReadRevision: result.Revision,
	}, nil
}
func validatePlatformComponentRecord(record ComponentRecord) error {
	if err := validateComponentRecord(record); err != nil {
		return err
	}
	if record.Desired.Owner != core.ComponentOwnerPlatform || record.Desired.OwnerID != "" {
		return errs.New(errs.KindValidationFailed, "platform Component ownership is invalid")
	}
	return nil
}

// ComponentObservationRecord is replaceable Agent evidence, separate from desired state.
type ComponentObservationRecord struct {
	ComponentID         string                              `json:"component_id"`
	ServiceID           string                              `json:"service_id"`
	PlanID              string                              `json:"plan_id"`
	ComposeArtifactID   string                              `json:"compose_artifact_id"`
	ComposeArtifact     *agentpb.ComposeArtifact            `json:"compose_artifact,omitempty"`
	Enabled             bool                                `json:"enabled"`
	Healthy             bool                                `json:"healthy"`
	DesiredGeneration   uint64                              `json:"desired_generation"`
	RenderGeneration    uint64                              `json:"render_generation"`
	AgentID             string                              `json:"agent_id"`
	AgentGeneration     uint64                              `json:"agent_generation"`
	BaselineGeneration  uint64                              `json:"baseline_generation"`
	OwnershipGeneration uint64                              `json:"ownership_generation"`
	CorefileSHA256      string                              `json:"corefile_sha256,omitempty"`
	InputSHA256         string                              `json:"input_sha256,omitempty"`
	ObservedAt          time.Time                           `json:"observed_at"`
	TaskID              string                              `json:"task_id"`
	StepID              string                              `json:"step_id"`
	Revision            uint64                              `json:"revision"`
	PredecessorTaskID   string                              `json:"predecessor_task_id,omitempty"`
	DNSResolverProof    *TaskDNSResolverObservationEvidence `json:"dns_resolver_proof,omitempty"`
}

func validateComponentObservation(record ComponentObservationRecord) error {
	if ids.Validate(ids.KindComponent, record.ComponentID) != nil ||
		ids.Validate(ids.KindService, record.ServiceID) != nil ||
		ids.Validate(ids.KindPlan, record.PlanID) != nil ||
		ids.Validate(ids.KindConfig, record.ComposeArtifactID) != nil ||
		ids.Validate(ids.KindAgent, record.AgentID) != nil ||
		ids.Validate(ids.KindTask, record.TaskID) != nil ||
		ids.Validate(ids.KindStep, record.StepID) != nil ||
		record.Revision == 0 ||
		(record.PredecessorTaskID != "" && ids.Validate(ids.KindTask, record.PredecessorTaskID) != nil) ||
		(record.Revision == 1 && record.PredecessorTaskID != "") ||
		(record.Revision > 1 && record.PredecessorTaskID == "") ||
		record.DesiredGeneration == 0 ||
		record.RenderGeneration == 0 ||
		record.AgentGeneration == 0 ||
		record.OwnershipGeneration == 0 ||
		!validMarkerTime(record.ObservedAt) {
		return errs.New(errs.KindValidationFailed, "Component observation identity or generation is invalid")
	}
	if record.Enabled {
		if !validSHA256(record.CorefileSHA256) || !validSHA256(record.InputSHA256) ||
			record.DNSResolverProof == nil || validateTaskResult(TaskResultRecord{
			Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone,
			DNSResolverCandidateObservation: record.DNSResolverProof,
		}, nil, TaskStatusCompleted) != nil || record.DNSResolverProof.ComponentID != record.ComponentID ||
			record.DNSResolverProof.ServiceID != record.ServiceID ||
			record.DNSResolverProof.RenderGeneration != record.RenderGeneration ||
			record.DNSResolverProof.ArtifactSHA256 != record.CorefileSHA256 ||
			record.ComposeArtifact == nil || !platformComponentArtifactMatches(
			record.ComposeArtifact,
			record.ComposeArtifactID,
			record.ServiceID,
			record.DNSResolverProof.ImageConfigDigest,
		) {
			return errs.New(errs.KindValidationFailed, "enabled Component observation digests are invalid")
		}
	} else if record.CorefileSHA256 != "" && !validSHA256(record.CorefileSHA256) ||
		record.InputSHA256 != "" || record.DNSResolverProof != nil || record.ComposeArtifact != nil {
		return errs.New(errs.KindValidationFailed, "disabled Component observation carries rendered digests")
	}
	return nil
}
func encodeComponentObservation(record ComponentObservationRecord) ([]byte, error) {
	if err := validateComponentObservation(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("component_observation", record)
}
func decodeComponentObservation(value []byte) (ComponentObservationRecord, error) {
	record, err := decodeEnvelope[ComponentObservationRecord](value, "component_observation")
	if err != nil || validateComponentObservation(record) != nil {
		return ComponentObservationRecord{}, errs.New(errs.KindInternal, "Component observation is corrupt")
	}
	return record, nil
}

// PutPlatformComponentObservation fences the component and prior observation in one transaction.
func (repository *ComponentRepository) PutPlatformComponentObservation(
	ctx context.Context,
	record ComponentObservationRecord,
	expectedComponentRevision int64,
	expectedObservationRevision int64,
) (Versioned[ComponentObservationRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ComponentObservationRecord]{}, err
	}
	if err := validateComponentObservation(record); err != nil {
		return Versioned[ComponentObservationRecord]{}, err
	}
	component, err := repository.GetComponent(ctx, record.ComponentID)
	if err != nil {
		return Versioned[ComponentObservationRecord]{}, err
	}
	if component.Record.Desired.Owner != core.ComponentOwnerPlatform || component.Record.Desired.OwnerID != "" ||
		component.Revision != expectedComponentRevision {
		return Versioned[ComponentObservationRecord]{}, stateConflict("platform component", record.ComponentID)
	}
	current, err := repository.store.Get(ctx, componentObservationKey(record.ComponentID))
	if err != nil {
		return Versioned[ComponentObservationRecord]{}, err
	}
	if current == nil || revisionChanged(current.Entry, expectedObservationRevision) {
		return Versioned[ComponentObservationRecord]{}, stateConflict(
			"platform component observation",
			record.ComponentID,
		)
	}
	value, err := encodeComponentObservation(record)
	if err != nil {
		return Versioned[ComponentObservationRecord]{}, err
	}
	defer clear(value)
	result, err := repository.store.Transact(
		ctx,
		[]Condition{
			{Key: componentKey(record.ComponentID), ModRevision: expectedComponentRevision},
			{Key: componentObservationKey(record.ComponentID), ModRevision: expectedObservationRevision},
			{Key: deletionTombstoneKey("component", record.ComponentID)},
		},
		[]Mutation{{Type: MutationPut, Key: componentObservationKey(record.ComponentID), Value: value}},
	)
	if err != nil {
		return Versioned[ComponentObservationRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[ComponentObservationRecord]{}, stateConflict(
			"platform component observation",
			record.ComponentID,
		)
	}
	return Versioned[ComponentObservationRecord]{
		Record:       record,
		Revision:     result.Revision,
		ReadRevision: result.Revision,
	}, nil
}

func (repository *ComponentRepository) GetPlatformComponentObservation(
	ctx context.Context,
	componentID string,
) (Versioned[ComponentObservationRecord], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ComponentObservationRecord]{}, false, err
	}
	if err := ids.Validate(ids.KindComponent, componentID); err != nil {
		return Versioned[ComponentObservationRecord]{}, false, errs.New(errs.KindValidationFailed, err.Error())
	}
	result, err := repository.store.Get(ctx, componentObservationKey(componentID))
	if err != nil {
		return Versioned[ComponentObservationRecord]{}, false, err
	}
	if result == nil {
		return Versioned[ComponentObservationRecord]{}, false, errs.New(
			errs.KindInternal,
			"Component observation read is empty",
		)
	}
	if result.Entry == nil {
		return Versioned[ComponentObservationRecord]{ReadRevision: result.ReadRevision}, false, nil
	}
	record, err := decodeComponentObservation(result.Entry.Value)
	if err != nil || record.ComponentID != componentID {
		return Versioned[ComponentObservationRecord]{}, false, errs.New(
			errs.KindInternal,
			"Component observation is corrupt",
		)
	}
	return Versioned[ComponentObservationRecord]{
		Record:       record,
		Revision:     result.Entry.ModRevision,
		ReadRevision: result.ReadRevision,
	}, true, nil
}
