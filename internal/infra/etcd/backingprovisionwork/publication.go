package backingprovisionwork

import (
	"context"
	"encoding/hex"
	"errors"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type CreationCapture struct {
	PlanID, TargetName, ConfigurationID string
	Environment                         hierarchy.EnvironmentRecord
	Procedure                           *agentpb.BackingProvisionInput
	Files                               []taskmaterialization.Record
	Hook                                *taskconfiguration.BackingHookEncryptedInputs
}

// Publication includes inert captures and the activation fragment that MUST
// join facade, native Task, desired head and idempotency publication atomically.
type Publication struct {
	Input  Input
	Change workqueue.Change
	store  workqueue.Store
	pins   *tasksecretpins.Repository
}

func (repository *Repository) PrepareCreation(
	ctx context.Context,
	capture CreationCapture,
) (_ Publication, resultErr error) {
	if ctx == nil || capture.Procedure == nil || capture.Procedure.Configuration != nil ||
		capture.Procedure.AfterStart != nil &&
			capture.Procedure.AfterStart.EncryptedInputs != nil {
		return Publication{}, invalid("backing creation capture must select configuration and encrypted inputs once")
	}
	procedure := proto.CloneOf(capture.Procedure)
	if err := validateConfigurationSources(procedure, capture.Files); err != nil {
		return Publication{}, err
	}
	configurations, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return Publication{}, err
	}
	reference, err := configurations.Stage(ctx, runtimeconfiguration.Snapshot{ID: capture.ConfigurationID,
		EnvironmentID: procedure.EnvironmentId, Generation: procedure.RenderGeneration, Files: capture.Files})
	if err != nil {
		return Publication{}, err
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil {
		return Publication{}, err
	}
	procedure.Configuration = &agentpb.WorkConfigurationReference{ConfigurationId: reference.ID,
		EnvironmentId: reference.EnvironmentID, Generation: reference.Generation, Sha256: digest}
	publication := Publication{
		Input: Input{PlanID: capture.PlanID, TargetName: capture.TargetName, Environment: capture.Environment},
		store: repository.store,
	}
	defer func() {
		if resultErr != nil {
			keyvalue.ClearMutationValues(publication.Change.Mutations)
			clear(publication.Input.Wire)
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			resultErr = errors.Join(resultErr, publication.AbandonUnpublished(cleanup))
		}
	}()
	if envelope := capture.Hook; envelope != nil {
		if procedure.AfterStart == nil || taskconfiguration.ValidateBackingHookEncryptedInputs(*envelope) != nil ||
			envelope.OperationID != procedure.OperationId {
			return Publication{}, invalid("backing creation hook envelope has no matching after-start command")
		}
		digest, err := hex.DecodeString(envelope.CiphertextSHA256)
		if err != nil {
			return Publication{}, err
		}
		procedure.AfterStart.EncryptedInputs = &agentpb.WorkInputReference{
			InputId: procedure.OperationId,
			Sha256:  digest,
		}
		publication.Input.Hook = &HookSources{Secrets: slices.Clone(envelope.SecretSources)}
		if len(envelope.SecretSources) != 0 {
			pins, err := tasksecretpins.NewEtcdRepository(repository.store, procedure.ProjectId)
			if err != nil {
				return Publication{}, err
			}
			publication.pins = pins
			prepared, err := pins.PrepareOwned(
				ctx,
				procedure.OperationId,
				procedure.TaskId,
				procedure.WorkItemId,
				envelope.SecretSources,
				[]tasksecretpins.Condition{
					{Key: workqueue.TaskKey(procedure.TaskId)},
					{Key: workqueue.ItemKey(procedure.WorkItemId)},
					{Key: InputKey(procedure.WorkItemId)},
				},
			)
			if err != nil {
				return Publication{}, err
			}
			binding, err := prepared.Binding()
			if err != nil {
				return Publication{}, err
			}
			publication.Input.Hook.Pins = &binding
			activation, err := tasksecretpins.Activation(prepared)
			if err != nil {
				return Publication{}, err
			}
			conditions, mutations, err := tasksecretpins.EtcdFragment(activation)
			if err != nil {
				activation.Clear()
				return Publication{}, err
			}
			publication.Change.Conditions = append(publication.Change.Conditions, conditions...)
			publication.Change.Mutations = append(publication.Change.Mutations, mutations...)
		}
		value, err := taskconfiguration.EncodeBackingHookEncryptedInputs(*envelope)
		if err != nil {
			return Publication{}, err
		}
		key := taskconfiguration.BackingHookTaskInputKey(procedure.OperationId)
		publication.Change.Conditions = append(publication.Change.Conditions, keyvalue.Condition{Key: key})
		publication.Change.Mutations = append(
			publication.Change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value},
		)
	}
	publication.Input.Wire, err = (proto.MarshalOptions{Deterministic: true}).Marshal(procedure)
	if err != nil {
		return Publication{}, err
	}
	change, err := PreparePublication(publication.Input)
	if err != nil {
		return Publication{}, err
	}
	publication.Change.Conditions = append(publication.Change.Conditions, change.Conditions...)
	publication.Change.Mutations = append(publication.Change.Mutations, change.Mutations...)
	return publication, nil
}

// AbandonUnpublished never revokes an accepted or uncertain publication. The
// pin repository also compares its own preparing/sealed root before removal.
func (publication Publication) AbandonUnpublished(ctx context.Context) error {
	if publication.pins == nil {
		return nil
	}
	binding := publication.Input.Hook.Pins
	if binding == nil {
		// PrepareOwned may have failed before producing its binding. Its
		// operation id is still in the captured Secret-source membership.
		if len(publication.Input.Hook.Secrets) == 0 {
			return invalid("backing creation pin preparation lost its operation identity")
		}
		return publication.pins.Abandon(ctx, publication.Input.Hook.Secrets[0].OperationID)
	}
	keys := []string{
		workqueue.TaskKey(binding.TaskID),
		workqueue.ItemKey(binding.WorkItemID),
		InputKey(binding.WorkItemID),
	}
	read, err := publication.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != len(keys) {
		return invalid("backing creation publication absence cannot be established")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value != nil {
			if value.Key != keys[index] || value.ModRevision <= 0 {
				return invalid("backing creation publication absence has invalid evidence")
			}
			return nil
		}
	}
	return publication.pins.Abandon(ctx, binding.OperationID)
}

func (publication Publication) Clear() {
	clear(publication.Input.Wire)
	keyvalue.ClearMutationValues(publication.Change.Mutations)
}

// ConfigurationConditions keeps staged sources exact and proves initial head
// absence. It does not acknowledge file application during acceptance.
func (publication Publication) ConfigurationConditions(
	ctx context.Context,
	revision int64,
) ([]keyvalue.Condition, error) {
	procedure, err := publication.Input.Procedure()
	if err != nil {
		return nil, err
	}
	configuration := procedure.Configuration
	sources, err := runtimeconfiguration.New(publication.store)
	if err != nil {
		return nil, err
	}
	guards, head, err := sources.PrepareInitialAcknowledgement(ctx, runtimeconfiguration.Reference{
		ID: configuration.ConfigurationId, EnvironmentID: configuration.EnvironmentId, Generation: configuration.Generation,
		SHA256: hex.EncodeToString(configuration.Sha256)}, revision)
	clear(head.Value)
	if err != nil {
		return nil, err
	}
	conditions := make([]keyvalue.Condition, 0, len(guards))
	for _, guard := range guards {
		conditions = append(conditions, keyvalue.Condition{Key: guard.Key, ModRevision: guard.ModRevision})
	}
	return conditions, nil
}
