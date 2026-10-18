package componentwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareAppliedState publishes only the acknowledged runtime and configuration.
// The operation owner joins Blueprint receipts, reservation cleanup and queue
// completion. This method never rewrites the operator's current desired record.
func (repository *InputRepository) PrepareAppliedState(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.ComponentWorkInput, at time.Time,
) (Capture, workqueue.Change, error) {
	capture, proof, err := repository.PrepareCapturedCompletion(ctx, view, input, at)
	if err != nil {
		return Capture{}, workqueue.Change{}, err
	}
	runtime, err := AppliedRuntime(view, input, capture)
	if err != nil {
		keyvalue.ClearMutationValues(proof.Mutations)
		return Capture{}, workqueue.Change{}, err
	}
	defer clear(runtime.Artifact)
	route, err := repository.settleRoute(ctx, view.Work, capture.Route)
	if err != nil {
		keyvalue.ClearMutationValues(proof.Mutations)
		return Capture{}, workqueue.Change{}, err
	}
	proof.Conditions = append(proof.Conditions, route.Conditions...)
	proof.Mutations = append(proof.Mutations, route.Mutations...)
	addresses, err := repository.settleAddresses(ctx, view.Work, capture, runtime.Component)
	if err != nil {
		keyvalue.ClearMutationValues(proof.Mutations)
		return Capture{}, workqueue.Change{}, err
	}
	proof.Conditions = append(proof.Conditions, addresses.Conditions...)
	proof.Mutations = append(proof.Mutations, addresses.Mutations...)
	value, err := recordcodec.Encode("blueprint-component-runtime", runtime)
	if err != nil {
		keyvalue.ClearMutationValues(proof.Mutations)
		return Capture{}, workqueue.Change{}, err
	}
	configuration, err := repository.configurationAcknowledgement(ctx, view.Work, capture.Configuration)
	if err != nil {
		clear(value)
		keyvalue.ClearMutationValues(proof.Mutations)
		return Capture{}, workqueue.Change{}, err
	}
	key := componentplanning.RuntimeKey(input.ComponentId)
	proof.Conditions = append(proof.Conditions, keyvalue.Condition{Key: key, ModRevision: capture.RuntimeRevision})
	proof.Conditions = append(proof.Conditions, configuration.Conditions...)
	proof.Mutations = append(proof.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	proof.Mutations = append(proof.Mutations, configuration.Mutations...)
	return capture, proof, nil
}

func (repository *InputRepository) configurationAcknowledgement(ctx context.Context, view workqueue.View,
	capture runtimeconfiguration.WorkCapture,
) (workqueue.Change, error) {
	sources, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions, err := sources.WorkConditions(ctx, capture, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	change := workqueue.Change{}
	for _, condition := range conditions {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: condition.Key, ModRevision: condition.ModRevision})
	}
	if capture.Prior != nil && capture.Current == *capture.Prior {
		return change, nil
	}
	value, err := runtimeconfiguration.EncodeReference(capture.Current)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
		Key: runtimeconfiguration.HeadKey(capture.Current.EnvironmentID), Value: value})
	return change, nil
}
