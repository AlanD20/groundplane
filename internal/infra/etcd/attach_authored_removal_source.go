package etcd

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareAuthoredAttachRuntime binds the removal plan to the exact
// acknowledged runtimes captured with its applied projection. It performs no
// latest-state read.
func PrepareAuthoredAttachRuntime(
	plan *agentpb.ExecutionPlan,
	sources []etcdstore.Versioned[serviceruntimerecord.Record],
) (serviceruntimerecord.AttachPreparation, error) {
	sealed, err := executionplan.Validate(plan)
	if err != nil {
		return serviceruntimerecord.AttachPreparation{}, err
	}
	if sealed.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_DETACH || len(sealed.GetArtifacts()) != 1 {
		return serviceruntimerecord.AttachPreparation{}, errs.New(
			errs.KindValidationFailed, "authored Attach runtime requires one Detach artifact",
		)
	}
	prepared := serviceruntimerecord.AttachPreparation{
		EnvironmentID: sealed.GetArtifacts()[0].GetOwnerId(), PlanID: sealed.GetPlanId(),
		PlanHash: hex.EncodeToString(sealed.GetPlanHash()), RenderGeneration: sealed.GetRenderGeneration(),
	}
	var selected []string
	for _, step := range sealed.GetSteps() {
		if step.GetComposeApply() == nil {
			continue
		}
		if prepared.StepID != "" {
			return serviceruntimerecord.AttachPreparation{}, errs.New(
				errs.KindValidationFailed, "authored Attach runtime has multiple Compose steps",
			)
		}
		prepared.StepID = step.GetStepId()
		if _, _, err := executionplan.AttachMutationServices(sealed, prepared.StepID); err != nil {
			return serviceruntimerecord.AttachPreparation{}, err
		}
		selected = step.GetComposeApply().GetServiceIds()
	}
	if len(selected) > 1 || len(selected) != len(sources) {
		return serviceruntimerecord.AttachPreparation{}, errs.New(
			errs.KindStateConflict, "authored Attach captured runtime selection changed",
		)
	}
	previous := make([]executionplan.CandidateRuntime, len(sources))
	for index, source := range sources {
		if source.Revision <= 0 || source.Record.EnvironmentID != prepared.EnvironmentID ||
			source.Record.Runtime.ServiceID != selected[index] || serviceruntimerecord.Validate(source.Record) != nil {
			return serviceruntimerecord.AttachPreparation{}, errs.New(
				errs.KindStateConflict, "authored Attach captured runtime changed",
			)
		}
		previous[index] = source.Record.Runtime
	}
	runtimes, err := executionplan.PrepareAttachRuntimes(sealed, previous)
	if err != nil {
		return serviceruntimerecord.AttachPreparation{}, err
	}
	for index, runtime := range runtimes {
		prepared.Updates = append(prepared.Updates, serviceruntimerecord.AttachUpdate{
			PreviousRevision: sources[index].Revision, Runtime: runtime,
		})
	}
	return prepared, serviceruntimerecord.ValidateAttachPreparation(prepared)
}

// GetAttachFactsVersioned reads the exact encrypted fact envelope selected by
// a fixed-revision Attach source. Absence is represented by revision zero.
func (repository *AttachRepository) GetAttachFactsVersioned(
	ctx context.Context,
	current etcdstore.Versioned[attachrecord.Record],
) (etcdstore.Versioned[attachrecord.EncryptedFacts], bool, error) {
	if err := attachrecord.ValidateAttachVersion(current); err != nil {
		return etcdstore.Versioned[attachrecord.EncryptedFacts]{}, false, err
	}
	revision := current.ReadRevision
	if revision == 0 {
		revision = current.Revision
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{attachrecord.AttachFactsKey(current.Record.ID)}, Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[attachrecord.EncryptedFacts]{}, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		if read != nil {
			etcdstore.ClearValues(read.Values)
		}
		return etcdstore.Versioned[attachrecord.EncryptedFacts]{}, false, errs.New(
			errs.KindInternal, "Attach encrypted fact read is incomplete",
		)
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] == nil {
		if current.Record.OwnsCredential() && (len(current.Record.FactSets) != 0 || current.Record.HookBundle) {
			return etcdstore.Versioned[attachrecord.EncryptedFacts]{}, false, errs.New(
				errs.KindInternal, "Attach encrypted facts are missing",
			)
		}
		return etcdstore.Versioned[attachrecord.EncryptedFacts]{ReadRevision: revision}, false, nil
	}
	facts, err := attachrecord.DecodeAttachEncryptedFacts(read.Values[0].Value)
	if err != nil || facts.AttachID != current.Record.ID {
		clear(facts.Ciphertext)
		return etcdstore.Versioned[attachrecord.EncryptedFacts]{}, false, attachrecord.CorruptAttachRecord()
	}
	return etcdstore.Versioned[attachrecord.EncryptedFacts]{
		Record: facts, Revision: read.Values[0].ModRevision, ReadRevision: revision,
	}, true, nil
}

func (repository *AttachRepository) GetAuthoredAttachRemovalIntent(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[environmentchanges.AttachRemovalIntent], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[environmentchanges.AttachRemovalIntent]{}, false, err
	}
	key := environmentchanges.AttachRemovalIntentKey(taskID)
	read, err := repository.store.Get(ctx, key)
	if err != nil {
		return etcdstore.Versioned[environmentchanges.AttachRemovalIntent]{}, false, err
	}
	if read == nil || read.Entry == nil {
		return etcdstore.Versioned[environmentchanges.AttachRemovalIntent]{}, false, nil
	}
	intent, err := environmentchanges.DecodeAttachRemovalIntent(read.Entry.Value)
	if err != nil || intent.TaskID != taskID {
		environmentchanges.ClearAttachRemovalIntent(&intent)
		return etcdstore.Versioned[environmentchanges.AttachRemovalIntent]{}, false,
			errs.New(errs.KindInternal, "authored Attach removal authority is corrupt")
	}
	return etcdstore.Versioned[environmentchanges.AttachRemovalIntent]{
		Record: intent, Revision: read.Entry.ModRevision, ReadRevision: read.ReadRevision,
	}, true, nil
}
