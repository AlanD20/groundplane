package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Planning retains only resource identity, authored intent and dependency edges.
// Commands, hook inputs and encrypted values are not planning state.
type authoredAttachPlanInput struct {
	Name               string
	ConsumerServiceID  string
	AuthoredSpecSHA256 string
	Fingerprint        string
	Reads              []blueprintunits.ResourceKey
}

type authoredAdmissionPlan struct {
	Plan       blueprintunits.DesiredPlan
	Conditions []keyvalue.Condition
}

func (service *Service) planAuthoredAdmission(ctx context.Context, baseline authoredApplyBaseline,
	desired projectionrecord.EnvironmentDesiredInput, identities projectionrecord.EnvironmentOwnedIdentities,
	drafts []etcd.BlueprintAttachInputGenerationPublication,
) (authoredAdmissionPlan, error) {
	input := authoredParentInput{desired: desired, identities: identities, owner: baseline.scope}
	inputs := make(map[string]authoredAttachPlanInput, len(identities.Attaches))
	for _, draft := range drafts {
		generation := draft.Generation
		if attachinputs.ValidateDraft(generation) != nil || generation.EnvironmentID != desired.EnvironmentID ||
			generation.ParentTaskID != desired.RevisionID {
			return authoredAdmissionPlan{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Attach admission inputs are inconsistent",
			)
		}
		if _, duplicate := inputs[generation.AttachID]; duplicate {
			return authoredAdmissionPlan{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Attach admission input is duplicated",
			)
		}
		authority := generation
		authority.WorkItemID = ""
		authority.SecretPins = nil
		fingerprint, err := authoredUnitFingerprint("attach", authority)
		if err != nil {
			return authoredAdmissionPlan{}, err
		}
		selected := authoredAttachPlanInput{
			Name:               generation.AttachName,
			ConsumerServiceID:  generation.ConsumerServiceID,
			AuthoredSpecSHA256: generation.AuthoredSpecSHA256,
			Fingerprint:        fingerprint,
		}
		if generation.Provision != nil {
			if generation.CredentialOwnerID != generation.AttachID {
				selected.Reads = append(
					selected.Reads,
					blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: generation.CredentialOwnerID},
				)
			} else {
				for _, grant := range generation.Provision.Grants {
					selected.Reads = append(selected.Reads, blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: grant.Record.ID})
				}
			}
		}
		inputs[generation.AttachID] = selected
	}
	reader, ok := service.repository.(appliedAttachBindingReader)
	if !ok {
		return authoredAdmissionPlan{}, errs.New(errs.KindInternal, "Applied Attach planning reader is not configured")
	}
	applied := make(map[string]blueprintunits.AppliedRecord)
	for _, receipt := range baseline.ledger.Applied {
		if receipt.Record.Target.Kind == ids.KindAttach && receipt.Record.State == blueprintunits.Applied {
			applied[receipt.Record.Target.ID] = receipt.Record
		}
	}
	var conditions []keyvalue.Condition
	for _, identity := range identities.Attaches {
		if _, captured := inputs[identity.ID]; captured {
			continue
		}
		receipt, found := applied[identity.ID]
		if !found {
			continue
		}
		binding, guards, err := reader.ReadAppliedAttachBinding(
			ctx,
			desired.EnvironmentID,
			identity.ID,
			baseline.ledger.ReadRevision,
		)
		if err != nil {
			return authoredAdmissionPlan{}, err
		}
		selected := authoredAttachPlanInput{
			Name:               binding.Consumer.Name,
			ConsumerServiceID:  binding.Consumer.ServiceID,
			AuthoredSpecSHA256: binding.AuthoredSpecSHA256,
			Fingerprint:        receipt.Fingerprint,
		}
		if binding.Consumer.CredentialAttachID != binding.Consumer.ID {
			selected.Reads = append(
				selected.Reads,
				blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: binding.Consumer.CredentialAttachID},
			)
		} else {
			for _, grant := range binding.Grants {
				selected.Reads = append(selected.Reads, blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: grant.ID})
			}
		}
		binding.Clear()
		inputs[identity.ID] = selected
		conditions = append(conditions, guards...)
	}
	plan, err := service.planAuthoredUnits(ctx, input, inputs, baseline.ledger)
	if err != nil {
		return authoredAdmissionPlan{}, err
	}
	if !plan.Complete {
		return authoredAdmissionPlan{}, errs.New(
			errs.KindStateConflict,
			"Blueprint resource plan is missing Attach authority",
		)
	}
	return authoredAdmissionPlan{Plan: plan, Conditions: conditions}, nil
}
