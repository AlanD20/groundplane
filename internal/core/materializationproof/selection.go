package materializationproof

import (
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type SelectionRequest struct {
	EnvironmentID              string
	AppliedRevisionID          string
	RenderGeneration           uint64
	ProducingTaskID            string
	CanonicalSHA256            string
	TargetServiceID            string
	RequiredMaterializationIDs []string
}

type Selection struct {
	members []MemberRecord
	secrets []ReusableSecretRecord
}

func (selection Selection) Members() []MemberRecord { return cloneMembers(selection.members) }
func (selection Selection) ReusableSecrets() []ReusableSecretRecord {
	return append([]ReusableSecretRecord(nil), selection.secrets...)
}

// SelectTargetService binds one Script target to the exact required members.
// A required environment-level member is shared by all Services, while a
// nonempty Service binding must match the target exactly.
func SelectTargetService(proof Proof, request SelectionRequest) (Selection, error) {
	if ids.Validate(ids.KindEnvironment, request.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, request.AppliedRevisionID) != nil ||
		request.RenderGeneration == 0 ||
		ids.Validate(ids.KindTask, request.ProducingTaskID) != nil ||
		ids.Validate(ids.KindService, request.TargetServiceID) != nil ||
		!validDigest(request.CanonicalSHA256) {
		return Selection{}, validationError("materialization proof selection identity is invalid")
	}
	if proof.record.EnvironmentID != request.EnvironmentID ||
		proof.record.AppliedRevisionID != request.AppliedRevisionID ||
		proof.record.RenderGeneration != request.RenderGeneration ||
		proof.record.ProducingTaskID != request.ProducingTaskID ||
		proof.record.CanonicalSHA256 != request.CanonicalSHA256 {
		return Selection{}, conflictError("materialization proof does not match the expected applied generation")
	}
	required := append([]string(nil), request.RequiredMaterializationIDs...)
	sort.Strings(required)
	for index, materializationID := range required {
		if ids.Validate(ids.KindConfig, materializationID) != nil {
			return Selection{}, validationError("required materialization identity is invalid")
		}
		if index > 0 && materializationID == required[index-1] {
			return Selection{}, validationError("required materialization identity is duplicated")
		}
	}
	byMaterializationID := make(map[string]MemberRecord, len(proof.record.Members))
	for _, member := range proof.record.Members {
		byMaterializationID[member.MaterializationID] = member
	}
	requiredSet := make(map[string]struct{}, len(required))
	for _, materializationID := range required {
		requiredSet[materializationID] = struct{}{}
	}
	for _, member := range proof.record.Members {
		if member.ServiceID != request.TargetServiceID {
			continue
		}
		if _, requiredForTarget := requiredSet[member.MaterializationID]; !requiredForTarget {
			return Selection{}, conflictError("materialization proof has an unrequired target Service member")
		}
	}
	selected := make([]MemberRecord, 0, len(required))
	secretByID := make(map[string]ReusableSecretRecord)
	for _, materializationID := range required {
		member, found := byMaterializationID[materializationID]
		if !found {
			return Selection{}, conflictError("required materialization is missing from the proof")
		}
		if member.ServiceID != "" && member.ServiceID != request.TargetServiceID {
			return Selection{}, conflictError("required materialization is not bound to the target Service")
		}
		selected = append(selected, member)
		for _, secret := range member.ReusableSecrets {
			if existing, exists := secretByID[secret.SecretID]; exists && existing != secret {
				return Selection{}, errs.New(errs.KindInternal, "materialization proof Secret inputs are inconsistent")
			}
			secretByID[secret.SecretID] = secret
		}
	}
	secrets := make([]ReusableSecretRecord, 0, len(secretByID))
	for _, secret := range secretByID {
		secrets = append(secrets, secret)
	}
	sort.Slice(secrets, func(left, right int) bool { return secrets[left].SecretID < secrets[right].SecretID })
	return Selection{members: cloneMembers(selected), secrets: secrets}, nil
}

func conflictError(message string) error { return errs.New(errs.KindStateConflict, message) }
