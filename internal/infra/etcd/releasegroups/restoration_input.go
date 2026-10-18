package releasegroups

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func RestorationInputKey(workID string) string {
	return "/v1/execution/group-restoration-inputs/" + workID
}

// RestorationInput names the successful source's retained executable input,
// exact attempt receipt and real later failure. Artifacts and configuration are
// not copied into Group policy or subsequent attempts.
type RestorationInput struct {
	Preparation RestorationPreparation                     `json:"preparation"`
	Authority   releaseexecution.GroupRestorationAuthority `json:"authority"`
	Source      workexecution.Attempt                      `json:"source"`
}

func (input RestorationInput) Validate() error {
	if _, err := input.Preparation.Reference(); err != nil {
		return err
	}
	if input.Authority.ValidateSource(input.Source) != nil ||
		input.Source.TaskID != input.Preparation.TaskID || input.Source.WorkItemID != input.Preparation.SourceWorkID ||
		input.Authority.GroupID != input.Preparation.GroupID || input.Authority.GroupOperationID != input.Preparation.GroupOperationID ||
		input.Authority.MemberOrdinal != input.Preparation.Ordinal || input.Preparation.WorkItemID == input.Authority.FailedWorkID {
		return errs.New(errs.KindValidationFailed, "Group restoration input has no exact captured source and failure")
	}
	return nil
}

func EncodeRestorationInput(input RestorationInput) ([]byte, workexecution.Input, error) {
	if err := input.Validate(); err != nil {
		return nil, workexecution.Input{}, err
	}
	value, err := recordcodec.Encode("group-restoration-input", input)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	if len(value) > keyvalue.MaximumBytes {
		clear(value)
		return nil, workexecution.Input{}, errs.New(errs.KindReleasePlanTooLarge, "Group restoration input exceeds its bounded storage format")
	}
	digest := sha256.Sum256(value)
	// Existing runtime ownership labels retain the successful source's Plan id;
	// this distinct digest binds the new restoration work and its actual cause.
	return value, workexecution.Input{ID: input.Source.Input.ID, SHA256: hex.EncodeToString(digest[:])}, nil
}

func DecodeRestorationInput(value []byte, reference workexecution.Input) (RestorationInput, error) {
	if len(value) == 0 || len(value) > keyvalue.MaximumBytes || reference.Validate() != nil {
		return RestorationInput{}, errs.New(errs.KindStateConflict, "Group restoration input reference is invalid")
	}
	digest := sha256.Sum256(value)
	if hex.EncodeToString(digest[:]) != reference.SHA256 {
		return RestorationInput{}, errs.New(errs.KindStateConflict, "Group restoration input digest changed")
	}
	input, err := recordcodec.Decode[RestorationInput](value, "group-restoration-input")
	if err != nil {
		return RestorationInput{}, err
	}
	if input.Validate() != nil || input.Source.Input.ID != reference.ID {
		return RestorationInput{}, errs.New(errs.KindStateConflict, "Group restoration input differs from its captured source identity")
	}
	return input, nil
}
