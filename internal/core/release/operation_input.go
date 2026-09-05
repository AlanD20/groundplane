package release

import (
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type ServiceDeployInput struct {
	Tag       string
	Strategy  Strategy
	OnFailure OnFailure
}

type ServiceRollbackInput struct{ Tag string }
type GroupDeployInput struct{ Tag string }

type GroupRollbackInput struct {
	Tag             *string
	PreviewRevision *int64
}

type GroupRollbackPreviewInput struct{ Tag *string }

type RollbackSource struct {
	ServiceID string
	ReleaseID string
	Tag       string
}

type GroupRollbackPreview struct {
	GroupID  string
	Revision int64
	Sources  []RollbackSource
}

func NewGroupRollbackInput(tag *string, revision *int64) (GroupRollbackInput, error) {
	if err := validateOptionalRollbackTag(tag); err != nil {
		return GroupRollbackInput{}, err
	}
	if revision != nil && *revision <= 0 {
		return GroupRollbackInput{}, errs.New(errs.KindValidationFailed, "rollback preview revision is invalid")
	}
	return GroupRollbackInput{Tag: cloneString(tag), PreviewRevision: cloneInt64(revision)}, nil
}

func NewGroupRollbackPreviewInput(tag *string) (GroupRollbackPreviewInput, error) {
	if err := validateOptionalRollbackTag(tag); err != nil {
		return GroupRollbackPreviewInput{}, err
	}
	return GroupRollbackPreviewInput{Tag: cloneString(tag)}, nil
}

func validateOptionalRollbackTag(tag *string) error {
	if tag != nil && (*tag == "" || strings.TrimSpace(*tag) != *tag) {
		return errs.New(errs.KindValidationFailed, "rollback tag is invalid")
	}
	return nil
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
