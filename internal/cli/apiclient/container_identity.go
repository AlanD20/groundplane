package apiclient

import (
	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func containerIdentityFromGenerated(identity *generated.ContainerIdentity) (*version.ContainerIdentity, error) {
	if identity == nil {
		return nil, nil
	}
	source, err := buildProvenanceFromGenerated(identity.Source)
	if err != nil {
		return nil, err
	}
	result := &version.ContainerIdentity{Image: identity.Image, ConfigDigest: identity.ConfigDigest}
	if identity.SelectedTag != nil {
		result.SelectedTag = *identity.SelectedTag
	}
	if identity.Version != nil {
		result.Version = *identity.Version
	}
	if source != nil {
		result.Source = *source
	}
	if err := result.Validate(); err != nil {
		return nil, errs.New(errs.KindInternal, "apiclient: installed container identity is invalid")
	}
	return result, nil
}
