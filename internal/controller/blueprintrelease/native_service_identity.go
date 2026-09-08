package blueprintrelease

import (
	"crypto/sha256"
	"encoding/json"

	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// normalizedNativeServiceIdentities freezes complete native Service decisions.
// core.Service alone is not lossless: rendering also consumes environment,
// entrypoint, health, mounts, and other native Compose configuration.
func normalizedNativeServiceIdentities(project *composetypes.Project) (map[string][sha256.Size]byte, error) {
	identities := make(map[string][sha256.Size]byte)
	if project == nil {
		return identities, nil
	}
	for _, services := range []composetypes.Services{project.Services, project.DisabledServices} {
		for name, service := range services {
			if _, duplicate := identities[name]; duplicate {
				return nil, errs.New(errs.KindInternal, "Blueprint normalized Service content is ambiguous")
			}
			canonical, err := json.Marshal(service)
			if err != nil {
				return nil, errs.Wrap(errs.KindInternal, err)
			}
			identities[name] = sha256.Sum256(canonical)
		}
	}
	return identities, nil
}

type blueprintServiceMembership uint8

const (
	blueprintServiceActive blueprintServiceMembership = iota + 1
	blueprintServiceProfileDisabled
)

type NormalizedServiceMemberships struct {
	previous        map[string]blueprintServiceMembership
	candidate       map[string]blueprintServiceMembership
	previousNative  map[string][sha256.Size]byte
	candidateNative map[string][sha256.Size]byte
	initialized     bool
}

func BuildNormalizedServiceMemberships(
	previous *composetypes.Project,
	candidate *composetypes.Project,
) (NormalizedServiceMemberships, error) {
	if candidate == nil {
		return NormalizedServiceMemberships{}, errs.New(
			errs.KindInternal,
			"Blueprint candidate normalized Service membership is absent",
		)
	}
	previousMemberships := make(map[string]blueprintServiceMembership)
	if previous != nil {
		var err error
		previousMemberships, err = normalizedProjectServiceMemberships(previous)
		if err != nil {
			return NormalizedServiceMemberships{}, err
		}
	}
	candidateMemberships, err := normalizedProjectServiceMemberships(candidate)
	if err != nil {
		return NormalizedServiceMemberships{}, err
	}
	previousNative, err := normalizedNativeServiceIdentities(previous)
	if err != nil {
		return NormalizedServiceMemberships{}, err
	}
	candidateNative, err := normalizedNativeServiceIdentities(candidate)
	if err != nil {
		return NormalizedServiceMemberships{}, err
	}
	return NormalizedServiceMemberships{
		previous: previousMemberships, candidate: candidateMemberships, initialized: true,
		previousNative: previousNative, candidateNative: candidateNative,
	}, nil
}

func normalizedProjectServiceMemberships(
	project *composetypes.Project,
) (map[string]blueprintServiceMembership, error) {
	memberships := make(map[string]blueprintServiceMembership, len(project.Services)+len(project.DisabledServices))
	for name := range project.Services {
		memberships[name] = blueprintServiceActive
	}
	for name := range project.DisabledServices {
		if _, duplicate := memberships[name]; duplicate {
			return nil, errs.New(errs.KindInternal, "Blueprint normalized Service membership is ambiguous")
		}
		memberships[name] = blueprintServiceProfileDisabled
	}
	return memberships, nil
}
