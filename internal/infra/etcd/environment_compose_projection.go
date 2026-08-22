package etcd

import (
	"context"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EnvironmentComposeIdentity struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type EnvironmentRouteIdentity struct {
	ID   string `json:"id"`
	Host string `json:"host,omitempty"`
	Path string `json:"path"`
}

// EnvironmentComposeProjection is the durable identity input needed to
// reproduce one Environment render. Collections are strictly sorted by Name.
type EnvironmentComposeProjection struct {
	EnvironmentID       string                       `json:"environment_id"`
	BlueprintRevisionID string                       `json:"blueprint_revision_id"`
	RenderGeneration    uint64                       `json:"render_generation"`
	Services            []EnvironmentComposeIdentity `json:"services,omitempty"`
	Networks            []EnvironmentComposeIdentity `json:"networks,omitempty"`
	Volumes             []EnvironmentComposeIdentity `json:"volumes,omitempty"`
	Routes              []EnvironmentRouteIdentity   `json:"routes,omitempty"`
}

func environmentComposeProjectionKey(environmentID string) string {
	return "/v1/records/environment-compose-projections/" + environmentID
}

func (repository *HierarchyRepository) GetEnvironmentComposeProjection(
	ctx context.Context,
	environmentID string,
) (Versioned[EnvironmentComposeProjection], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	result, err := repository.store.Get(ctx, environmentComposeProjectionKey(environmentID))
	if err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	if result == nil {
		return Versioned[EnvironmentComposeProjection]{}, false, errs.New(
			errs.KindInternal,
			"Environment Compose projection read is empty",
		)
	}
	if result.Entry == nil {
		return Versioned[EnvironmentComposeProjection]{ReadRevision: result.ReadRevision}, false, nil
	}
	projection, err := decodeEnvironmentComposeProjection(result.Entry.Value)
	if err != nil || projection.EnvironmentID != environmentID {
		return Versioned[EnvironmentComposeProjection]{}, false, corruptEnvironmentComposeProjection()
	}
	return Versioned[EnvironmentComposeProjection]{
		Record: projection, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func encodeEnvironmentComposeProjection(projection EnvironmentComposeProjection) ([]byte, error) {
	if err := validateEnvironmentComposeProjection(projection); err != nil {
		return nil, err
	}
	return encodeEnvelope("environment-compose-projection", projection)
}

func decodeEnvironmentComposeProjection(value []byte) (EnvironmentComposeProjection, error) {
	projection, err := decodeEnvelope[EnvironmentComposeProjection](value, "environment-compose-projection")
	if err != nil {
		return EnvironmentComposeProjection{}, err
	}
	if err := validateEnvironmentComposeProjection(projection); err != nil {
		return EnvironmentComposeProjection{}, corruptEnvironmentComposeProjection()
	}
	return projection, nil
}

func validateEnvironmentComposeProjection(projection EnvironmentComposeProjection) error {
	if validateStableID(ids.KindEnvironment, projection.EnvironmentID) != nil ||
		validateStableID(ids.KindTask, projection.BlueprintRevisionID) != nil || projection.RenderGeneration == 0 {
		return errs.New(errs.KindValidationFailed, "Environment Compose projection identity is invalid")
	}
	if err := validateEnvironmentComposeIdentities(ids.KindService, projection.Services); err != nil {
		return err
	}
	if err := validateEnvironmentComposeIdentities(ids.KindNetwork, projection.Networks); err != nil {
		return err
	}
	if err := validateEnvironmentComposeIdentities(ids.KindVolume, projection.Volumes); err != nil {
		return err
	}
	return validateEnvironmentRouteIdentities(projection.EnvironmentID, projection.Routes)
}

func validateEnvironmentRouteIdentities(environmentID string, values []EnvironmentRouteIdentity) error {
	previousMatch := ""
	idsSeen := make(map[string]struct{}, len(values))
	for _, value := range values {
		match := value.Host + "\x00" + value.Path
		record := RouteRecord{EnvironmentID: environmentID, Desired: core.Route{
			ID: value.ID, Host: value.Host, Path: value.Path,
			TargetServiceID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", TargetPort: 1, Exposure: "internal",
		}}
		if match <= previousMatch || validateRouteRecord(record) != nil {
			return errs.New(errs.KindValidationFailed, "Environment Route identities are invalid or unsorted")
		}
		if _, duplicate := idsSeen[value.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment Route identity id is duplicated")
		}
		idsSeen[value.ID] = struct{}{}
		previousMatch = match
	}
	return nil
}

func validateEnvironmentComposeIdentities(kind ids.Kind, values []EnvironmentComposeIdentity) error {
	previousName := ""
	idsSeen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if validateStableID(kind, value.ID) != nil || value.Name <= previousName ||
			!validEnvironmentComposeName(value.Name) {
			return errs.New(errs.KindValidationFailed, "Environment Compose identities are invalid or unsorted")
		}
		if _, duplicate := idsSeen[value.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment Compose identity id is duplicated")
		}
		idsSeen[value.ID] = struct{}{}
		previousName = value.Name
	}
	return nil
}

func validateEnvironmentComposeProjectionAdvance(
	previous EnvironmentComposeProjection,
	hasPrevious bool,
	next EnvironmentComposeProjection,
) error {
	if err := validateEnvironmentComposeProjection(next); err != nil {
		return err
	}
	if !hasPrevious {
		if next.RenderGeneration != 1 {
			return errs.New(errs.KindStateConflict, "first Environment render generation must be one")
		}
		return nil
	}
	if previous.EnvironmentID != next.EnvironmentID || next.RenderGeneration != previous.RenderGeneration+1 {
		return errs.New(errs.KindStateConflict, "Environment render generation did not advance exactly once")
	}
	if err := preserveEnvironmentComposeIdentities("service", previous.Services, next.Services); err != nil {
		return err
	}
	if err := preserveEnvironmentComposeIdentities("network", previous.Networks, next.Networks); err != nil {
		return err
	}
	return preserveEnvironmentComposeIdentities("volume", previous.Volumes, next.Volumes)
}

func preserveEnvironmentComposeIdentities(
	kind string,
	previous []EnvironmentComposeIdentity,
	next []EnvironmentComposeIdentity,
) error {
	byName := make(map[string]string, len(next))
	for _, identity := range next {
		byName[identity.Name] = identity.ID
	}
	for _, identity := range previous {
		nextID, exists := byName[identity.Name]
		if !exists {
			return errs.Newf(
				errs.KindResourceInUse,
				"Blueprint omits existing %s %s; remove it explicitly before apply",
				kind,
				identity.Name,
			)
		}
		if nextID != identity.ID {
			return errs.New(errs.KindStateConflict, "Environment Compose stable identity changed")
		}
	}
	return nil
}

func validEnvironmentComposeName(value string) bool {
	if value == "" || len(value) > 255 || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, character := range value {
		if character <= ' ' || character == '/' || character == '\\' {
			return false
		}
	}
	return true
}

func corruptEnvironmentComposeProjection() error {
	return errs.New(errs.KindInternal, "Environment Compose projection is corrupt")
}
