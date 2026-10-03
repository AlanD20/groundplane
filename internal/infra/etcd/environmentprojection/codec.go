package environmentprojection

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EncodePreparedEnvironmentComposeProjectionStorage excludes execution receipts
// from desired/effective input without changing the caller's applied candidate.
func EncodePreparedEnvironmentComposeProjectionStorage(projection EnvironmentComposeProjection) ([]byte, error) {
	projection.BackingRuntime = nil
	return EncodeEnvironmentComposeProjectionStorage(projection)
}

func EncodeEnvironmentComposeProjectionStorage(projection EnvironmentComposeProjection) ([]byte, error) {
	if err := ValidateEnvironmentComposeProjection(projection); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("environment-compose-projection", projection)
	if err != nil {
		return nil, err
	}
	if len(value) > EnvironmentBlueprintProjectionMaxBytes {
		clear(value)
		return nil, errs.New(
			errs.KindValidationFailed,
			"Blueprint normalized projection exceeds the 2 MiB ceiling",
		)
	}
	return value, nil
}

func DecodeEnvironmentComposeProjectionStorage(value []byte) (EnvironmentComposeProjection, error) {
	projection, err := recordcodec.Decode[EnvironmentComposeProjection](value, "environment-compose-projection")
	if err != nil {
		return EnvironmentComposeProjection{}, err
	}
	if err := ValidateEnvironmentComposeProjection(projection); err != nil {
		return EnvironmentComposeProjection{}, CorruptEnvironmentComposeProjection()
	}
	return projection, nil
}

func ValidateEnvironmentComposeProjection(projection EnvironmentComposeProjection) error {
	return ValidateEnvironmentProjection(projection, environmentArtifactDesired)
}
