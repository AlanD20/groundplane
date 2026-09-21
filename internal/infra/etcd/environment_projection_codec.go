package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func encodeEnvironmentComposeProjection(projection EnvironmentComposeProjection) ([]byte, error) {
	if err := validateEnvironmentComposeProjection(projection); err != nil {
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

func EncodeEnvironmentComposeProjectionStorage(projection EnvironmentComposeProjection) ([]byte, error) {
	return encodeEnvironmentComposeProjection(projection)
}

func decodeEnvironmentComposeProjection(value []byte) (EnvironmentComposeProjection, error) {
	projection, err := recordcodec.Decode[EnvironmentComposeProjection](value, "environment-compose-projection")
	if err != nil {
		return EnvironmentComposeProjection{}, err
	}
	if err := validateEnvironmentComposeProjection(projection); err != nil {
		return EnvironmentComposeProjection{}, corruptEnvironmentComposeProjection()
	}
	return projection, nil
}

func DecodeEnvironmentComposeProjectionStorage(value []byte) (EnvironmentComposeProjection, error) {
	return decodeEnvironmentComposeProjection(value)
}

func validateEnvironmentComposeProjection(projection EnvironmentComposeProjection) error {
	return validateEnvironmentProjection(projection, environmentArtifactDesired)
}
