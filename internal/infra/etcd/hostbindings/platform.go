package hostbindings

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Platform is a property of the enrolled host, not the Controller binary.
// Replacing an Agent cannot silently change the host's execution architecture.
type Platform struct {
	HostID       string `json:"host_id"`
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	Variant      string `json:"variant"`
}

func PlatformKey(hostID string) string { return "/v1/hosts/" + hostID + "/platform" }

func (platform Platform) Validate() error {
	if ids.Validate(ids.KindHost, platform.HostID) != nil || platform.OS != "linux" ||
		platform.Architecture != "amd64" && platform.Architecture != "arm64" ||
		platform.Architecture == "amd64" && platform.Variant != "" ||
		platform.Architecture == "arm64" && platform.Variant != "" && platform.Variant != "v8" {
		return errs.New(errs.KindValidationFailed, "Agent must report a supported Linux host platform")
	}
	return nil
}

func EncodePlatform(platform Platform) ([]byte, error) {
	if err := platform.Validate(); err != nil {
		return nil, err
	}
	return recordcodec.Encode("host-platform", platform)
}

func ReadPlatform(ctx context.Context, store Reader, hostID string, revision int64) (keyvalue.Versioned[Platform], error) {
	if ctx == nil || store == nil || ids.Validate(ids.KindHost, hostID) != nil || revision < 0 {
		return keyvalue.Versioned[Platform]{}, errs.New(errs.KindValidationFailed, "host platform read is invalid")
	}
	key := PlatformKey(hostID)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[Platform]{}, err
	}
	if read == nil || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Versioned[Platform]{}, errs.New(errs.KindInternal, "host platform snapshot is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return keyvalue.Versioned[Platform]{ReadRevision: read.ReadRevision}, nil
	}
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
		return keyvalue.Versioned[Platform]{}, errs.New(errs.KindInternal, "host platform revision is corrupt")
	}
	platform, err := recordcodec.Decode[Platform](value.Value, "host-platform")
	if err != nil {
		return keyvalue.Versioned[Platform]{}, err
	}
	if platform.Validate() != nil || platform.HostID != hostID {
		return keyvalue.Versioned[Platform]{}, errs.New(errs.KindInternal, "host platform identity is corrupt")
	}
	return keyvalue.Versioned[Platform]{Record: platform, Revision: value.ModRevision, ReadRevision: read.ReadRevision}, nil
}
