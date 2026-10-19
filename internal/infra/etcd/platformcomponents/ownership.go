package platformcomponents

import (
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const PlatformComponentOwnerPrefix = "/v1/indexes/components/by-owner/platform/"

func PlatformComponentOwnerKey(id string) string { return PlatformComponentOwnerPrefix + id }

func PlatformComponentKindKey(kind core.ComponentKind) string {
	return "/v1/indexes/components/by-kind/platform/" + recordcodec.EncodeKeySegment(string(kind))
}

func ValidatePlatformComponentRecord(record componentrecord.Record) error {
	if err := componentrecord.ValidateRecord(record); err != nil {
		return err
	}
	if record.Desired.Owner != core.ComponentOwnerPlatform || record.Desired.OwnerID != "" {
		return errs.New(errs.KindValidationFailed, "platform Component ownership is invalid")
	}
	return nil
}
