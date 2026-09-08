package etcd

import (
	"encoding/hex"
	"strings"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReleaseProxyImage is the frozen image authority of one stable Service proxy.
// It is separate from the operator workload and survives historical rerenders.
type ReleaseProxyImage struct {
	Repository  string                   `json:"repository"`
	IndexDigest string                   `json:"index_digest"`
	Platform    componentsdk.OCIPlatform `json:"platform"`
}

func (image ReleaseProxyImage) Reference() string {
	return image.Repository + "@sha256:" + image.Platform.ChildDigest
}

func (image ReleaseProxyImage) Validate() error {
	p := image.Platform
	if image.Repository == "" || strings.ContainsAny(image.Repository, "@ \t\r\n") ||
		p.OS != "linux" || p.Architecture != "amd64" && p.Architecture != "arm64" ||
		p.Architecture == "amd64" && p.Variant != "" ||
		p.Architecture == "arm64" && p.Variant != "" && p.Variant != "v8" {
		return errs.New(errs.KindValidationFailed, "release proxy image authority is invalid")
	}
	for _, digest := range []string{image.IndexDigest, p.ChildDigest, p.ConfigDigest} {
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != 32 || strings.ToLower(digest) != digest {
			return errs.New(errs.KindValidationFailed, "release proxy image digest is invalid")
		}
	}
	return nil
}
