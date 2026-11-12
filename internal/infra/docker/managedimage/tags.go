package managedimage

import (
	"context"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/distribution/reference"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TagEngine keeps descriptive names separate from immutable runtime references.
type TagEngine interface {
	ImageInspect(context.Context, string, ...client.ImageInspectOption) (client.ImageInspectResult, error)
	ImageTag(context.Context, client.ImageTagOptions) (client.ImageTagResult, error)
}

// PreserveTag fills an absent local name. Reconciliation must not move a name
// that an explicit Fetch has already assigned to other content.
func PreserveTag(ctx context.Context, engine TagEngine, pinned, tag string) error {
	if tag == "" {
		return nil
	}
	if !imageref.IsDigestPinned(pinned) {
		return errs.New(errs.KindValidationFailed, "image tag requires an immutable source")
	}
	named, err := reference.ParseNormalizedNamed(pinned)
	if err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	target, err := reference.WithTag(reference.TrimNamed(named), tag)
	if err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	if _, err := engine.ImageInspect(ctx, target.String()); err == nil {
		return nil
	} else if !errdefs.IsNotFound(err) {
		return errs.Wrap(errs.KindInternal, err)
	}
	if _, err := engine.ImageTag(ctx, client.ImageTagOptions{Source: pinned, Target: target.String()}); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

// BuildTag uses the identity in the selected image, never the Controller's
// version or an upstream mutable tag lookup.
func BuildTag(labels map[string]string) string {
	switch labels["com.groundplane.source-kind"] {
	case "source_ref":
		return labels["org.opencontainers.image.revision"]
	case "release":
		version := labels["com.groundplane.version"]
		if version == "" {
			version = labels["org.opencontainers.image.version"]
		}
		version = strings.TrimPrefix(strings.TrimPrefix(version, "agent/"), "controller/")
		if version != "" && version != "dev" {
			return "v" + strings.TrimPrefix(version, "v")
		}
	}
	return ""
}
