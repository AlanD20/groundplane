package releasegroup

import (
	"context"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "release group context is required")
	}
	return ctx.Err()
}

func validateScope(environmentID string, name string) error {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || strings.TrimSpace(name) == "" ||
		!utf8.ValidString(name) {
		return errs.New(errs.KindValidationFailed, "release group environment id and name are required")
	}
	for _, character := range name {
		if unicode.IsControl(character) {
			return errs.New(errs.KindValidationFailed, "release group environment id and name are required")
		}
	}
	return nil
}

func validateVersion(value Versioned) error {
	if err := domain.Validate(value.Group); err != nil {
		return err
	}
	if value.Revision <= 0 || value.ReadRevision < value.Revision {
		return errs.New(errs.KindValidationFailed, "release group revision is invalid")
	}
	return nil
}
