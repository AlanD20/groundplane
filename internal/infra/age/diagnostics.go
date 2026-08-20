package age

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadControllerRecipient reads the public recipient derived from an existing
// Controller identity. Unlike ControllerKey.Load, it never creates a missing
// directory or key.
func ReadControllerRecipient(ctx context.Context, path string) (string, error) {
	return readControllerRecipient(ctx, path, 0)
}

func readControllerRecipient(ctx context.Context, path string, expectedUID uint32) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if path == "" {
		return "", errs.New(errs.KindValidationFailed, "age: controller key path is required")
	}

	parentPath := filepath.Dir(path)
	if err := validateOwnedDirectory(parentPath, expectedUID); err != nil {
		return "", err
	}
	root, err := os.OpenRoot(parentPath)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, fmt.Errorf("age: open key directory: %w", err))
	}
	defer root.Close()

	identity, err := loadIdentityOwnedBy(root, filepath.Base(path), expectedUID)
	if errors.Is(err, fs.ErrNotExist) {
		return "", errs.New(errs.KindInternal, "age: controller key does not exist")
	}
	if err != nil {
		return "", err
	}
	return identity.Recipient().String(), nil
}
