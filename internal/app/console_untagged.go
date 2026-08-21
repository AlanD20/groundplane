//go:build !groundplane_console

package app

import "io/fs"

func controllerConsoleAssets() (fs.FS, error) {
	return nil, nil
}
