//go:build groundplane_console

package app

import (
	"io/fs"

	consoleassets "github.com/AlanD20/groundplane/console"
)

func controllerConsoleAssets() (fs.FS, error) {
	return consoleassets.Assets()
}
