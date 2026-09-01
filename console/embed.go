//go:build groundplane_console

// Package console exposes the production Vite output to the Controller build.
package console

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var embedded embed.FS

// Assets returns the embedded Vite output rooted at dist.
func Assets() (fs.FS, error) {
	return fs.Sub(embedded, "dist")
}
