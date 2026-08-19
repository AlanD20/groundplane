// Package version owns the one build version exposed by every Groundplane
// binary and protocol surface.
package version

// Value is replaced at build time with:
//
// -ldflags "-X github.com/AlanD20/groundplane/internal/common/version.Value=<version>"
var Value = "dev"
