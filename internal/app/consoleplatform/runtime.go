// Package consoleplatform composes embedded migration assets with the verified
// installed Console store without exposing arbitrary filesystem directories.
package consoleplatform

import (
	"fmt"
	"io/fs"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/internal/controller/handlers"
	"github.com/AlanD20/groundplane/internal/infra/consolerelease"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Runtime struct {
	Assets handlers.ConsoleAssets
	Store  *consolerelease.Store
}

func Load(loader func() (fs.FS, error)) (Runtime, error) {
	embedded, err := loader()
	if err != nil {
		return Runtime{}, errs.Wrap(errs.KindInternal, fmt.Errorf("controller: load embedded Console: %w", err))
	}
	runtime := Runtime{Assets: handlers.FixedConsoleAssets(embedded)}
	release, installed := installedVersion(version.Value, version.Provenance())
	if !installed || embedded == nil {
		return runtime, nil
	}
	runtime.Store, err = consolerelease.New(consolerelease.DefaultRoot, embedded, release)
	if err != nil {
		return Runtime{}, fmt.Errorf("controller: initialize Console release store: %w", err)
	}
	runtime.Assets = runtime.Store
	return runtime, nil
}

func installedVersion(controllerVersion string, source version.BuildProvenance) (string, bool) {
	if source.Kind == "source_ref" && source.Validate() == nil {
		return "console/ref/" + source.Commit, true
	}
	value := strings.TrimPrefix(controllerVersion, "controller/")
	if !strings.HasPrefix(value, "v") {
		return "", false
	}
	return "console/" + value, true
}
