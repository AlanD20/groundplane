package softwarepreparation

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// Docker's containerd store retains an index as its local target even after a
// platform-filtered pull. Only the sealed native child may enter publication.
func TestPublishedAgentImportSelectsNativeManifest(t *testing.T) {
	for _, store := range []string{"classic", "containerd", "wrong-index"} {
		t.Run(store, func(t *testing.T) {
			index, child, config := digest.FromString("index"), digest.FromString("child"), digest.FromString("config")
			plan := imagefetch.Plan{Requested: "ghcr.io/aland20/groundplane-agent@" + index.String(),
				Repository: "ghcr.io/aland20/groundplane-agent", ManifestDigest: child.String(),
				ConfigDigest: config.String(), Architecture: "amd64"}
			inspection := imageInspection{ID: config.String(), OS: "linux", Architecture: "amd64"}
			inspection.Config.Labels = map[string]string{"org.opencontainers.image.version": "agent/v0.0.1"}
			if store != "classic" {
				inspection.ID = child.String()
				inspection.Descriptor = &ocispec.Descriptor{
					Digest:    child,
					Size:      1000,
					MediaType: ocispec.MediaTypeImageManifest,
				}
			}
			if store == "wrong-index" {
				inspection.ID = index.String()
				inspection.Descriptor.Digest = index
				inspection.Descriptor.MediaType = ocispec.MediaTypeImageIndex
			}
			commands := &manifestImportRunner{reference: plan.Reference(), inspection: inspection}
			operation := dockerOperation{preparer: &Preparer{commands: commands}, operationID: "manifest-import"}
			source := ResolvedSource{SourceKind: SourceRef, ResolvedSHA: strings.Repeat("a", 40),
				Platform: Platform{OS: "linux", Architecture: "amd64"}}
			_, err := operation.pullAgentManifest(t.Context(), plan, source, "agent/v0.0.1")
			if store == "wrong-index" {
				if err == nil || commands.tagged {
					t.Fatalf("unsealed index entered publication: %v", err)
				}
				return
			}
			if err != nil || !commands.pulled || !commands.tagged {
				t.Fatalf("native import failed: pulled=%t tagged=%t error=%v", commands.pulled, commands.tagged, err)
			}
		})
	}
}

type manifestImportRunner struct {
	reference  string
	inspection imageInspection
	pulled     bool
	tagged     bool
}

func (commands *manifestImportRunner) Run(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
	switch {
	case slices.Equal(opts.Args, []string{"image", "pull", "--platform", "linux/amd64", commands.reference}):
		commands.pulled = true
		return runner.Result{}, nil
	case slices.Equal(opts.Args, []string{"image", "inspect", "--format", "{{json .}}", commands.reference}):
		encoded, err := json.Marshal(commands.inspection)
		return runner.Result{Stdout: encoded}, err
	case len(opts.Args) == 4 && slices.Equal(opts.Args[:3], []string{"image", "tag", commands.reference}):
		commands.tagged = true
		return runner.Result{}, nil
	default:
		return runner.Result{}, fmt.Errorf("unsealed image command: %v", opts.Args)
	}
}

func (commands *manifestImportRunner) Stream(
	ctx context.Context, opts runner.RunCmdOpts, _ func(bool, string),
) (runner.Result, error) {
	return commands.Run(ctx, opts)
}
