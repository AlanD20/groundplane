package managedimage

import (
	"context"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

type tagEngine struct {
	names map[string]string
	calls int
}

func (engine *tagEngine) ImageInspect(
	_ context.Context,
	name string,
	_ ...client.ImageInspectOption,
) (client.ImageInspectResult, error) {
	if _, exists := engine.names[name]; !exists {
		return client.ImageInspectResult{}, errdefs.ErrNotFound
	}
	return client.ImageInspectResult{}, nil
}

func (engine *tagEngine) ImageTag(_ context.Context, options client.ImageTagOptions) (client.ImageTagResult, error) {
	engine.calls++
	engine.names[options.Target] = options.Source
	return client.ImageTagResult{}, nil
}

// IMG-05: startup must recover a missing readable name without moving an
// existing operator-selected tag back to the predecessor's image.
func TestPreserveTagKeepsSelectedContent(t *testing.T) {
	pinned := "ghcr.io/example/agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	engine := &tagEngine{names: make(map[string]string)}
	if err := PreserveTag(context.Background(), engine, pinned, "v0.0.1"); err != nil {
		t.Fatal(err)
	}
	if engine.names["ghcr.io/example/agent:v0.0.1"] != pinned || engine.calls != 1 {
		t.Fatal("missing name did not select the captured index")
	}
	engine.names["ghcr.io/example/agent:v0.0.1"] = "newer content"
	if err := PreserveTag(context.Background(), engine, pinned, "v0.0.1"); err != nil {
		t.Fatal(err)
	}
	if engine.names["ghcr.io/example/agent:v0.0.1"] != "newer content" || engine.calls != 1 {
		t.Fatal("startup moved an existing tag back to old content")
	}
	if err := PreserveTag(context.Background(), engine, "not pinned", "v0.0.1"); err == nil {
		t.Fatal("mutable source was accepted")
	}
}

// IMG-05: Agent and Runner have different version-label owners; branch builds
// must use their captured commit and unknown builds must not invent a version.
func TestBuildTagUsesSelectedArtifactIdentity(t *testing.T) {
	for _, item := range []struct {
		labels map[string]string
		want   string
	}{
		{map[string]string{"com.groundplane.source-kind": "release", "org.opencontainers.image.version": "agent/v0.0.1"}, "v0.0.1"},
		{map[string]string{"com.groundplane.source-kind": "release", "com.groundplane.version": "0.0.2", "org.opencontainers.image.version": "2.337.0"}, "v0.0.2"},
		{map[string]string{"com.groundplane.source-kind": "source_ref", "org.opencontainers.image.revision": "abcdef123456"}, "abcdef123456"},
		{nil, ""},
	} {
		if got := BuildTag(item.labels); got != item.want {
			t.Fatalf("BuildTag() = %q, want %q", got, item.want)
		}
	}
}
