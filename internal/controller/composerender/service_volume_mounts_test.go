package composerender

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestServiceVolumeMountEditPreservesFileMountsAndVolumeOptions(t *testing.T) {
	// Saving storage must not erase native file binds, secret grants, or
	// advanced options on a Volume mount whose identity has not changed.
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(`image: example/app:1
volumes:
  - type: bind
    source: /managed/config
    target: /etc/app
    read_only: true
  - type: volume
    source: data
    target: /data
    volume:
      nocopy: true
secrets:
  - source: credential
    target: /run/secrets/credential
`), &document); err != nil {
		t.Fatal(err)
	}
	service := document.Content[0]
	if err := replaceServiceArtifactVolumeMounts(service, []ServiceArtifactVolumeMount{{Key: "data", Target: "/data", ReadOnly: true}, {Key: "cache", Target: "/cache"}}); err != nil {
		t.Fatal(err)
	}
	volumes := service.Content[MappingIndex(service, "volumes")+1]
	if len(volumes.Content) != 3 || MappingScalar(volumes.Content[0], "type") != "bind" ||
		MappingIndex(volumes.Content[1], "volume") < 0 ||
		MappingScalar(volumes.Content[1], "read_only") != "true" {
		t.Fatal("save lost existing mount options or file binds")
	}
	if MappingScalar(volumes.Content[2], "source") != "cache" ||
		MappingScalar(volumes.Content[2], "target") != "/cache" {
		t.Fatal("new Volume not rendered")
	}
	if err := replaceServiceArtifactVolumeMounts(service, nil); err != nil {
		t.Fatal(err)
	}
	volumes = service.Content[MappingIndex(service, "volumes")+1]
	if len(volumes.Content) != 1 || MappingScalar(volumes.Content[0], "target") != "/etc/app" ||
		MappingIndex(service, "secrets") < 0 ||
		MappingScalar(service, "image") != "example/app:1" {
		t.Fatal("unmount changed unrelated configuration")
	}
	for _, target := range []string{"/etc/app", "/run/secrets/credential"} {
		if err := replaceServiceArtifactVolumeMounts(service, []ServiceArtifactVolumeMount{{Key: "data", Target: target}}); err == nil {
			t.Fatalf("accepted conflicting target %s", target)
		}
	}
}
