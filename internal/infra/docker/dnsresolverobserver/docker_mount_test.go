package dnsresolverobserver

import "testing"

func TestArtifactDirectoryMountOwnsExactFileTarget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		destination string
		readWrite   bool
		target      string
		want        bool
	}{
		{
			name:        "read-only parent",
			destination: "/etc/groundplane/coredns",
			target:      "/etc/groundplane/coredns/Corefile",
			want:        true,
		},
		{
			name:        "read-write parent",
			destination: "/etc/groundplane/coredns",
			readWrite:   true,
			target:      "/etc/groundplane/coredns/Corefile",
		},
		{name: "prefix sibling", destination: "/etc/groundplane/core", target: "/etc/groundplane/coredns/Corefile"},
		{
			name:        "file mount",
			destination: "/etc/groundplane/coredns/Corefile",
			target:      "/etc/groundplane/coredns/Corefile",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := artifactDirectoryMountOwnsTarget(test.destination, test.readWrite, test.target); got != test.want {
				t.Fatalf("artifactDirectoryMountOwnsTarget() = %t, want %t", got, test.want)
			}
		})
	}
}
