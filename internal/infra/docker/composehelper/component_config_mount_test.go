package composehelper

import "testing"

// Rationale: a file bind pins the old inode and a child mount can hide the
// replacement even when the parent directory is correctly bound.
func TestComponentConfigMountRequiresUnshadowedReadOnlyDirectory(t *testing.T) {
	for _, test := range []struct {
		name   string
		mounts []componentConfigMount
		want   bool
	}{
		{"directory", []componentConfigMount{{Type: "bind", Source: "/env/components/caddy", Destination: "/etc/caddy"}}, true},
		{"file", []componentConfigMount{{Type: "bind", Source: "/env/components/caddy/Caddyfile", Destination: "/etc/caddy/Caddyfile"}}, false},
		{"writable", []componentConfigMount{{Type: "bind", Source: "/env/components/caddy", Destination: "/etc/caddy", RW: true}}, false},
		{"foreign", []componentConfigMount{{Type: "bind", Source: "/foreign", Destination: "/etc/caddy"}}, false},
		{"shadowed", []componentConfigMount{
			{Type: "bind", Source: "/env/components/caddy", Destination: "/etc/caddy"},
			{Type: "volume", Source: "foreign", Destination: "/etc/caddy/Caddyfile"},
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := hasExactComponentConfigMount(test.mounts, "/env/components/caddy/Caddyfile", "/etc/caddy/Caddyfile"); got != test.want {
				t.Fatalf("mount accepted = %v, want %v", got, test.want)
			}
		})
	}
}
