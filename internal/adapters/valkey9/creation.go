package valkey9

import (
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/core"
)

func (a *adapter) CreationSpec(authentication core.BackingAuthentication) adapters.CreationSpec {
	return adapters.CreationSpec{
		ServiceName: "valkey",
		VolumeSlug:  "data",
		VolumeKey:   "data",
		MountPath:   "/data",
		Command: []string{
			"sh",
			"-ec",
			valkeyStartup,
		},
		HealthCommand: []string{"CMD-SHELL", "valkey-cli --user groundplane -e ping >/dev/null"},
		Expose:        []string{"6379"},
		HealthTCP:     "127.0.0.1:6379",
		Environment: []adapters.CreationEnvironment{
			{Name: "VALKEY_AUTHENTICATION", Literal: string(authentication)},
			{Name: "VALKEY_PASSWORD", BootstrapKey: "password", Secret: true},
			{Name: "REDISCLI_AUTH", BootstrapKey: "password", Secret: true},
		},
	}
}

// Initialize only a new ACL file. Subsequent starts retain saved consumer
// credentials, and only the dedicated management user receives the bootstrap.
const valkeyStartup = `umask 077
if [ ! -f /data/groundplane.acl ]; then
  gp_admin_hash=$(printf '%s' "$VALKEY_PASSWORD" | sha256sum | cut -d ' ' -f 1)
  case "$VALKEY_AUTHENTICATION" in
    username_password) gp_default='off resetpass' ;;
    password) gp_default='on resetpass' ;;
    none) gp_default='on nopass' ;;
    *) exit 1 ;;
  esac
  printf 'user groundplane on #%s ~* &* +@all\nuser default %s ~* &* +@all -@admin\n' "$gp_admin_hash" "$gp_default" > /data/groundplane.acl
fi
exec valkey-server --appendonly yes --aclfile /data/groundplane.acl`
