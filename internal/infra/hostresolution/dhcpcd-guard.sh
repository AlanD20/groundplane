# Groundplane-owned dhcpcd DNS integration.
# Sourced immediately before 20-resolv.conf; 21-groundplane-dns releases fd 9.
if [ -d '@STATE@' ] && [ ! -L '@STATE@' ]; then
    if { exec 9>'@STATE@/resolver.lock'; } 2>/dev/null; then
        _groundplane_dns_lock_open=1
        if /usr/bin/flock -x -w 15 9; then
            if [ -f '@STATE@/resolv.conf.previous' ] &&
                [ "$(wc -c < /etc/resolv.conf)" -eq 21 ] &&
                [ "$(cat /etc/resolv.conf)" = 'nameserver 127.0.0.1' ]; then
                skip_hooks="${skip_hooks:+$skip_hooks }resolv.conf"
            fi
        else
            # Never overwrite a concurrent GP operation on lock timeout.
            skip_hooks="${skip_hooks:+$skip_hooks }resolv.conf"
        fi
    else
        skip_hooks="${skip_hooks:+$skip_hooks }resolv.conf"
    fi
fi
