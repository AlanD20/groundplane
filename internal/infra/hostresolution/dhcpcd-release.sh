# Groundplane-owned dhcpcd DNS integration.
# Later service hooks must not inherit the resolver lock in background daemons.
if [ "${_groundplane_dns_lock_open:-}" = 1 ]; then
    exec 9>&-
    unset _groundplane_dns_lock_open
fi
