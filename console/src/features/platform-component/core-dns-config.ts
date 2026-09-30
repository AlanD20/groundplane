import type { DNSRecord } from "@/lib/types";

export type CoreDNSDraft = {
  upstream: string;
  upstreamAuto: boolean;
  tailnetDelegation: boolean;
  corefileTemplate: string;
  forwarders: { domain: string; upstream: string }[];
  records: DNSRecord[];
};

function resolverList(value: string): string[] {
  return value
    .trim()
    .split(/[\s,]+/)
    .filter(Boolean);
}

export function coreDNSConfigInput(config: CoreDNSDraft) {
  return {
    upstream_auto: config.upstreamAuto,
    upstream_resolvers: config.upstreamAuto
      ? []
      : resolverList(config.upstream),
    tailnet_delegation: config.tailnetDelegation,
    corefile_template: config.corefileTemplate,
    forwarders: config.forwarders.map((value) => ({
      domain: value.domain,
      resolvers: resolverList(value.upstream),
    })),
    records: config.records,
  };
}
