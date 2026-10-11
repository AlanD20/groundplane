import type { components } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";

export type DNSRecord = components["schemas"]["EnvironmentDNSRecord"];
export type DNSWrite = components["schemas"]["EnvironmentDNSWrite"];
type WireView = components["schemas"]["EnvironmentDNS"];
export type DNSView = Omit<WireView, "records" | "withdrawals"> & {
  records: DNSRecord[];
  withdrawals: components["schemas"]["EnvironmentDNSWithdrawal"][];
};
export type DNSChange =
  | { action: "create"; revision: string; body: DNSWrite }
  | { action: "edit"; revision: string; recordId: string; body: DNSWrite }
  | { action: "remove"; revision: string; recordId: string };

const path = (environmentId: string) =>
  `/environments/${encodeURIComponent(environmentId)}/dns-records`;

export async function getDNS(
  environmentId: string,
  signal: AbortSignal,
): Promise<DNSView> {
  const view = await controllerRequest<WireView>(path(environmentId), 200, {
    signal,
  });
  if (
    view.environment_id !== environmentId ||
    !(
      view.revision === "0" ||
      /^task_[0-9A-HJKMNP-TV-Z]{26}$/.test(view.revision)
    ) ||
    !["pending", "applied", "failed", "resolver_disabled"].includes(view.state)
  )
    throw new Error("Controller returned invalid Environment DNS status.");
  for (const record of view.records ?? []) {
    if (
      !/^dns_[0-9A-HJKMNP-TV-Z]{26}$/.test(record.id) ||
      ![
        "pending",
        "applied",
        "failed",
        "resolver_disabled",
        "waiting_for_service",
      ].includes(record.state) ||
      !["environment", "project", "tenant"].includes(record.scope)
    )
      throw new Error("Controller returned an invalid DNS record.");
  }
  return {
    ...view,
    records: view.records ?? [],
    withdrawals: view.withdrawals ?? [],
  };
}

export async function saveDNS(
  environmentId: string,
  change: DNSChange,
  key: string,
  signal: AbortSignal,
) {
  const endpoint =
    path(environmentId) +
    (change.action === "create"
      ? ""
      : `/${encodeURIComponent(change.recordId)}`);
  const result = await controllerRequest<
    components["schemas"]["EnvironmentDNSSaved"]
  >(endpoint, change.action === "create" ? 201 : 200, {
    method:
      change.action === "create"
        ? "POST"
        : change.action === "edit"
          ? "PUT"
          : "DELETE",
    body: change.action === "remove" ? undefined : change.body,
    ifMatch: `"${change.revision}"`,
    idempotencyKey: key,
    signal,
  });
  if (
    result.environment_id !== environmentId ||
    (change.action !== "create" && result.record_id !== change.recordId)
  )
    throw new Error(
      "Controller returned a saved DNS record for a different selection.",
    );
  return result;
}
