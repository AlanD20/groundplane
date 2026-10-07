import { ArrowLeft } from "lucide-react";
import { Link, useLocation, type LinkProps } from "react-router-dom";
import { useStore } from "@/lib/store";
import { serviceDestinations } from "@/features/service/workspace-navigation";

function returnContext(search: string) {
  const params = new URLSearchParams(search);
  const href = params.get("returnTo");
  const label = params.get("returnLabel");
  // Only Console paths; never hand untrusted external schemes to navigation.
  if (
    !href ||
    !label ||
    !/^\/(t|platform)\//.test(href) ||
    /[\\\r\n]/.test(href)
  )
    return null;
  return { href, label };
}

export function ContextLink({
  to,
  returnLabel,
  ...props
}: Omit<LinkProps, "to"> & { to: string; returnLabel?: string }) {
  const location = useLocation();
  const store = useStore();
  const params = new URLSearchParams(location.search);
  const serviceId = params.get("service");
  const service = store.tenantProjects
    .flatMap((project) => project.environments ?? [])
    .flatMap((env) => env.services)
    .find((service) => service.id === serviceId);
  const tab =
    serviceDestinations.find((item) => item.key === params.get("serviceTab"))
      ?.label ?? "Overview";
  const sourceSearch = new URLSearchParams(location.search);
  if (service) sourceSearch.delete("task");
  const origin = returnContext(location.search) ?? {
    href:
      location.pathname +
      (sourceSearch.size ? `?${sourceSearch}` : "") +
      location.hash,
    label:
      returnLabel ??
      (service
        ? `${service.name} · ${tab}`
        : params.has("task")
          ? "Task details"
          : "Previous workspace"),
  };
  const [path, query = ""] = to.split("?");
  const next = new URLSearchParams(query);
  next.set("returnTo", origin.href);
  next.set("returnLabel", origin.label);
  return <Link {...props} to={`${path}?${next}`} />;
}

export function ContextReturnLink() {
  const location = useLocation();
  const context = returnContext(location.search);
  if (!context) return null;
  return (
    <Link
      to={context.href}
      className="mb-5 inline-flex max-w-full items-center gap-2 rounded text-sm text-primary hover:underline focus-visible:outline-2 focus-visible:outline-ring"
    >
      <ArrowLeft aria-hidden className="size-4 shrink-0" />
      <span className="min-w-0 [overflow-wrap:anywhere]">
        Back to {context.label}
      </span>
    </Link>
  );
}
