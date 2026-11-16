import { useState } from "react";
import { AnimatedSection } from "@/components/common/animated-section";
import {
  ResourceForm,
  ResourceFormHeader as DialogHeader,
  ResourceFormTitle as DialogTitle,
  ResourceFormFooter as DialogFooter,
} from "@/components/common/resource-form";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { TaskRunAfterSelector } from "@/features/task/task-run-after-selector";
import { useStore } from "@/lib/store";
import type { Environment, Route } from "@/lib/types";

export function routeHostError(
  host: string,
  exposure: Route["exposure"],
): string | null {
  if (!host)
    return exposure === "public"
      ? "Public Routes require a DNS hostname."
      : null;
  if (host.length > 253 || host.endsWith(".") || host !== host.toLowerCase()) {
    return "Use a lowercase ASCII DNS hostname without a trailing dot.";
  }
  if (/^\d+(?:\.\d+){3}$/.test(host))
    return "Use a DNS hostname, not an IP address.";
  const labels = host.split(".");
  if (
    labels.some(
      (label) => !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label),
    )
  ) {
    return "Each DNS label must use letters, numbers, or internal hyphens.";
  }
  return null;
}

export function routePathError(path: string): string | null {
  if (path.length > 2048) return "Route paths cannot exceed 2048 bytes.";
  if (!path.startsWith("/")) return "Route paths must start with /.";
  if (/[?#]/.test(path))
    return "Route paths cannot contain a query or fragment.";
  const firstWildcard = path.indexOf("*");
  if (
    firstWildcard >= 0 &&
    (firstWildcard !== path.length - 1 ||
      path.lastIndexOf("*") !== firstWildcard)
  ) {
    return "A Route path may use one wildcard only, at the end.";
  }
  if (/%(?![0-9A-Fa-f]{2})/u.test(path))
    return "Percent escapes must use exactly two hexadecimal digits.";
  if (/[^A-Za-z0-9/%\-._~:@!$&()+,;=*]/u.test(path)) {
    return "Route paths contain a character that cannot be rendered safely.";
  }
  return null;
}

export function RouteFormDialog({
  env,
  open,
  onOpenChange,
}: {
  env: Environment;
  open: boolean;
  onOpenChange: (v: boolean) => void;
}) {
  return (
    <AnimatedSection open={open}>
      {open ? (
        <RouteForm key={env.id} env={env} open onOpenChange={onOpenChange} />
      ) : null}
    </AnimatedSection>
  );
}

function RouteForm({
  env,
  open,
  onOpenChange,
}: {
  env: Environment;
  open: boolean;
  onOpenChange: (v: boolean) => void;
}) {
  const store = useStore();
  const [host, setHost] = useState("");
  const [path, setPath] = useState("/");
  const [target, setTarget] = useState(env.services[0]?.id ?? "");
  const [targetPort, setTargetPort] = useState("");
  const [exposure, setExposure] = useState<"public" | "internal">("internal");
  const [after, setAfter] = useState<string[]>([]);
  const [afterLoading, setAfterLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string>();
  const normalizedHost = host.trim();
  const normalizedPath = path.trim();
  const hostValidationError = routeHostError(normalizedHost, exposure);
  const pathValidationError = routePathError(normalizedPath);
  const validTargetPort =
    /^\d+$/.test(targetPort) &&
    Number(targetPort) >= 1 &&
    Number(targetPort) <= 65535;

  const submit = async () => {
    setSubmitting(true);
    setSubmitError(undefined);
    try {
      await store.addRoute(env.id, {
        host: normalizedHost,
        path: normalizedPath,
        exposure,
        targetServiceId: target,
        targetPort: Number(targetPort),
        after,
      });
      onOpenChange(false);
      setHost("");
      setPath("/");
      setTargetPort("");
    } catch (error) {
      setSubmitError(
        error instanceof Error ? error.message : "Unable to create Route",
      );
    } finally {
      setSubmitting(false);
    }
  };
  return (
    <ResourceForm open={open} onOpenChange={onOpenChange}>
      <DialogHeader>
        <DialogTitle>Add route · {env.name}</DialogTitle>
      </DialogHeader>
      <div className="grid max-w-4xl gap-4 sm:grid-cols-2">
        <p className="text-xs text-muted-foreground sm:col-span-2">
          Public routes require an enabled router.
        </p>
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="r-host">Host</Label>
          <Input
            id="r-host"
            value={host}
            onChange={(event) => setHost(event.target.value)}
            placeholder="app.example.com"
            aria-invalid={hostValidationError !== null}
            autoFocus
          />
          <p
            className={
              hostValidationError
                ? "text-xs text-destructive"
                : "text-xs text-muted-foreground"
            }
          >
            {hostValidationError ??
              "Required for public Routes; optional for hostless internal Routes."}
          </p>
        </div>
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="r-path">Path</Label>
          <Input
            id="r-path"
            value={path}
            onChange={(event) => setPath(event.target.value)}
            placeholder="/api/*"
            aria-invalid={pathValidationError !== null}
          />
          <p
            className={
              pathValidationError
                ? "text-xs text-destructive"
                : "text-xs text-muted-foreground"
            }
          >
            {pathValidationError ??
              "Absolute path with one optional terminal wildcard."}
          </p>
        </div>
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="r-target">Target service</Label>
          <Select
            searchable
            id="r-target"
            value={target}
            onValueChange={setTarget}
            options={
              env.services.length > 0
                ? env.services.map((service) => ({
                    value: service.id,
                    label: service.name,
                  }))
                : [{ value: "", label: "— no services yet —" }]
            }
          />
        </div>
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="r-target-port">Target port</Label>
          <Input
            id="r-target-port"
            inputMode="numeric"
            value={targetPort}
            onChange={(event) => setTargetPort(event.target.value)}
            placeholder="8080"
          />
        </div>
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="r-exposure">Exposure</Label>
          <Select
            id="r-exposure"
            value={exposure}
            onValueChange={(v) => setExposure(v as "public" | "internal")}
            options={[
              { value: "public", label: "public — needs ingress component" },
              { value: "internal", label: "internal — no host port" },
            ]}
          />
        </div>
        {submitError ? (
          <p role="alert" className="text-sm text-destructive">
            {submitError}
          </p>
        ) : null}
      </div>
      <TaskRunAfterSelector
        value={after}
        onChange={setAfter}
        onBusyChange={setAfterLoading}
      />
      <DialogFooter>
        <Button variant="outline" onClick={() => onOpenChange(false)}>
          Cancel
        </Button>
        <Button
          data-action-id="route.create"
          disabled={
            hostValidationError !== null ||
            pathValidationError !== null ||
            !env.services.some((service) => service.id === target) ||
            !validTargetPort ||
            submitting ||
            afterLoading
          }
          onClick={() => void submit()}
        >
          {submitting ? "Adding..." : "Add route"}
        </Button>
      </DialogFooter>
    </ResourceForm>
  );
}
