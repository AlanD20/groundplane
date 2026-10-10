import { FormActions as DialogFooter } from "./form-actions";
import { editorFooterClassName } from "./workspace-section";
("use client");

import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { DrawerContent } from "@/components/ui/drawer";
import { FormSection } from "@/components/ui/form-section";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import type { BackingHooks } from "@/features/backing-service/api";
import { BackingHookFields } from "@/features/backing-service/hook-fields";
import { ImagePicker } from "@/features/image-delivery/image-picker";
import { HealthcheckFields } from "@/features/service/healthcheck-fields";
import { portInputError } from "@/features/service/port-input";
import { useStore } from "@/lib/store";
import type { Service } from "@/lib/types";
import { useState } from "react";
import { useDraftDirty } from "@/lib/use-draft-dirty";

// The full service form, shared between:
//  - tenant environments ("Add service" / "Edit service") — one container
//    with zones, healthcheck, resources, expose ports, restart…
//  - backing services ("New backing service") — the SAME form, plus the
//    adapter + facts-prefix fields, creating the backing project with one
//    environment ("main") and one adapter-backed service. A backing service
//    is a service: it gets the same full spec, never a reduced subset.

import type {
  ServicePatch,
  ServiceFormSection,
  ServiceFormBodyProps,
} from "./service-form-types";

export function ServiceFormBody({
  env,
  initial,
  section,
  backing,
  onCreateBacking,
  onClose,
  onDiscard,
  inline = false,
  onSaved,
  onDirtyChange,
}: ServiceFormBodyProps) {
  const store = useStore();
  const editing = !!initial;
  const show = (group: ServiceFormSection) => !section || section === group;
  const isBacking = !!backing;
  const selectedAdapter = store.adapters.find(
    (adapter) => adapter.key === backing?.adapterKey,
  );

  const [name, setName] = useState(initial?.name ?? "");
  const [image, setImage] = useState(initial?.image ?? "");
  const [role, setRole] = useState(initial?.role ?? "");
  const [strategy, setStrategy] = useState<Service["strategy"]>(
    initial?.strategy ?? (isBacking ? "recreate" : "recreate"),
  );
  const [onFailure, setOnFailure] = useState<"switch_back" | "leave_active">(
    initial?.onFailure ?? "switch_back",
  );
  const [zones, setZones] = useState<string[]>(initial?.zones ?? []);
  const [healthcheck, setHealthcheck] = useState(initial?.healthcheck ?? null);
  const [healthcheckError, setHealthcheckError] = useState<string | null>(null);
  const [healthcheckDirty, setHealthcheckDirty] = useState(false);
  const [mem, setMem] = useState(
    initial?.resources.mem ?? (isBacking ? "1g" : ""),
  );
  const [cpus, setCpus] = useState(
    initial?.resources.cpus ?? (isBacking ? "1.0" : ""),
  );
  const [expose, setExpose] = useState((initial?.expose ?? []).join(", "));
  const [exposeEdited, setExposeEdited] = useState(
    (initial?.expose ?? []).length > 0,
  );
  const exposeError = portInputError(expose);
  const [restart, setRestart] = useState<Service["restart"]>(
    initial?.restart ?? "unless-stopped",
  );
  const [replicas, setReplicas] = useState(String(initial?.replicas ?? 1));
  const [hooks, setHooks] = useState<BackingHooks>(initial?.hooks ?? {});
  useDraftDirty(
    [
      name,
      image,
      role,
      strategy,
      onFailure,
      zones,
      healthcheck,
      healthcheckDirty,
      mem,
      cpus,
      expose,
      restart,
      replicas,
      hooks,
    ],
    onDirtyChange,
  );
  const [hookError, setHookError] = useState<string | null>(null);
  // A backing service owns its own network: the zone named after it is
  // created with it, and consumers join it as `external` when they attach —
  // the operator never picks a zone for a backing service.
  const backingZone = isBacking ? name.trim().toLowerCase() || "backing" : "";

  function applyAdapter(key: string) {
    const a = store.adapters.find((x) => x.key === key);
    if (!a) return;
    const port = backingPort(a.urlScheme);
    if (isBacking) {
      backing?.onAdapterChange(key);
      backing?.onPrefixChange(a.prefix);
      if (!exposeEdited) setExpose(port);
    }
  }

  function buildPatch(): ServicePatch {
    return {
      name: name.trim(),
      image: isBacking ? "" : image.trim(),
      role:
        role.trim() ||
        (isBacking ? "Adapter-backed service" : "Added in the Console"),
      zones: isBacking ? [backingZone] : zones,
      strategy,
      onFailure,
      healthcheck: healthcheck ?? {
        test: ["NONE"],
        disable: false,
        interval: "",
        timeout: "",
        startPeriod: "",
        startInterval: "",
        retries: 0,
      },
      resources: { mem: mem.trim() || "128m", cpus: cpus.trim() || "0.25" },
      expose: expose
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean),
      restart,
      replicas: Math.max(1, parseInt(replicas, 10) || 1),
      hooks: initial?.adapter === "custom" ? hooks : undefined,
    };
  }

  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const canSubmit =
    !!name.trim() &&
    (isBacking ? !!backing.adapterKey : !!image.trim()) &&
    !submitting &&
    !exposeError &&
    !hookError &&
    !healthcheckError;

  async function save() {
    if (hookError || healthcheckError || exposeError) return;
    if (isBacking && onCreateBacking) {
      onCreateBacking(buildPatch(), backing.adapterKey, backing.prefix);
      return;
    }
    setSubmitting(true);
    setSubmitError(null);
    try {
      const current =
        initial && section ? await store.getService(initial.id) : initial;
      const values = buildPatch();
      const patch =
        !current || !section
          ? values
          : {
              ...current,
              ...(section === "workload" ? { image: values.image } : {}),
              ...(section === "network"
                ? { zones: values.zones, expose: values.expose }
                : {}),
              ...(section === "runtime"
                ? {
                    strategy: values.strategy,
                    onFailure: values.onFailure,
                    resources: values.resources,
                    restart: values.restart,
                    replicas: values.replicas,
                  }
                : {}),
              ...(section === "hooks" ? { hooks: values.hooks } : {}),
              ...(section === "healthcheck"
                ? { healthcheck: values.healthcheck }
                : {}),
            };
      if (initial && env) await store.updateService(env.id, initial.id, patch);
      else if (env) await store.addService(env.id, patch);
      onSaved?.();
      onClose();
    } catch (error) {
      setSubmitError(
        error instanceof Error ? error.message : "Service mutation failed",
      );
    } finally {
      setSubmitting(false);
    }
  }

  const content = (
    <>
      {!inline && (
        <DialogHeader>
          <DialogTitle>
            {isBacking
              ? "New backing service"
              : editing
                ? `${section ? { workload: "Image", network: "Networking", runtime: "Runtime and resources", healthcheck: "Healthcheck", hooks: "Lifecycle hooks" }[section] : "Edit service"} · ${initial.name}`
                : `Add service · ${env?.name}`}
          </DialogTitle>
        </DialogHeader>
      )}
      <form
        className="space-y-5"
        onSubmit={(event) => {
          event.preventDefault();
          if (canSubmit) void save();
        }}
      >
        <fieldset disabled={submitting} className="contents">
          <div className="flex flex-col gap-4">
            {!inline && (
              <p className="text-xs text-muted-foreground">
                {isBacking
                  ? `A backing service is a service plus the adapter that knows how to provision, connect, and back it up. The adapter resolves its immutable managed workload release; operators do not select an image. It follows the same hierarchy: backing project → one environment ("main") → this service. It owns its own network: the zone "${backingZone}" is created with it, and consumers join it as external when they attach. It stays running even with zero consumers; only an explicit Destroy removes it.`
                  : section && section !== "workload"
                    ? "Save updates desired configuration. Deploy when you are ready to apply it to the running Service."
                    : "Saving only stores configuration; it does not start a container. Make the image available on the Agent host, then use Deploy. Deploy does not pull or build images."}
              </p>
            )}
            {show("workload") && (
              <FormSection
                className={
                  inline ? "border-0 p-0 [&>legend]:sr-only" : undefined
                }
                title="Service"
              >
                <div className="flex flex-col gap-4 sm:col-span-2">
                  {isBacking && (
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                      <div className="flex flex-col gap-1.5">
                        <Label htmlFor="bs-adapter">Adapter</Label>
                        <Select
                          searchable
                          id="bs-adapter"
                          value={backing.adapterKey}
                          onValueChange={applyAdapter}
                          options={store.adapters.map((a) => ({
                            value: a.key,
                            label: a.key,
                          }))}
                        />
                      </div>
                      <div className="flex flex-col gap-1.5">
                        <Label htmlFor="bs-prefix">
                          Prefix (facts keys on attach — e.g. pg_URL)
                        </Label>
                        <Input
                          id="bs-prefix"
                          value={backing.prefix}
                          onChange={(e) =>
                            backing.onPrefixChange(e.target.value)
                          }
                          placeholder="pg16"
                        />
                      </div>
                    </div>
                  )}
                  <div className="flex flex-col gap-1.5">
                    <Label htmlFor="sv-name">
                      {isBacking ? "Name (unique service name)" : "Name"}
                    </Label>
                    <Input
                      id="sv-name"
                      value={name}
                      onChange={(e) => {
                        setName(e.target.value);
                        if (isBacking && !exposeEdited) {
                          setExpose(backingPort(selectedAdapter?.urlScheme));
                        }
                      }}
                      autoFocus
                      disabled={editing}
                    />
                  </div>
                  {!isBacking && (
                    <div className="flex flex-col gap-1.5">
                      <Label htmlFor="sv-image">Image</Label>
                      <ImagePicker
                        id="sv-image"
                        value={image}
                        onChange={setImage}
                      />
                    </div>
                  )}
                  {isBacking && (
                    <div className="flex flex-col gap-1.5">
                      <Label htmlFor="sv-role">Note</Label>
                      <Input
                        id="sv-role"
                        value={role}
                        onChange={(e) => setRole(e.target.value)}
                      />
                    </div>
                  )}
                </div>
              </FormSection>
            )}
            {show("network") && (
              <FormSection
                className={
                  inline ? "border-0 p-0 [&>legend]:sr-only" : undefined
                }
                title="Network"
              >
                <div className="flex flex-col gap-4 sm:col-span-2">
                  {!isBacking && env && env.zones.length > 0 && (
                    <div className="flex flex-col gap-1.5">
                      <Label>Zones</Label>
                      <div className="flex flex-wrap gap-2">
                        {env.zones.map((z) => (
                          <label
                            key={z.name}
                            className="flex items-center gap-1.5 text-xs"
                          >
                            <Checkbox
                              checked={zones.includes(z.name)}
                              onChange={(e) =>
                                setZones((prev) =>
                                  e.target.checked
                                    ? [...prev, z.name]
                                    : prev.filter((x) => x !== z.name),
                                )
                              }
                              className="accent-primary"
                            />
                            <span>
                              {z.name}
                              {z.internal ? " (internal)" : ""}
                            </span>
                          </label>
                        ))}
                      </div>
                    </div>
                  )}
                  <div className="flex flex-col gap-1.5">
                    <Label htmlFor="sv-expose">Container ports</Label>
                    <Input
                      id="sv-expose"
                      value={expose}
                      placeholder="80/tcp, 53/udp"
                      aria-invalid={!!exposeError}
                      aria-describedby="sv-expose-help"
                      onChange={(event) => {
                        setExpose(event.target.value);
                        setExposeEdited(true);
                      }}
                    />
                    <p
                      id="sv-expose-help"
                      className={
                        exposeError
                          ? "text-xs text-destructive"
                          : "text-xs text-muted-foreground"
                      }
                    >
                      {exposeError ??
                        "Comma-separated ports; TCP is the default. No host ports. UDP proxy deployment is not available yet."}
                    </p>
                  </div>
                </div>
              </FormSection>
            )}
            {show("runtime") && (
              <FormSection
                className={
                  inline ? "border-0 p-0 [&>legend]:sr-only" : undefined
                }
                title="Runtime and resources"
              >
                <div className="flex flex-col gap-4 sm:col-span-2">
                  <div className="flex flex-col gap-1.5">
                    <Label htmlFor="sv-strategy">
                      Strategy (default — chosen per deployment)
                    </Label>
                    <Select
                      id="sv-strategy"
                      value={strategy}
                      onValueChange={(v) =>
                        setStrategy(v as Service["strategy"])
                      }
                      options={[
                        { value: "blue-green", label: "blue-green" },
                        { value: "recreate", label: "recreate" },
                        { value: "rolling", label: "rolling (deferred)" },
                      ]}
                    />
                  </div>
                  <div className="flex flex-col gap-1.5">
                    <Label htmlFor="sv-on-failure">
                      Failure policy (default)
                    </Label>
                    <Select
                      id="sv-on-failure"
                      value={onFailure}
                      onValueChange={(value) =>
                        setOnFailure(value as "switch_back" | "leave_active")
                      }
                      options={[
                        { value: "switch_back", label: "switch_back" },
                        { value: "leave_active", label: "leave_active" },
                      ]}
                    />
                  </div>
                  <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                    <div className="flex flex-col gap-1.5">
                      <Label htmlFor="sv-restart">Restart</Label>
                      <Select
                        id="sv-restart"
                        value={restart}
                        onValueChange={(v) =>
                          setRestart(v as Service["restart"])
                        }
                        options={[
                          { value: "unless-stopped", label: "unless-stopped" },
                          { value: "always", label: "always" },
                          { value: "no", label: "no" },
                        ]}
                      />
                    </div>
                    <div className="flex flex-col gap-1.5">
                      <Label htmlFor="sv-replicas">Replicas</Label>
                      <Input
                        id="sv-replicas"
                        value={replicas}
                        onChange={(e) => setReplicas(e.target.value)}
                        placeholder="1"
                      />
                    </div>
                  </div>
                  <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                    <div className="flex flex-col gap-1.5">
                      <Label htmlFor="sv-mem">Memory limit</Label>
                      <Input
                        id="sv-mem"
                        value={mem}
                        onChange={(e) => setMem(e.target.value)}
                        placeholder="512m"
                      />
                    </div>
                    <div className="flex flex-col gap-1.5">
                      <Label htmlFor="sv-cpu">CPU limit (cores)</Label>
                      <Input
                        id="sv-cpu"
                        value={cpus}
                        onChange={(e) => setCpus(e.target.value)}
                        placeholder="0.5"
                      />
                    </div>
                  </div>
                </div>
              </FormSection>
            )}
            {show("healthcheck") && (
              <FormSection
                className={
                  inline ? "border-0 p-0 [&>legend]:sr-only" : undefined
                }
                title="Health check"
              >
                <div className="sm:col-span-2">
                  <HealthcheckFields
                    value={healthcheck}
                    onChange={setHealthcheck}
                    onError={setHealthcheckError}
                    onDirtyChange={setHealthcheckDirty}
                  />
                </div>
                {healthcheckError && (
                  <p
                    className="sm:col-span-2 text-sm text-destructive"
                    role="alert"
                  >
                    {healthcheckError}
                  </p>
                )}
              </FormSection>
            )}
          </div>
          {show("hooks") && initial?.adapter === "custom" && (
            <BackingHookFields
              value={hooks}
              onChange={setHooks}
              onError={setHookError}
            />
          )}
          {hookError && (
            <p className="text-sm text-destructive" role="alert">
              {hookError}
            </p>
          )}
          {submitError && (
            <p className="text-sm text-destructive" role="alert">
              {submitError}
            </p>
          )}
          <DialogFooter
            onDiscard={onDiscard ?? onClose}
            pinned={inline}
            className={inline ? editorFooterClassName : undefined}
          >
            <Button
              type="button"
              variant="outline"
              disabled={submitting}
              onClick={onClose}
            >
              Cancel
            </Button>
            <Button type="submit" disabled={!canSubmit}>
              {submitting
                ? "Saving…"
                : isBacking
                  ? "Create + deploy"
                  : editing
                    ? "Save changes"
                    : "Create service"}
            </Button>
          </DialogFooter>
        </fieldset>
      </form>
    </>
  );
  return inline ? (
    <div className="min-w-0">{content}</div>
  ) : (
    <DrawerContent>{content}</DrawerContent>
  );
}

function backingPort(scheme: string | undefined): string {
  switch (scheme) {
    case "pgsql":
      return "5432";
    case "redis":
      return "6379";
    case "mysql":
      return "3306";
    default:
      return "";
  }
}
