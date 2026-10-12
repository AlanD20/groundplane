import { useState } from "react";
import {
  ResourceForm,
  ResourceFormHeader,
  ResourceFormTitle,
  ResourceFormFooter,
} from "@/components/common/resource-form";
import { FormSection } from "@/components/ui/form-section";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import type { Environment } from "@/lib/types";
import type { DNSRecord, DNSWrite } from "./api";

export function DNSEditor({
  env,
  tenant,
  project,
  record,
  saving,
  onClose,
  onSave,
}: {
  env: Environment;
  tenant: string;
  project: string;
  record?: DNSRecord;
  saving: boolean;
  onClose: () => void;
  onSave: (body: DNSWrite) => Promise<void>;
}) {
  const [label, setLabel] = useState(record?.label ?? "");
  const [scope, setScope] = useState<DNSRecord["scope"]>(
    record?.scope ?? "environment",
  );
  const [scopeChanged, setScopeChanged] = useState(false);
  const [target, setTarget] = useState(record?.service ? "service" : "address");
  const [address, setAddress] = useState(record?.address ?? "");
  const [service, setService] = useState(record?.service ?? "");
  const [zone, setZone] = useState(record?.zone ?? "");
  const [error, setError] = useState<string>();
  const suffix =
    record && !scopeChanged
      ? record.hostname.slice(record.label.length + 1)
      : [
          scope === "environment" ? env.name : "",
          scope !== "tenant" ? project : "",
          tenant,
          "internal",
        ]
          .filter(Boolean)
          .join(".");
  async function submit(event: React.FormEvent) {
    event.preventDefault();
    if (saving) return;
    setError(undefined);
    try {
      await onSave({
        label: label.trim(),
        scope: !record || scopeChanged ? scope : undefined,
        address: target === "address" ? address.trim() : undefined,
        service: target === "service" ? service.trim() : undefined,
        zone: target === "service" ? zone : undefined,
      });
      onClose();
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "Unable to save DNS record.",
      );
    }
  }
  return (
    <ResourceForm
      open
      onOpenChange={(open) => {
        if (!open && !saving) onClose();
      }}
    >
      <form onSubmit={(event) => void submit(event)} className="space-y-4">
        <ResourceFormHeader>
          <ResourceFormTitle>
            {record ? "Edit DNS record" : "Add DNS record"}
          </ResourceFormTitle>
        </ResourceFormHeader>
        <fieldset disabled={saving} className="space-y-4">
          <FormSection title="Name">
            <div className="space-y-1.5">
              <Label htmlFor="dns-label">Record label</Label>
              <Input
                id="dns-label"
                value={label}
                onChange={(e) => setLabel(e.target.value)}
                required
                maxLength={63}
                pattern={"[a-z0-9](?:[a-z0-9\\-]*[a-z0-9])?"}
                placeholder="api"
                autoFocus
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="dns-scope">Namespace</Label>
              <Select
                id="dns-scope"
                value={scope}
                options={[
                  { value: "environment", label: "Environment" },
                  { value: "project", label: "Project" },
                  { value: "tenant", label: "Tenant" },
                ]}
                onValueChange={(value) => {
                  if (
                    value === "environment" ||
                    value === "project" ||
                    value === "tenant"
                  ) {
                    setScope(value);
                    setScopeChanged(true);
                  }
                }}
              />
            </div>
            <p className="break-all text-sm font-mono sm:col-span-2">
              {label.trim() || "record"}.{suffix}
            </p>
          </FormSection>
          <FormSection
            title="Destination"
            description="Names use the shared platform resolver. A Service target waits until its selected Zone has an acknowledged TCP proxy."
          >
            <div className="space-y-1.5">
              <Label htmlFor="dns-target">Target type</Label>
              <Select
                id="dns-target"
                value={target}
                onValueChange={setTarget}
                options={[
                  { value: "address", label: "IPv4 address" },
                  { value: "service", label: "Service in this Environment" },
                ]}
              />
            </div>
            {target === "address" ? (
              <div className="space-y-1.5">
                <Label htmlFor="dns-address">IPv4 address</Label>
                <Input
                  id="dns-address"
                  value={address}
                  onChange={(e) => setAddress(e.target.value)}
                  placeholder="10.90.1.10"
                  required
                />
              </div>
            ) : (
              <>
                <div className="space-y-1.5">
                  <Label htmlFor="dns-service">Service name</Label>
                  <Input
                    id="dns-service"
                    value={service}
                    onChange={(e) => setService(e.target.value)}
                    placeholder="app"
                    required
                  />
                  <p className="text-xs text-muted-foreground">
                    The record can wait for a Service with this name.
                  </p>
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="dns-zone">Zone</Label>
                  <Select
                    id="dns-zone"
                    searchable
                    value={zone}
                    onValueChange={setZone}
                    options={env.zones.map((item) => ({
                      value: item.name,
                      label: item.name,
                    }))}
                  />
                </div>
              </>
            )}
          </FormSection>
        </fieldset>
        {error && (
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
        )}
        <ResourceFormFooter>
          <Button
            type="button"
            variant="outline"
            disabled={saving}
            onClick={onClose}
          >
            Cancel
          </Button>
          <Button
            type="submit"
            disabled={
              saving ||
              !label.trim() ||
              (target === "service" && (!zone || !service.trim()))
            }
          >
            {saving ? "Saving…" : "Save DNS record"}
          </Button>
        </ResourceFormFooter>
      </form>
    </ResourceForm>
  );
}
