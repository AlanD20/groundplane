import { useState } from "react";
import { Plus, Trash2 } from "lucide-react";
import { ResourcePanel } from "@/components/common/resource-panel";
import { ResourceTable } from "@/components/common/resource-table";
import {
  CollectionToolbar,
  TablePagination,
  TableSortHead,
  useTableView,
} from "@/components/common/table-controls";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Radio } from "@/components/ui/radio";
import { SearchableSelect } from "@/components/ui/searchable-select";
import { useStore } from "@/lib/store";
import type { DNSRecord } from "@/lib/types";

export function DNSRecordSettings({
  records,
  disabled,
  onChange,
}: {
  records: DNSRecord[];
  disabled: boolean;
  onChange: (records: DNSRecord[]) => Promise<void>;
}) {
  const { tenantProjects } = useStore();
  const [hostname, setHostname] = useState("");
  const [mode, setMode] = useState<"address" | "service">("address");
  const [address, setAddress] = useState("");
  const [serviceId, setServiceId] = useState("");
  const [zoneId, setZoneId] = useState("");
  const [query, setQuery] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const targets = tenantProjects.flatMap((project) =>
    (project.environments ?? []).flatMap((environment) =>
      environment.services
        .filter(
          (service) =>
            !service.adapter &&
            service.expose.length > 0 &&
            service.expose.every((port) => !port.endsWith("/udp")),
        )
        .map((service) => ({
          service,
          environment,
          label: `${project.name} / ${environment.name} / ${service.name}`,
        })),
    ),
  );
  const selected = targets.find((target) => target.service.id === serviceId);
  const zones =
    selected?.environment.zones.filter((zone) =>
      selected.service.zones.includes(zone.name),
    ) ?? [];
  const targetLabel = (record: DNSRecord) =>
    record.address ??
    `${targets.find((target) => target.service.id === record.service_id)?.label ?? record.service_id} / ${targets.flatMap((target) => target.environment.zones).find((zone) => zone.id === record.zone_id)?.name ?? record.zone_id}`;
  const table = useTableView(
    records.filter((record) =>
      `${record.hostname} ${targetLabel(record)}`
        .toLowerCase()
        .includes(query.toLowerCase()),
    ),
    { hostname: (record: DNSRecord) => record.hostname, target: targetLabel },
    "hostname",
    "asc",
    query,
  );

  async function save(next: DNSRecord[], clear = false) {
    setSaving(true);
    setError(null);
    try {
      await onChange(next);
      if (clear) {
        setHostname("");
        setAddress("");
        setServiceId("");
        setZoneId("");
      }
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "Unable to update DNS records",
      );
    } finally {
      setSaving(false);
    }
  }

  function add() {
    const name = hostname.trim().toLowerCase();
    if (!name || records.some((record) => record.hostname === name)) {
      setError("Enter a unique hostname.");
      return;
    }
    if (
      (mode === "address" && !address.trim()) ||
      (mode === "service" && (!serviceId || !zoneId))
    ) {
      setError("Choose a target address or a Service and Zone.");
      return;
    }
    const record: DNSRecord =
      mode === "address"
        ? { hostname: name, address: address.trim() }
        : { hostname: name, service_id: serviceId, zone_id: zoneId };
    void save([...records, record], true);
  }

  return (
    <ResourcePanel title="Hostname records">
      <p className="text-xs text-muted-foreground">
        Service names resolve to stable TCP proxies. Deploy a Service once
        before selecting it; recreate and blue-green releases keep its address.
      </p>
      <fieldset disabled={disabled || saving} className="space-y-4">
        <div className="flex gap-4 text-sm">
          <Label className="flex items-center gap-2">
            <Radio
              name="dns-target"
              checked={mode === "address"}
              onChange={() => setMode("address")}
            />{" "}
            IPv4 address
          </Label>
          <Label className="flex items-center gap-2">
            <Radio
              name="dns-target"
              checked={mode === "service"}
              onChange={() => setMode("service")}
            />{" "}
            Service
          </Label>
        </div>
        <div className="grid items-end gap-3 md:grid-cols-2 xl:grid-cols-4">
          <div className="space-y-1">
            <Label htmlFor="dns-record-name">Hostname</Label>
            <Input
              id="dns-record-name"
              value={hostname}
              onChange={(event) => setHostname(event.target.value)}
              placeholder="api.internal"
            />
          </div>
          {mode === "address" ? (
            <div className="space-y-1">
              <Label htmlFor="dns-record-address">IPv4 address</Label>
              <Input
                id="dns-record-address"
                value={address}
                onChange={(event) => setAddress(event.target.value)}
                placeholder="10.20.0.10"
              />
            </div>
          ) : (
            <>
              <div className="space-y-1">
                <Label>TCP Service</Label>
                <SearchableSelect
                  aria-label="DNS Service"
                  value={serviceId}
                  onValueChange={(value) => {
                    setServiceId(value);
                    setZoneId("");
                  }}
                  options={targets.map((target) => ({
                    value: target.service.id,
                    label: target.label,
                  }))}
                  disabled={disabled || saving}
                />
              </div>
              <div className="space-y-1">
                <Label>Zone</Label>
                <SearchableSelect
                  aria-label="DNS Service Zone"
                  value={zoneId}
                  onValueChange={setZoneId}
                  options={zones.map((zone) => ({
                    value: zone.id,
                    label: `${zone.name} (${zone.subnet})`,
                  }))}
                  disabled={!selected || disabled || saving}
                />
              </div>
            </>
          )}
          <Button disabled={!hostname || disabled || saving} onClick={add}>
            <Plus className="size-4" /> Add record
          </Button>
        </div>
      </fieldset>
      {error && (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      )}
      <CollectionToolbar
        query={query}
        onQueryChange={setQuery}
        label="DNS records"
      />
      <ResourceTable>
        <Table>
          <TableHeader>
            <TableRow>
              <TableSortHead sort={table} field="hostname">
                Hostname
              </TableSortHead>
              <TableSortHead sort={table} field="target">
                Target
              </TableSortHead>
              <TableHead className="w-16">
                <span className="sr-only">Actions</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {table.rows.map((record) => (
              <TableRow key={record.hostname}>
                <TableCell className="font-mono">{record.hostname}</TableCell>
                <TableCell className="break-all">
                  {targetLabel(record)}
                </TableCell>
                <TableCell>
                  <Button
                    variant="ghost"
                    size="icon"
                    aria-label={`Remove DNS record ${record.hostname}`}
                    disabled={disabled || saving}
                    onClick={() =>
                      void save(
                        records.filter(
                          (value) => value.hostname !== record.hostname,
                        ),
                      )
                    }
                  >
                    <Trash2 className="size-4" />
                  </Button>
                </TableCell>
              </TableRow>
            ))}
            {!table.rows.length && (
              <TableRow>
                <TableCell
                  colSpan={3}
                  className="py-6 text-center text-muted-foreground"
                >
                  {query ? "No matching records." : "No hostname records."}
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </ResourceTable>
      <TablePagination table={table} label="DNS records" />
    </ResourcePanel>
  );
}
