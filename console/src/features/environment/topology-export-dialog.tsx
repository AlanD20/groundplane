import { useState } from "react";
import { Download } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import type { Environment, Project } from "@/lib/types";
import { buildTopology, type TopologyLayer } from "./topology-model";
import { topologyImage, type TopologyImageFormat } from "./topology-export";

export function TopologyExportDialog({
  env,
  backingProjects,
  layers,
  selected,
  now,
  onClose,
}: {
  env: Environment;
  backingProjects: Project[];
  layers: TopologyLayer[];
  selected: string | null;
  now: number;
  onClose: () => void;
}) {
  const [scope, setScope] = useState("all");
  const [ids, setIds] = useState<string[]>(
    env.services.some((service) => service.id === selected) ? [selected!] : [],
  );
  const [query, setQuery] = useState("");
  const [format, setFormat] = useState<TopologyImageFormat>("svg");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const selectedIds = ids.filter((id) =>
    env.services.some((service) => service.id === id),
  );
  const download = async () => {
    setBusy(true);
    setError(undefined);
    try {
      const source =
        scope === "all"
          ? env
          : {
              ...env,
              services: env.services.filter((service) =>
                selectedIds.includes(service.id),
              ),
              routes: env.routes.filter((route) =>
                selectedIds.includes(route.targetServiceId),
              ),
              attaches: env.attaches.filter((attach) =>
                selectedIds.includes(attach.serviceId),
              ),
            };
      const graph = buildTopology(source, backingProjects);
      graph.edges = graph.edges.filter((edge) => layers.includes(edge.kind));
      const blob = await topologyImage(
        graph,
        `${env.name} topology`,
        document.documentElement,
        now,
        format,
      );
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = `${env.name.replace(/[^a-zA-Z0-9_-]/g, "-")}-topology-${scope}.${format}`;
      document.body.append(link);
      link.click();
      link.remove();
      window.setTimeout(() => URL.revokeObjectURL(url), 1000);
      onClose();
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : "Unable to export the diagram.",
      );
    } finally {
      setBusy(false);
    }
  };
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open && !busy) onClose();
      }}
    >
      <DialogContent submitOnEnter={false}>
        <DialogHeader>
          <DialogTitle>Export topology</DialogTitle>
          <DialogDescription>
            A centered diagram with 16 px margins, including Zones and connected
            resources. Uses the enabled connection layers, independently of pan,
            zoom or highlighting.
          </DialogDescription>
        </DialogHeader>
        <div className="grid grid-cols-2 gap-4">
          <label className="space-y-2 text-sm">
            Format
            <Select
              value={format}
              onValueChange={(value) =>
                setFormat(value === "png" ? "png" : "svg")
              }
              disabled={busy}
              options={[
                { value: "svg", label: "SVG · vector" },
                { value: "png", label: "PNG · image" },
              ]}
            />
          </label>
          <label className="space-y-2 text-sm">
            Services
            <Select
              value={scope}
              onValueChange={setScope}
              disabled={busy}
              options={[
                { value: "all", label: "All Services" },
                { value: "selected", label: "Selected Services" },
              ]}
            />
          </label>
        </div>
        {scope === "selected" && (
          <div className="space-y-3">
            <Input
              aria-label="Search Services to export"
              placeholder="Search Services…"
              value={query}
              disabled={busy}
              onChange={(event) => setQuery(event.target.value)}
            />
            <div className="flex items-center justify-between text-xs text-muted-foreground">
              <span role="status">{selectedIds.length} selected</span>
              <Button
                size="xs"
                variant="ghost"
                disabled={busy || !selectedIds.length}
                onClick={() => setIds([])}
              >
                Clear
              </Button>
            </div>
            <div className="max-h-56 space-y-1 overflow-y-auto rounded-lg border border-border p-2">
              {env.services
                .filter((service) =>
                  service.name.toLowerCase().includes(query.toLowerCase()),
                )
                .map((service) => (
                  <label
                    key={service.id}
                    className="flex cursor-pointer items-center gap-3 rounded-md px-2 py-2 text-sm hover:bg-muted"
                  >
                    <Checkbox
                      checked={selectedIds.includes(service.id)}
                      disabled={busy}
                      onChange={(event) =>
                        setIds((current) =>
                          event.target.checked
                            ? [...current, service.id]
                            : current.filter((id) => id !== service.id),
                        )
                      }
                    />
                    <span className="break-all">{service.name}</span>
                  </label>
                ))}
              {!env.services.some((service) =>
                service.name.toLowerCase().includes(query.toLowerCase()),
              ) && (
                <p className="p-2 text-sm text-muted-foreground">
                  No matching Services
                </p>
              )}
            </div>
            <p className="text-xs text-muted-foreground">
              Includes connected Routes, Backing Services and Volumes.
              Dependencies appear when both Services are selected.
            </p>
          </div>
        )}
        {error && (
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
        )}
        <DialogFooter>
          <Button variant="outline" disabled={busy} onClick={onClose}>
            Cancel
          </Button>
          <Button
            disabled={busy || (scope === "selected" && !selectedIds.length)}
            onClick={() => void download()}
          >
            <Download />{" "}
            {busy ? "Exporting…" : `Export ${format.toUpperCase()}`}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
