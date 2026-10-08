import { useEffect, useState } from "react";
import { ResourcePanel } from "@/components/common/resource-panel";
import { ResourceTable } from "@/components/common/resource-table";
import { TaskLink } from "@/components/common/task-link";
import { CompactReference } from "@/components/common/compact-reference";
import { TablePagination, TableSortHead, useTableView } from "@/components/common/table-controls";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { controllerRequest } from "@/lib/controller-json-request";
import type { SoftwareReleaseCatalog, SoftwareRequest } from "./software-api";
import { useSoftwarePreparations } from "./use-software-preparations";

export function SoftwarePreparationPanel({ active }: { active: boolean }) {
  const work = useSoftwarePreparations(active);
  const [selection, setSelection] = useState<SoftwareRequest["selection"]>("both");
  const [source, setSource] = useState<SoftwareRequest["source_kind"]>("source_ref");
  const [ref, setRef] = useState("main");
  const [releases, setReleases] = useState<SoftwareReleaseCatalog["items"]>([]);
  const [releaseError, setReleaseError] = useState<string | null>(null);
  const [loadingReleases, setLoadingReleases] = useState(false);
  const [review, setReview] = useState<string | null>(null);
  const table = useTableView(work.items, { ref: (item) => item.ref, created: (item) => item.created_at,
    selection: (item) => item.selection, phase: (item) => item.phase }, "created", "desc");
  useEffect(() => {
    if (!active || source !== "release") return;
    const abort = new AbortController();
    setLoadingReleases(true);
    setReleaseError(null);
    void controllerRequest<SoftwareReleaseCatalog>(`/software/releases?selection=${selection}`, 200, { signal: abort.signal })
      .then((catalog) => { if (!abort.signal.aborted) setReleases(catalog.items ?? []); })
      .catch((cause) => { if (!abort.signal.aborted) setReleaseError(cause instanceof Error ? cause.message : "Unable to list releases"); })
      .finally(() => { if (!abort.signal.aborted) setLoadingReleases(false); });
    return () => abort.abort();
  }, [active, source, selection]);
  const busy = work.publishing || work.pending !== null;
  return <div className="flex flex-col gap-5">
    <ResourcePanel title="Prepare software">
      <form className="flex flex-col gap-4" onSubmit={(event) => { event.preventDefault(); if (!busy && ref.trim()) void work.prepare({ selection, source_kind: source, ref: ref.trim() }); }}>
        <div className="grid gap-4 md:grid-cols-3">
          <div className="space-y-2"><Label htmlFor="software-components">Components</Label>
            <Select id="software-components" value={selection} disabled={busy} options={[{ value: "both", label: "Controller and Agent" }, { value: "controller", label: "Controller only" }, { value: "agent", label: "Agent only" }]}
              onValueChange={(value) => { setSelection(value as SoftwareRequest["selection"]); if (source === "release") setRef(""); }} /></div>
          <div className="space-y-2"><Label htmlFor="software-source">Source</Label>
            <Select id="software-source" value={source} disabled={busy} options={[{ value: "source_ref", label: "Build branch, tag or commit" }, { value: "release", label: "Published release" }]}
              onValueChange={(value) => { setSource(value as SoftwareRequest["source_kind"]); setRef(value === "source_ref" ? "main" : ""); }} /></div>
          <div className="space-y-2"><Label htmlFor="software-ref">{source === "source_ref" ? "Git ref" : "Release"}</Label>
            {source === "source_ref" ? <Input id="software-ref" value={ref} disabled={busy} onChange={(event) => setRef(event.target.value)} required /> :
              <Select id="software-ref" value={ref} disabled={busy || loadingReleases} searchable placeholder={loadingReleases ? "Loading releases…" : "Choose a release"}
                options={(releases ?? []).map((release) => ({ value: release.ref, label: release.name || release.ref }))} onValueChange={setRef} />}</div>
        </div>
        <p className="text-sm text-muted-foreground">Preparation stores verified outputs in GP’s registry. Running software changes only when you Apply below.</p>
        {releaseError && <p role="alert" className="text-sm text-destructive">{releaseError}</p>}
        <div><Button type="submit" disabled={busy || !ref.trim() || source === "release" && (loadingReleases || !(releases ?? []).some((release) => release.ref === ref))}>{work.publishing ? "Submitting…" : "Prepare software"}</Button></div>
      </form>
      {work.pending && <div className="mt-4 flex flex-wrap items-center gap-3 rounded-lg border p-3"><p className="text-sm">Acceptance is unresolved. Check the same request before starting another.</p><Button variant="outline" disabled={work.publishing} onClick={() => void work.resolve()}>Resolve request</Button></div>}
      {work.error && <p role="alert" className="mt-3 text-sm text-destructive">{work.error}</p>}
    </ResourcePanel>
    {work.activation && <ResourcePanel title="Latest activation" actions={<TaskLink taskId={work.activation.task_id} />}>
      <p className="text-sm">{work.activation.phase.replaceAll("_", " ")}</p>
      <div className="mt-3 flex flex-wrap gap-4 text-sm">
        {work.activation.controller_task_id && <TaskLink taskId={work.activation.controller_task_id}>Controller {work.activation.controller_applied ? "applied" : "update"}</TaskLink>}
        {work.activation.agent_task_id && <TaskLink taskId={work.activation.agent_task_id}>Agent {work.activation.agent_applied ? "applied" : "update"}</TaskLink>}
      </div>
      {work.activation.error_detail && <p role="alert" className="mt-3 text-sm text-destructive">{work.activation.error_detail}</p>}
    </ResourcePanel>}
    <ResourcePanel title="Preparations" actions={<Button size="sm" variant="outline" disabled={work.loading} onClick={() => void work.refresh()}>Refresh</Button>}>
      {!work.loaded && work.loading ? <p className="text-sm text-muted-foreground">Loading preparations…</p> :
        <><ResourceTable><Table><TableHeader><TableRow>
          <TableSortHead sort={table} field="ref">Source</TableSortHead><TableSortHead sort={table} field="selection">Components</TableSortHead>
          <TableSortHead sort={table} field="created">Created</TableSortHead><TableSortHead sort={table} field="phase">Status</TableSortHead><TableHead>Actions</TableHead>
        </TableRow></TableHeader><TableBody>
          {table.rows.map((item) => <TableRow key={item.task_id}>
            <TableCell><div className="space-y-1"><span className="font-medium">{item.ref}</span><p className="text-xs text-muted-foreground">{item.source_kind === "source_ref" ? "Host source build" : "Published release"}</p>
              {(item.provenance ?? []).map((source) => <CompactReference key={source.component} label={source.component} value={source.commit} />)}
              {(item.artifacts ?? []).map((artifact) => <CompactReference key={artifact.component} label={`${artifact.component} registry output`} value={artifact.reference} />)}
            </div></TableCell><TableCell>{item.selection === "both" ? "Controller + Agent" : item.selection}</TableCell>
            <TableCell>{new Date(item.created_at).toLocaleString()}</TableCell><TableCell><p>{item.phase.replaceAll("_", " ")}</p>{item.error_detail && <p className="mt-1 max-w-sm text-xs text-destructive">{item.error_detail}</p>}</TableCell>
            <TableCell><div className="flex flex-col items-start gap-2"><TaskLink taskId={item.task_id} />
              {item.phase === "verified" && (review === item.task_id ? <div className="flex flex-col gap-2 rounded-lg border p-3"><p className="max-w-xs text-xs">Apply these exact outputs? The Console may reconnect during the Controller restart. Applications are not redeployed.</p><div className="flex gap-2"><Button size="sm" disabled={busy} onClick={() => { void work.apply(item.task_id); setReview(null); }}>Apply</Button><Button size="sm" variant="outline" onClick={() => setReview(null)}>Cancel</Button></div></div> : <Button size="sm" variant="outline" disabled={busy} onClick={() => setReview(item.task_id)}>Review & apply</Button>)}
            </div></TableCell>
          </TableRow>)}
          {table.total === 0 && <TableRow><TableCell colSpan={5} className="text-muted-foreground">No software prepared yet.</TableCell></TableRow>}
        </TableBody></Table></ResourceTable><TablePagination table={table} label="preparations" /></>}
      {work.nextCursor && <Button className="mt-3" variant="outline" disabled={work.loading} onClick={() => void work.loadOlder()}>Load older preparations</Button>}
    </ResourcePanel>
  </div>;
}
