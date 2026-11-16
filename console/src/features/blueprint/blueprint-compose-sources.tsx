import type { Dispatch, SetStateAction } from "react";
import {
  ReorderList,
  ReorderItem,
  reorderItems,
} from "@/components/common/reorder-list";
import { Select, type SelectOption } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { ArrowDown, ArrowUp, Plus, Trash2 } from "lucide-react";

export function BlueprintComposeSources({
  rootPath,
  changeRoot,
  inputOptions,
  composeSources,
  setComposeSources,
  moveSource,
  sourceCandidate,
  setSourceCandidate,
  sourceOptions,
  addSource,
}: {
  rootPath: string;
  changeRoot: (path: string) => void;
  inputOptions: SelectOption[];
  composeSources: string[];
  setComposeSources: Dispatch<SetStateAction<string[]>>;
  moveSource: (index: number, offset: -1 | 1) => void;
  sourceCandidate: string;
  setSourceCandidate: (path: string) => void;
  sourceOptions: SelectOption[];
  addSource: () => void;
}) {
  return (
    <section className="space-y-3 rounded-xl border border-border bg-card p-4">
      <div>
        <h3 className="text-sm font-medium">2. Ordered Compose sources</h3>
        <p className="text-xs text-muted-foreground">
          The root stays first; drag additional sources to reorder.
        </p>
      </div>
      <div className="space-y-1.5">
        <Label htmlFor="blueprint-root">Root Blueprint</Label>
        <Select
          id="blueprint-root"
          value={rootPath}
          onValueChange={changeRoot}
          options={inputOptions}
          placeholder="Select the root file…"
        />
      </div>
      {rootPath && (
        <div className="flex items-center gap-2 rounded-lg border border-primary/30 bg-primary/5 px-3 py-2 text-sm">
          <span className="font-mono text-xs text-muted-foreground">01</span>
          <span className="min-w-0 truncate font-mono">{rootPath}</span>
          <Badge className="ml-auto" variant="outline">
            root
          </Badge>
        </div>
      )}
      <ReorderList
        className="space-y-2"
        onMove={(from, to) =>
          setComposeSources((current) => reorderItems(current, from, to))
        }
      >
        {composeSources.map((source, index) => (
          <ReorderItem
            index={index}
            label={source}
            key={source}
            className="flex items-center gap-2 rounded-lg border border-border bg-background px-3 py-2 text-sm"
          >
            <span className="font-mono text-xs text-muted-foreground">
              {String(index + 2).padStart(2, "0")}
            </span>
            <span className="min-w-0 flex-1 truncate font-mono">{source}</span>
            <Button
              aria-label={`Move ${source} up`}
              variant="ghost"
              size="icon-xs"
              disabled={index === 0}
              onClick={() => moveSource(index, -1)}
            >
              <ArrowUp />
            </Button>
            <Button
              aria-label={`Move ${source} down`}
              variant="ghost"
              size="icon-xs"
              disabled={index === composeSources.length - 1}
              onClick={() => moveSource(index, 1)}
            >
              <ArrowDown />
            </Button>
            <Button
              aria-label={`Remove ${source} source`}
              variant="ghost"
              size="icon-xs"
              onClick={() =>
                setComposeSources((current) =>
                  current.filter((candidate) => candidate !== source),
                )
              }
            >
              <Trash2 />
            </Button>
          </ReorderItem>
        ))}
      </ReorderList>
      <div className="flex flex-col gap-2 sm:flex-row">
        <Select
          id="blueprint-compose-source"
          aria-label="Additional Compose source"
          value={sourceCandidate}
          onValueChange={setSourceCandidate}
          options={sourceOptions}
          placeholder="Select an additional source…"
        />
        <Button
          variant="outline"
          disabled={!sourceCandidate}
          onClick={addSource}
        >
          <Plus /> Add source
        </Button>
      </div>
    </section>
  );
}
