import type { ReactNode } from "react";
import { Highlighter, Layers, X, type LucideIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuCheckboxItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuItem,
  DropdownMenuSub,
  DropdownMenuSubTrigger,
} from "@/components/ui/dropdown-menu";
import { topologyLayers, type TopologyLayer } from "./topology-model";

function Options({
  context,
  label,
  icon: Icon,
  active,
  children,
}: {
  context: boolean;
  label: string;
  icon: LucideIcon;
  active?: boolean;
  children: ReactNode;
}) {
  if (context)
    return (
      <DropdownMenuSub>
        <DropdownMenuSubTrigger>
          <Icon />
          {label}
        </DropdownMenuSubTrigger>
        <DropdownMenuContent align="start">{children}</DropdownMenuContent>
      </DropdownMenuSub>
    );
  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        render={<Button variant={active ? "secondary" : "outline"} size="sm" />}
      >
        <Icon />
        {label}
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start">{children}</DropdownMenuContent>
    </DropdownMenu>
  );
}

/** Both toolbar and blank-space menu share state and option handlers. */
export function TopologyViewControls({
  context = false,
  layers,
  onLayersChange,
  highlightKind,
  onHighlightChange,
  onlyRelated,
  onOnlyRelatedChange,
  hasFocus,
  selected,
  onClearSelection,
  counts,
  total,
  visible,
}: {
  context?: boolean;
  layers: TopologyLayer[];
  onLayersChange: (layers: TopologyLayer[]) => void;
  highlightKind: string | null;
  onHighlightChange: (kind: string | null) => void;
  onlyRelated: boolean;
  onOnlyRelatedChange: (value: boolean) => void;
  hasFocus: boolean;
  selected: boolean;
  onClearSelection: () => void;
  counts: Record<TopologyLayer, number>;
  total: number;
  visible: number;
}) {
  return (
    <>
      <Options context={context} label="Show" icon={Layers}>
        <DropdownMenuLabel>Connections</DropdownMenuLabel>
        {topologyLayers.map((layer) => (
          <DropdownMenuCheckboxItem
            key={layer}
            checked={layers.includes(layer)}
            closeOnClick={false}
            onCheckedChange={(checked) =>
              onLayersChange(
                checked
                  ? [...layers, layer]
                  : layers.filter((current) => current !== layer),
              )
            }
          >
            {layer}
            <span className="ml-auto text-muted-foreground">
              {counts[layer]}
            </span>
          </DropdownMenuCheckboxItem>
        ))}
        <DropdownMenuSeparator />
        <DropdownMenuCheckboxItem
          checked={onlyRelated}
          disabled={!hasFocus}
          closeOnClick={false}
          onCheckedChange={onOnlyRelatedChange}
        >
          Highlighted connections only
        </DropdownMenuCheckboxItem>
      </Options>
      <Options
        context={context}
        label={
          highlightKind
            ? `Highlight: ${highlightKind === "Backing" ? "Backing services" : highlightKind + "s"}`
            : "Highlight"
        }
        icon={Highlighter}
        active={!!highlightKind}
      >
        <DropdownMenuLabel>Highlight resources</DropdownMenuLabel>
        {[
          { kind: "Service", label: "Services" },
          { kind: "Route", label: "Routes" },
          { kind: "Backing", label: "Backing services" },
          { kind: "Volume", label: "Volumes" },
        ].map(({ kind, label }) => (
          <DropdownMenuCheckboxItem
            key={kind}
            checked={highlightKind === kind}
            onCheckedChange={(checked) =>
              onHighlightChange(checked ? kind : null)
            }
          >
            {label}
          </DropdownMenuCheckboxItem>
        ))}
        <DropdownMenuSeparator />
        <DropdownMenuItem onClick={() => onHighlightChange(null)}>
          Clear highlight
        </DropdownMenuItem>
      </Options>
      {selected &&
        (context ? (
          <DropdownMenuItem onClick={onClearSelection}>
            <X />
            Clear selection
          </DropdownMenuItem>
        ) : (
          <Button size="sm" variant="ghost" onClick={onClearSelection}>
            <X />
            Clear selection
          </Button>
        ))}
      {!context && (
        <span
          role="status"
          className="ml-auto px-1 text-xs text-muted-foreground"
        >
          {visible} / {total} connections
        </span>
      )}
    </>
  );
}
