import { Download } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuItem,
  DropdownMenuSub,
  DropdownMenuSubTrigger,
} from "@/components/ui/dropdown-menu";
import type { TopologyImageFormat } from "./topology-export";

export function TopologyExportMenu({
  context = false,
  busy,
  selectedCount,
  onExport,
}: {
  context?: boolean;
  busy: boolean;
  selectedCount: number;
  onExport: (format: TopologyImageFormat) => void;
}) {
  const options = (
    <>
      <DropdownMenuLabel>
        {selectedCount ? `${selectedCount} selected Services` : "All Services"}
      </DropdownMenuLabel>
      {(["svg", "png"] as const).map((format) => (
        <DropdownMenuItem
          key={format}
          disabled={busy}
          onClick={() => onExport(format)}
        >
          {format.toUpperCase()}
        </DropdownMenuItem>
      ))}
    </>
  );
  if (context)
    return (
      <DropdownMenuSub>
        <DropdownMenuSubTrigger disabled={busy}>
          <Download />
          {busy ? "Exporting…" : "Export"}
        </DropdownMenuSubTrigger>
        <DropdownMenuContent>{options}</DropdownMenuContent>
      </DropdownMenuSub>
    );
  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        disabled={busy}
        render={<Button variant="outline" size="sm" />}
      >
        <Download />
        {busy ? "Exporting…" : "Export"}
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start">{options}</DropdownMenuContent>
    </DropdownMenu>
  );
}
