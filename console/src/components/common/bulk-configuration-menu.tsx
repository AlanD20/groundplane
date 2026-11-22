import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { ChevronDown } from "lucide-react";

export type BulkConfigurationKind = "variables" | "secrets";

export function BulkConfigurationMenu({
  onSelect,
  disabled = false,
  size = "default",
}: {
  onSelect: (kind: BulkConfigurationKind) => void;
  disabled?: boolean;
  size?: "default" | "sm";
}) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger render={<Button variant="outline" size={size} disabled={disabled} />}>
        Bulk edit <ChevronDown className="size-3.5" />
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem onClick={() => onSelect("variables")}>
          Variables
        </DropdownMenuItem>
        <DropdownMenuItem onClick={() => onSelect("secrets")}>
          Secrets
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
