import type { ReactNode } from "react";
import { AnimatedSection } from "./animated-section";
import { TableCell, TableRow } from "@/components/ui/table";

/** Keep row actions inside the owning table; callers retain the expanded row when filtering. */
export function TableExpansion({
  children,
  columns,
}: {
  children: ReactNode;
  columns: number;
}) {
  return (
    <TableRow
      className={
        children ? "hover:bg-transparent" : "border-b-0 hover:bg-transparent"
      }
    >
      <TableCell colSpan={columns} className="p-0">
        <AnimatedSection open={!!children}>
          <div className="min-w-0 px-4 pb-4">{children}</div>
        </AnimatedSection>
      </TableCell>
    </TableRow>
  );
}
