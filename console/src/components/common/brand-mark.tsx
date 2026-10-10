import mark from "../../../../branding/mark-monochrome.svg";
import { cn } from "@/lib/utils";

export function BrandMark({ className }: { className?: string }) {
  return (
    <span
      aria-hidden="true"
      className={cn(
        "inline-block size-6 shrink-0 bg-current text-primary",
        className,
      )}
      style={{ mask: `url("${mark}") center / contain no-repeat` }}
    />
  );
}
