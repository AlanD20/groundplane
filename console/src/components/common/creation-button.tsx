import type { ComponentProps } from "react";
import { Button } from "@/components/ui/button";

export function CreationButton({
  active,
  onOpenChange,
  ...props
}: Omit<ComponentProps<typeof Button>, "onClick"> & {
  active: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  return (
    <Button
      type="button"
      aria-expanded={active}
      onClick={() => onOpenChange(!active)}
      {...props}
    />
  );
}
