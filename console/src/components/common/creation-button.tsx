import type { ComponentProps } from "react";
import { Button } from "@/components/ui/button";

/** A collection has one creation entry point; its open form owns submission. */
export function CreationButton({
  active,
  ...props
}: ComponentProps<typeof Button> & { active: boolean }) {
  return active ? null : <Button type="button" {...props} />;
}
