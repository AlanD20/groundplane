import type { ReactNode } from "react";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";

export function Field({
  id,
  label,
  children,
  description,
}: {
  id: string;
  label: string;
  children: ReactNode;
  description?: string;
}) {
  return (
    <div className="min-w-0 space-y-2">
      <Label htmlFor={id}>{label}</Label>
      {children}
      {description && (
        <p id={`${id}-help`} className="text-xs text-muted-foreground">
          {description}
        </p>
      )}
    </div>
  );
}

export function ChoiceError({
  error,
  onRetry,
}: {
  error?: string;
  onRetry: () => void;
}) {
  if (!error) return null;
  return (
    <div role="alert" className="flex flex-wrap items-center gap-2">
      <p className="text-xs text-destructive">{error}</p>
      <Button type="button" size="sm" variant="outline" onClick={onRetry}>
        Try again
      </Button>
    </div>
  );
}
