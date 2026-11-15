import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Plus, RotateCcw, Trash2 } from "lucide-react";
export function ArgumentList({
  id,
  label,
  description,
  values,
  onChange,
  itemName = "argument",
  emptyLabel = "Image default",
  resetLabel = "Reset to image default",
  requireNonEmpty = false,
}: {
  id: string;
  label: string;
  description: string;
  values: string[];
  onChange: (values: string[]) => void;
  itemName?: string;
  emptyLabel?: string;
  resetLabel?: string;
  requireNonEmpty?: boolean;
}) {
  const hasEmptyValue = requireNonEmpty && values.some((value) => value === "");
  return (
    <fieldset className="rounded-lg border border-border p-3">
      <legend className="px-1 text-sm font-medium">{label}</legend>
      {description && (
        <p className="mb-3 text-xs text-muted-foreground">{description}</p>
      )}
      {values.length > 0 ? (
        <div className="space-y-2">
          {values.map((value, index) => (
            <div key={index} className="flex min-w-0 items-center gap-2">
              <Label className="sr-only" htmlFor={`${id}-${index}`}>
                {label} {index + 1}
              </Label>
              <Input
                id={`${id}-${index}`}
                className="font-mono"
                value={value}
                aria-invalid={
                  requireNonEmpty && value === "" ? true : undefined
                }
                aria-describedby={
                  requireNonEmpty && value === "" ? `${id}-error` : undefined
                }
                onChange={(event) => {
                  const next = [...values];
                  next[index] = event.target.value;
                  onChange(next);
                }}
              />
              <Button
                type="button"
                variant="ghost"
                size="icon"
                aria-label={`Remove ${itemName} ${index + 1}`}
                onClick={() =>
                  onChange(values.filter((_, candidate) => candidate !== index))
                }
              >
                <Trash2 />
              </Button>
            </div>
          ))}
        </div>
      ) : (
        <p className="rounded-md bg-muted/50 px-3 py-2 text-sm text-muted-foreground">
          {emptyLabel}
        </p>
      )}
      {hasEmptyValue && (
        <p
          id={`${id}-error`}
          className="mt-2 text-xs text-destructive"
          role="alert"
        >
          Alias values cannot be empty.
        </p>
      )}
      <div className="mt-3 flex flex-wrap gap-2">
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={() => onChange([...values, ""])}
        >
          <Plus /> Add {itemName}
        </Button>
        <Button
          type="button"
          variant="ghost"
          size="sm"
          disabled={values.length === 0}
          onClick={() => onChange([])}
        >
          <RotateCcw /> {resetLabel}
        </Button>
      </div>
    </fieldset>
  );
}
