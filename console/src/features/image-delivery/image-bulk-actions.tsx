import { Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { HostImage } from "./api";

export function ImageBulkActions({
  images,
  selected,
  disabled,
  onReview,
  onClear,
}: {
  images: HostImage[];
  selected: HostImage[];
  disabled: boolean;
  onReview: (images: HostImage[], unused: boolean) => void;
  onClear: () => void;
}) {
  const unused = images.filter((image) => image.containers === 0);
  return (
    <div
      className="flex flex-wrap items-center justify-end gap-3"
      aria-label="Image removal actions"
    >
      {selected.length > 0 && (
        <>
          <span className="mr-auto text-sm">
            {selected.length} selected across all pages
          </span>
          <Button variant="ghost" size="sm" onClick={onClear}>
            Clear selection
          </Button>
          <Button
            variant="destructive"
            size="sm"
            disabled={disabled}
            onClick={() => onReview(selected, false)}
          >
            <Trash2 /> Remove selected
          </Button>
        </>
      )}
      <Button
        variant="outline"
        size="sm"
        disabled={disabled || !unused.length}
        onClick={() => onReview(unused, true)}
      >
        <Trash2 /> Remove unused
      </Button>
    </div>
  );
}
