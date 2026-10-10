import { useState } from "react";
import { ImageFetchAction } from "./image-fetch-action";
import { Download, RefreshCw } from "lucide-react";
import { Input } from "@/components/ui/input";
import { SearchableSelect } from "@/components/ui/searchable-select";
import { Button } from "@/components/ui/button";
import { imageReferences, type HostImage } from "./api";
import { imageChoice } from "./image-presentation";
import { useImageInventory } from "./use-image-inventory";

// Authored input remains editable even when Docker cannot be observed. Saving
// a Service is not Deploy admission; only the backend can prove availability.
export function ImagePicker({
  id,
  value,
  onChange,
  referencesForImage,
  placeholder = "nginx:latest or a pinned registry reference",
}: {
  id: string;
  value: string;
  onChange: (value: string) => void;
  referencesForImage?: (image: HostImage) => string[];
  placeholder?: string;
}) {
  const [fetchOpen, setFetchOpen] = useState(false);
  const { inventory, loading, error, refresh } = useImageInventory();
  const choices = (inventory?.images ?? [])
    .map((image) =>
      imageChoice(
        image,
        referencesForImage
          ? referencesForImage(image)
          : [...imageReferences(image), image.id],
      ),
    )
    .filter((choice) => choice !== null)
    .sort((left, right) => left.label.localeCompare(right.label));
  const selected = choices.find((choice) => choice.references.includes(value));
  return (
    <div className="flex min-w-0 flex-col gap-2">
      <Input
        id={id}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
      />
      <div className="flex min-w-0 flex-wrap gap-2">
        <SearchableSelect
          aria-label="Choose an image on the Agent host"
          value={selected?.value ?? null}
          searchPlaceholder="Search image names, tags or digests…"
          emptyText="No matching host images. Fetch the image first if it is missing."
          placeholder={
            loading ? "Loading host images…" : "Choose an available image…"
          }
          options={choices.map((choice) => ({
            value: choice.value,
            label: choice.label,
          }))}
          onValueChange={onChange}
          disabled={loading || choices.length === 0}
          className="min-w-40 flex-1 [&>span]:truncate"
        />
        <Button
          type="button"
          variant="outline"
          size="icon"
          disabled={loading}
          onClick={() => void refresh()}
          aria-label="Refresh available images"
        >
          <RefreshCw className="size-4" />
        </Button>
        <Button
          type="button"
          variant="outline"
          onClick={() => setFetchOpen(true)}
        >
          <Download className="size-4" /> Fetch image
        </Button>
      </div>
      {error && (
        <p role="alert" className="text-xs text-destructive">
          {error}
        </p>
      )}
      <ImageFetchAction
        open={fetchOpen}
        onOpenChange={setFetchOpen}
        initialImage={value}
      />
    </div>
  );
}
