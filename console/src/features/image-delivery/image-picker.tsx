import { Link } from 'react-router-dom'
import { RefreshCw } from 'lucide-react'
import { Input } from '@/components/ui/input'
import { SearchableSelect } from '@/components/ui/searchable-select'
import { Button } from '@/components/ui/button'
import { imageReferences } from './api'
import { useImageInventory } from './use-image-inventory'

// Authored input remains editable even when Docker cannot be observed. Saving
// a Service is not Deploy admission; only the backend can prove availability.
export function ImagePicker({ id, value, onChange }: { id: string; value: string; onChange: (value: string) => void }) {
  const { inventory, loading, error, refresh } = useImageInventory()
  const references = [...new Set(inventory?.images.flatMap(imageReferences) ?? [])].sort()
  return <div className="flex min-w-0 flex-col gap-2">
    <Input id={id} value={value} onChange={event => onChange(event.target.value)} placeholder="nginx:latest or a pinned registry reference" />
    <div className="flex min-w-0 gap-2">
      <SearchableSelect aria-label="Choose an image on the Agent host" value={references.includes(value) ? value : null}
        searchPlaceholder="Search image names, tags or digests…" emptyText="No matching host images. Fetch the image first if it is missing."
        placeholder={loading ? 'Loading host images…' : 'Choose an available image…'}
        options={references.map(reference => ({ value: reference, label: reference }))} onValueChange={onChange}
        disabled={loading || references.length === 0} className="min-w-0 flex-1 [&>span]:truncate" />
      <Button type="button" variant="outline" size="icon" disabled={loading} onClick={() => void refresh()} aria-label="Refresh available images"><RefreshCw className="size-4" /></Button>
    </div>
    {error && <p role="alert" className="text-xs text-destructive">{error}</p>}
    <p className="text-xs text-muted-foreground">Missing an image? <Link to={`/platform/host/images?fetch=${encodeURIComponent(value)}`} target="_blank" rel="noreferrer" className="text-primary hover:underline">Fetch it in Images</Link>, then refresh this list. Fetch supports public registries and GP's private registry.</p>
  </div>
}
