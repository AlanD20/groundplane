import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { ArrowLeft, Boxes, RefreshCw, Trash2 } from 'lucide-react'
import { PageHeader } from '@/components/common/page-header'
import { CopyButton } from '@/components/common/copy-button'
import { StatusBadge } from '@/components/common/status-badge'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Select } from '@/components/ui/select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { ImageFetchCard } from './image-fetch-card'
import { imageReferences, imageSize, type HostImage } from './api'
import { useImageInventory } from './use-image-inventory'
import { RemoveImageDialog } from './remove-image-dialog'

export default function ImagesPage() {
  const { inventory, loading, error, refresh } = useImageInventory()
  const [search, setSearch] = useState('')
  const [usage, setUsage] = useState('all')
  const [removing, setRemoving] = useState<HostImage | null>(null)
  useEffect(() => {
    const timer = setInterval(() => { if (!document.hidden) void refresh() }, 10000)
    return () => clearInterval(timer)
  }, [refresh])
  const images = useMemo(() => (inventory?.images ?? []).filter(image =>
    (usage === 'all' || (usage === 'containers' ? image.containers > 0 : image.containers === 0)) &&
    [image.id, ...imageReferences(image), ...image.fetches.map(fetch => fetch.requested)].some(value => value.toLowerCase().includes(search.toLowerCase())),
  ), [inventory, search, usage])
  return <div className="flex flex-col gap-6">
    <Link to="/platform/host" className="flex w-fit items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground"><ArrowLeft className="size-3.5" /> Host</Link>
    <PageHeader title="Images" icon={<Boxes />} description="Live images in the local Agent's Docker daemon. Fetching does not deploy or restart Services."
      actions={<Button variant="outline" onClick={() => void refresh()} disabled={loading}><RefreshCw className="size-4" /> Refresh</Button>} />
    <ImageFetchCard onSettled={refresh} />
    <Card><CardContent className="flex flex-col gap-4 p-5">
      <div className="flex flex-wrap items-center gap-3">
        <Input aria-label="Filter images" placeholder="Filter by repository, tag, digest or image ID…" value={search} onChange={event => setSearch(event.target.value)} className="min-w-48 flex-1" />
        <Select aria-label="Image usage" value={usage} onValueChange={setUsage} className="w-52" options={[
          { value: 'all', label: 'All images' }, { value: 'containers', label: 'Used by containers' }, { value: 'unused', label: 'No containers' },
        ]} />
      </div>
      {error && <p role="alert" className="text-sm text-destructive">{error} {inventory ? 'The inventory below may be stale.' : ''}</p>}
      {!inventory && loading && <p role="status" className="text-sm text-muted-foreground">Loading host images…</p>}
      {inventory && <>
        <p className="text-xs text-muted-foreground">{images.length} of {inventory.images.length} images · Observed {new Date(inventory.observed_at).toLocaleTimeString()}. Container counts include stopped containers; zero does not mean safe to delete.</p>
        <Table><TableHeader><TableRow><TableHead>Repository / references</TableHead><TableHead>Image ID</TableHead><TableHead>Size</TableHead><TableHead>Containers</TableHead><TableHead>Created</TableHead><TableHead>Removal</TableHead></TableRow></TableHeader>
          <TableBody>{images.map(image => <TableRow key={image.id}>
            <TableCell className="max-w-lg"><div className="flex flex-col gap-2">{imageReferences(image).map(reference => <div key={reference} className="flex min-w-0 items-center gap-2"><code className="min-w-0 break-all text-xs">{reference}</code><CopyButton value={reference} label="Copy image reference" /></div>)}{imageReferences(image).length === 0 && <span className="text-muted-foreground">Untagged</span>}
              {image.fetches.length > 0 && <details className="text-xs"><summary className="cursor-pointer text-muted-foreground">Requested as {Array.from(new Set(image.fetches.map(fetch => fetch.requested))).join(', ')}</summary>
                <div className="mt-2 flex flex-col gap-3">{image.fetches.map(fetch => <div key={fetch.task_id} className="flex flex-col gap-1"><code className="break-all">{fetch.requested} → {fetch.image}</code><div className="flex flex-wrap items-center gap-2"><StatusBadge status={fetch.status} label={fetch.status.replaceAll('_', ' ')} /><span className="text-muted-foreground">Requested {new Date(fetch.requested_at).toLocaleString()}</span></div><CopyButton value={`${fetch.requested} -> ${fetch.image}`} label="Copy requested tag and digest" /></div>)}<p className="text-muted-foreground">Retained Fetch attempts, not current Docker tags. Only completed Tasks confirm successful Fetch.</p></div>
              </details>}
            </div></TableCell>
            <TableCell><div className="flex items-center gap-1"><code title={image.id} className="text-xs">{image.id.replace('sha256:', '').slice(0, 12)}</code><CopyButton value={image.id} label="Copy image ID" /></div></TableCell>
            <TableCell className="whitespace-nowrap">{imageSize(image.size_bytes)}</TableCell><TableCell>{image.containers}</TableCell><TableCell className="whitespace-nowrap text-xs">{new Date(image.created_at).toLocaleDateString()}</TableCell>
            <TableCell className="max-w-64"><div className="flex flex-col items-start gap-2"><Button variant="outline" size="sm" disabled={!!image.removal_blocked || !!error || loading} aria-describedby={image.removal_blocked ? `image-protection-${image.id}` : undefined} onClick={() => setRemoving(image)}><Trash2 className="size-3.5" /> Remove</Button>{image.removal_blocked && <p id={`image-protection-${image.id}`} className="text-xs text-muted-foreground">{image.removal_blocked}</p>}</div></TableCell>
          </TableRow>)}</TableBody>
        </Table>
        {images.length === 0 && <p className="py-4 text-center text-sm text-muted-foreground">{inventory.images.length === 0 ? 'No host images. Fetch an image above to make it available.' : 'No images match these filters.'}</p>}
      </>}
    </CardContent></Card>
    {removing && <RemoveImageDialog image={removing} onClose={() => setRemoving(null)} onSettled={refresh} />}
  </div>
}
