import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { ArrowLeft, Boxes, RefreshCw, Trash2 } from 'lucide-react'
import { PageHeader } from '@/components/common/page-header'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Select } from '@/components/ui/select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { ImageFetchCard } from './image-fetch-card'
import { imageReferences, imageSize, type HostImage } from './api'
import { imageName, imageRepositories, imageTag, shortImageId } from './image-presentation'
import { ImageDetails } from './image-details'
import { useImageInventory } from './use-image-inventory'
import { RemoveImageDialog } from './remove-image-dialog'
import { TablePagination, TableSortHead, useTableView } from '@/components/common/table-controls'

export default function ImagesPage() {
  const { inventory, loading, error, refresh } = useImageInventory()
  const [search, setSearch] = useState('')
  const [usage, setUsage] = useState('all')
  const [selected, setSelected] = useState<string | null>(null)
  const [removing, setRemoving] = useState<HostImage | null>(null)
  useEffect(() => {
    const timer = setInterval(() => { if (!document.hidden) void refresh() }, 10000)
    return () => clearInterval(timer)
  }, [refresh])
  const images = useMemo(() => (inventory?.images ?? []).filter(image =>
    (usage === 'all' || (usage === 'containers' ? image.containers > 0 : usage === 'removable' ? !image.removal_blocked : !!image.removal_blocked)) &&
    [image.id, ...imageReferences(image), ...image.fetches.map(fetch => fetch.requested)].some(value => value.toLowerCase().includes(search.toLowerCase())),
  ).sort((a, b) => imageName(a).localeCompare(imageName(b)) || b.created_at.localeCompare(a.created_at)), [inventory, search, usage])
  const detail = inventory?.images.find(image => image.id === selected)
  const table = useTableView(images, { name: imageName, created: image => Date.parse(image.created_at), size: image => image.size_bytes }, 'name', 'asc', `${search}/${usage}`)

  return <div className="flex flex-col gap-5">
    <Link to="/platform/host" className="flex w-fit items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground"><ArrowLeft className="size-3.5" /> Host</Link>
    <PageHeader title="Images" icon={<Boxes />} description="Images available on this host. One row per image, regardless of how many tags it has."
      actions={<Button variant="outline" onClick={() => void refresh()} disabled={loading}><RefreshCw className="size-4" /> Refresh</Button>} />
    <ImageFetchCard onSettled={refresh} />
    <Card><CardContent className="flex flex-col gap-4 p-4 sm:p-5">
      <div className="flex flex-wrap gap-3">
        <Input aria-label="Filter images" placeholder="Search names, tags or image IDs…" value={search} onChange={event => setSearch(event.target.value)} className="min-w-0 flex-1 basis-56" />
        <Select aria-label="Image usage" value={usage} onValueChange={setUsage} className="w-full sm:w-44" options={[
          { value: 'all', label: 'All images' }, { value: 'containers', label: 'Used by containers' }, { value: 'removable', label: 'Removable' }, { value: 'protected', label: 'Protected' },
        ]} />
        <div className="flex w-full items-center gap-2 md:hidden"><Select aria-label="Sort images by" value={table.field} onValueChange={table.sortBy} className="flex-1" options={[{value: 'name', label: 'Image name'}, {value: 'created', label: 'Creation date'}, {value: 'size', label: 'Size'}]} /><Button variant="outline" onClick={() => table.sortBy(table.field)} aria-label="Reverse image sort">{table.direction === 'asc' ? 'Ascending ↑' : 'Descending ↓'}</Button></div>
      </div>
      {error && <p role="alert" className="text-sm text-destructive">{error} {inventory ? 'The list may be stale.' : ''}</p>}
      {!inventory && loading && <p role="status" className="text-sm text-muted-foreground">Loading host images…</p>}
      {inventory && <>
        <div className="flex flex-wrap justify-between gap-2 text-xs text-muted-foreground"><span>{images.length} of {inventory.images.length} images</span><span>Updated {new Date(inventory.observed_at).toLocaleTimeString()}</span></div>
        <Table className="table-fixed"><TableHeader><TableRow>
          <TableSortHead sort={table} field="name">Image</TableSortHead><TableHead className="hidden w-[18%] lg:table-cell">Tags</TableHead><TableSortHead sort={table} field="created" className="hidden w-32 md:table-cell">Created</TableSortHead><TableSortHead sort={table} field="size" className="hidden w-24 sm:table-cell">Size</TableSortHead><TableHead className="w-28 sm:w-36">Usage</TableHead><TableHead className="w-12"><span className="sr-only">Actions</span></TableHead>
        </TableRow></TableHeader><TableBody>{table.rows.map(image => {
          const repositories = imageRepositories(image)
          const tags = [...new Set(image.tags.map(imageTag))]
          return <TableRow key={image.id} className="cursor-pointer" onClick={() => setSelected(image.id)}>
            <TableCell><Button variant="ghost" size="content" className="flex w-full min-w-0 flex-col items-start gap-1 text-left" onClick={() => setSelected(image.id)} aria-label={`Inspect ${imageName(image)}, image ${shortImageId(image.id)}`}>
              <span className="w-full truncate text-sm font-medium">{imageName(image)}</span>
              <span className="w-full truncate text-xs font-normal text-muted-foreground">{repositories[0] ?? 'No repository'}{repositories.length > 1 ? ` +${repositories.length - 1}` : ''}</span>
              <code className="text-[11px] font-normal text-muted-foreground/70">{shortImageId(image.id)}</code>
            </Button></TableCell>
            <TableCell className="hidden lg:table-cell"><div className="flex flex-wrap gap-1">
              {tags.slice(0, 2).map(tag => <Badge key={tag} variant="outline" className="max-w-full" title={tag}><span className="truncate">{tag}</span></Badge>)}
              {tags.length > 2 && <Badge variant="muted">+{tags.length - 2} tags</Badge>}
              {!tags.length && <span className="text-xs text-muted-foreground">By digest</span>}
            </div></TableCell>
            <TableCell className="hidden text-xs text-muted-foreground md:table-cell" title={new Date(image.created_at).toLocaleString()}>{new Date(image.created_at).toLocaleDateString()}</TableCell>
            <TableCell className="hidden whitespace-nowrap text-xs sm:table-cell">{imageSize(image.size_bytes)}</TableCell>
            <TableCell><Button variant="ghost" size="content" aria-label={`Show usage of ${imageName(image)}`} onClick={event => { event.stopPropagation(); setSelected(image.id) }}><Badge variant={image.containers > 0 ? 'primary' : image.removal_blocked ? 'muted' : 'success'} title={image.removal_blocked || 'No container or retained runtime requires this image'}>
              {image.containers > 0 ? `${image.containers} containers` : image.removal_blocked ? 'Protected' : 'Unused'}
            </Badge></Button></TableCell>
            <TableCell className="px-1"><Button variant="ghost" size="icon" aria-label={`Remove ${imageName(image)}, image ${shortImageId(image.id)}`} title={image.removal_blocked || 'Remove image and its tags'} disabled={!!image.removal_blocked || !!error || loading} onClick={event => { event.stopPropagation(); setRemoving(image) }}><Trash2 className="size-4" /></Button></TableCell>
          </TableRow>
        })}</TableBody></Table>
        <TablePagination table={table} label="Images" />
        {images.length === 0 && <p className="py-6 text-center text-sm text-muted-foreground">{inventory.images.length === 0 ? 'No images yet. Fetch an image to get started.' : 'No images match these filters.'}</p>}
        <p className="text-xs text-muted-foreground">Open an image for all tags, exact digests, fetch Tasks and protection details. Container counts include stopped containers.</p>
      </>}
    </CardContent></Card>
    {detail && <ImageDetails image={detail} unavailable={!!error || loading} onClose={() => setSelected(null)} onRemove={() => { setSelected(null); setRemoving(detail) }} />}
    {removing && <RemoveImageDialog image={removing} onClose={() => setRemoving(null)} onSettled={refresh} />}
  </div>
}
