import { Trash2 } from 'lucide-react'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import { DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { CopyButton } from '@/components/common/copy-button'
import { TaskLink } from '@/components/common/task-link'
import { StatusBadge } from '@/components/common/status-badge'
import { imageSize, type HostImage } from './api'
import { imageName, shortImageId } from './image-presentation'

export function ImageDetails({ image, unavailable, onClose, onRemove }: {
  image: HostImage
  unavailable: boolean
  onClose: () => void
  onRemove: () => void
}) {
  return <Drawer open onOpenChange={open => { if (!open) onClose() }}><DrawerContent>
    <DialogHeader>
      <DialogTitle className="break-words">{imageName(image)}</DialogTitle>
      <DialogDescription>One local image, with all names that point to it.</DialogDescription>
    </DialogHeader>
    <div className="flex flex-wrap gap-2">
      <Badge variant="outline">{imageSize(image.size_bytes)}</Badge>
      <Badge variant={image.containers ? 'primary' : 'muted'}>{image.containers} containers</Badge>
      <Badge variant="outline">{image.tags.length} tags</Badge>
    </div>
    <section className="space-y-2"><h3 className="text-sm font-medium">Image ID</h3><Reference value={image.id} /></section>
    <section className="space-y-2"><h3 className="text-sm font-medium">Local tags</h3>
      {image.tags.length ? image.tags.map(tag => <Reference key={tag} value={tag} />) : <p className="text-sm text-muted-foreground">No local tags. This image is available by digest.</p>}
    </section>
    {image.digests.length > 0 && <details className="rounded-lg border border-border p-3">
      <summary className="cursor-pointer text-sm font-medium">Repository digests ({image.digests.length})</summary>
      <div className="mt-3 space-y-2">{image.digests.map(digest => <Reference key={digest} value={digest} />)}</div>
    </details>}
    {image.fetches.length > 0 && <section className="space-y-3"><h3 className="text-sm font-medium">Fetch history</h3>
      {image.fetches.map(fetch => <div key={fetch.task_id} className="space-y-2 rounded-lg border border-border p-3 text-xs">
        <p className="break-all font-medium">{fetch.requested}</p>
        <div className="flex flex-wrap items-center gap-2"><StatusBadge status={fetch.status} label={fetch.status.replaceAll('_', ' ')} /><span className="text-muted-foreground">{new Date(fetch.requested_at).toLocaleString()}</span></div>
        <div className="flex flex-wrap items-center gap-2"><span className="text-muted-foreground">Selected digest</span><code>{shortImageId(fetch.image.split('@')[1] ?? fetch.image)}</code><CopyButton value={fetch.image} label="Copy selected image digest" /></div>
        <TaskLink taskId={fetch.task_id} onClick={onClose} />
      </div>)}
      <p className="text-xs text-muted-foreground">Requested tags are historical. Only completed Tasks confirm a successful Fetch.</p>
    </section>}
    {image.removal_blocked && <p role="status" className="rounded-lg border border-border bg-surface p-3 text-sm text-muted-foreground">{image.removal_blocked}</p>}
    <div className="flex justify-end gap-2 border-t border-border pt-4">
      <Button variant="outline" onClick={onClose}>Close</Button>
      <Button variant="destructive" disabled={unavailable || !!image.removal_blocked} onClick={onRemove}><Trash2 className="size-4" /> Remove image</Button>
    </div>
  </DrawerContent></Drawer>
}

function Reference({ value }: { value: string }) {
  return <div className="flex min-w-0 items-start gap-2 rounded-md bg-surface p-2"><code className="min-w-0 flex-1 break-all text-xs">{value}</code><CopyButton value={value} label="Copy reference" /></div>
}
