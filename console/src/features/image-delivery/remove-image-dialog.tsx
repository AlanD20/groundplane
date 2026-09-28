import { useRef } from 'react'
import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'
import { newULID } from '@/lib/utils'
import { imageReferences, removeImage, type HostImage } from './api'

export function RemoveImageDialog({ image, onClose, onSettled }: { image: HostImage; onClose: () => void; onSettled: () => Promise<void> }) {
  const key = useRef(newULID())
  return <TaskRunnerDialog open onOpenChange={open => { if (!open) onClose() }} variant="drawer" title="Remove host image"
    type="remove" target={image.id} workspace="platform" startLabel="Remove image" steps={[]}
    description="Remove only this local Docker image. Registry content, Service configuration and application data are unchanged."
    executionCopy="GP rechecks containers and retained execution authority. Docker removal never uses force or prunes other images."
    review={<div className="space-y-3 text-sm"><code className="block break-all">{image.id}</code>{imageReferences(image).map(reference => <code key={reference} className="block break-all text-xs">{reference}</code>)}<p>New image selections are fenced during removal. If GP or Docker needs this image, removal fails without forcing it.</p></div>}
    onDispatch={async () => (await removeImage(image.id, key.current)).task_id} onSettled={onSettled} />
}
