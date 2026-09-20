'use client'

import { Button } from '@/components/ui/button'

import { Download } from 'lucide-react'
import { cn } from '@/lib/utils'

export function DownloadButton({
  contents,
  filename,
  label = 'Download',
  mimeType = 'text/plain;charset=utf-8',
  className,
}: {
  contents: string
  filename: string
  label?: string
  mimeType?: string
  className?: string
}) {
  return (
    <Button variant="ghost" size="content"
      type="button"
      onClick={() => {
        const url = URL.createObjectURL(new Blob([contents], { type: mimeType }))
        const link = document.createElement('a')
        link.href = url
        link.download = filename
        link.click()
        URL.revokeObjectURL(url)
      }}
      className={cn(
        'inline-flex items-center gap-1.5 rounded-md p-1.5 text-muted-foreground outline-none transition-colors hover:bg-muted hover:text-foreground focus-visible:ring-1 focus-visible:ring-ring',
        className,
      )}
      aria-label={`${label} ${filename}`}
    >
      <Download className="size-3.5" />
      <span className="text-xs">{label}</span>
    </Button>
  )
}
