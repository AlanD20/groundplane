'use client'

import { Button } from '@/components/ui/button'

import { useState } from 'react'
import { Check, Copy } from 'lucide-react'
import { cn } from '@/lib/utils'

export function CopyButton({ value, className, label }: { value: string; className?: string; label?: string }) {
  const [copied, setCopied] = useState(false)
  return (
    <Button variant="ghost" size="content"
      type="button"
      onClick={() => {
        navigator.clipboard?.writeText(value).catch(() => {})
        setCopied(true)
        setTimeout(() => setCopied(false), 1400)
      }}
      className={cn(
        'inline-flex items-center gap-1.5 rounded-md p-1.5 text-muted-foreground outline-none transition-colors hover:bg-muted hover:text-foreground focus-visible:ring-1 focus-visible:ring-ring',
        className,
      )}
      aria-label={label ?? 'Copy'}
    >
      {copied ? <Check className="size-3.5 text-success" /> : <Copy className="size-3.5" />}
      {label && <span className="text-xs">{copied ? 'Copied' : label}</span>}
    </Button>
  )
}
