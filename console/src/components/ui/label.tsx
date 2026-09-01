import { cn } from '@/lib/utils'

function Label({ className, ...props }: React.ComponentProps<'label'>) {
  return (
    <label
      data-slot="label"
      className={cn('flex items-center gap-1 text-xs font-medium text-muted-foreground select-none', className)}
      {...props}
    />
  )
}

export { Label }
