import { cn } from '@/lib/utils'

function Input({ className, type, ...props }: React.ComponentProps<'input'>) {
  return (
    <input
      type={type}
      data-slot="input"
      className={cn(
        'flex h-8 w-full min-w-0 rounded-lg border border-input bg-background px-2.5 py-1 text-sm outline-none transition-colors',
        'placeholder:text-muted-foreground focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/40',
        'disabled:pointer-events-none disabled:opacity-50 file:border-0 file:bg-transparent file:text-sm',
        className,
      )}
      {...props}
    />
  )
}

export { Input }
