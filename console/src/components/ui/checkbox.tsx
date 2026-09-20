import type { ComponentProps } from 'react'

export function Checkbox(props: Omit<ComponentProps<'input'>, 'type'>) {
  return <input {...props} type="checkbox" data-slot="checkbox" />
}
