import type { ComponentProps } from 'react'

export function Radio(props: Omit<ComponentProps<'input'>, 'type'>) {
  return <input {...props} type="radio" data-slot="radio" />
}
