import { useParams } from 'react-router-dom'

export function useRequiredParams<const Name extends string>(...names: Name[]): Record<Name, string> {
  const params = useParams()
  const required = {} as Record<Name, string>

  for (const name of names) {
    const value = params[name]
    if (!value) {
      throw new Error(`Missing required route parameter: ${name}`)
    }
    required[name] = value
  }

  return required
}
