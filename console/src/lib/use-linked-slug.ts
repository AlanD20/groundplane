import { useRef, useState } from 'react'

export function normalizeSlug(value: string): string {
  return value
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
}

export function useLinkedSlug() {
  const [name, setNameValue] = useState('')
  const [slug, setSlugValue] = useState('')
  const slugWasEdited = useRef(false)

  function setName(value: string) {
    setNameValue(value)
    if (!slugWasEdited.current) setSlugValue(normalizeSlug(value))
  }

  function setSlug(value: string) {
    slugWasEdited.current = true
    setSlugValue(normalizeSlug(value))
  }

  function reset() {
    setNameValue('')
    setSlugValue('')
    slugWasEdited.current = false
  }

  return { name, slug, setName, setSlug, reset }
}
