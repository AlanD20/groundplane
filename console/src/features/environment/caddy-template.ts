const maximumTemplateBytes = 32 * 1024

export function caddyTemplateError(template: string): string | null {
  if (new TextEncoder().encode(template).byteLength > maximumTemplateBytes) {
    return 'Caddyfile template must not exceed 32 KiB.'
  }
  if (template.includes('\0')) return 'Caddyfile template must not contain NUL bytes.'
  return null
}

export async function readCaddyTemplate(file: Pick<File, 'size' | 'arrayBuffer'>): Promise<string> {
  if (file.size > maximumTemplateBytes) throw new Error('Caddyfile template must not exceed 32 KiB.')
  const buffer = await file.arrayBuffer()
  if (buffer.byteLength > maximumTemplateBytes) throw new Error('Caddyfile template must not exceed 32 KiB.')
  let template: string
  try {
    template = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(buffer)
  } catch {
    throw new Error('Caddyfile template must be valid UTF-8.')
  }
  const error = caddyTemplateError(template)
  if (error) throw new Error(error)
  return template
}
