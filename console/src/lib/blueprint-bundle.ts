import type { BlueprintApplyAudit, BlueprintFileAudit } from './types'

export const BLUEPRINT_BUNDLE_LIMITS = {
  files: 64,
  fileBytes: 256 * 1024,
  totalBytes: 768 * 1024,
  pathBytes: 240,
} as const

export type BlueprintInterpolation = {
  key: string
  value: string
}

export type BlueprintBundleManifest = {
  root: string
  compose_sources: string[]
  interpolation: Record<string, string>
  files: BlueprintFileAudit[]
}

export type BlueprintFilePart = {
  part: string
  content: File
}

export type BlueprintApplyRequest = {
  manifest: BlueprintBundleManifest
  parts: BlueprintFilePart[]
}

export type InspectedBlueprintFile = BlueprintFileAudit & {
  content: File
  pathError?: string
}

const interpolationKey = /^[A-Za-z_][A-Za-z0-9_]*$/
const textEncoder = new TextEncoder()

function includesBytes(value: Uint8Array, candidate: Uint8Array) {
  if (candidate.length === 0 || candidate.length > value.length) return false
  for (let offset = 0; offset <= value.length - candidate.length; offset += 1) {
    let matches = true
    for (let index = 0; index < candidate.length; index += 1) {
      if (value[offset + index] !== candidate[index]) {
        matches = false
        break
      }
    }
    if (matches) return true
  }
  return false
}

const byPath = (left: InspectedBlueprintFile, right: InspectedBlueprintFile) =>
  left.path < right.path ? -1 : left.path > right.path ? 1 : 0

function isWellFormedUnicode(value: string) {
  for (let index = 0; index < value.length; index += 1) {
    const unit = value.charCodeAt(index)
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(index + 1)
      if (next < 0xdc00 || next > 0xdfff) return false
      index += 1
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      return false
    }
  }
  return true
}

function validatePath(path: string) {
  if (!path) return 'Path is required.'
  if (!isWellFormedUnicode(path) || path.includes('\0')) return 'Path must be valid NUL-free UTF-8.'
  if (textEncoder.encode(path).byteLength > BLUEPRINT_BUNDLE_LIMITS.pathBytes) {
    return `Path exceeds ${BLUEPRINT_BUNDLE_LIMITS.pathBytes} UTF-8 bytes.`
  }
  if (path.startsWith('/') || path.includes('\\')) return 'Path must be relative and slash-separated.'
  const segments = path.split('/')
  if (segments.some((segment) => !segment || segment === '.' || segment === '..')) {
    return 'Path must be normalized and traversal-free.'
  }
  return undefined
}

function selectedPath(file: File, directorySelection: boolean) {
  if (!directorySelection || !file.webkitRelativePath) return file.name
  return file.webkitRelativePath.split('/').slice(1).join('/')
}

async function sha256(file: File) {
  const digest = await crypto.subtle.digest('SHA-256', await file.arrayBuffer())
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('')
}

export async function inspectBlueprintFiles(selected: File[], directorySelection: boolean) {
  const totalBytes = selected.reduce((sum, file) => sum + file.size, 0)
  const hashable =
    selected.length <= BLUEPRINT_BUNDLE_LIMITS.files &&
    totalBytes <= BLUEPRINT_BUNDLE_LIMITS.totalBytes &&
    selected.every((file) => file.size <= BLUEPRINT_BUNDLE_LIMITS.fileBytes)

  const inspected = await Promise.all(
    selected.map(async (content) => {
      const path = selectedPath(content, directorySelection)
      const pathError = validatePath(path)
      return {
        content,
        path,
        pathError,
        part: '',
        size: content.size,
        sha256: hashable && !pathError ? await sha256(content) : '',
      }
    }),
  )

  return inspected
    .sort(byPath)
    .map((file, index) => ({ ...file, part: `file-${String(index + 1).padStart(6, '0')}` }))
}

export function validateBlueprintSelection(
  files: InspectedBlueprintFile[],
  root: string,
  composeSources: string[],
  interpolation: BlueprintInterpolation[],
) {
  const errors: string[] = []
  const totalBytes = files.reduce((sum, file) => sum + file.size, 0)
  const pathCounts = new Map<string, number>()
  for (const file of files) pathCounts.set(file.path, (pathCounts.get(file.path) ?? 0) + 1)
  const duplicatePaths = [...pathCounts].filter(([, count]) => count > 1).map(([path]) => path)

  if (files.length === 0) errors.push('Select a bundle directory or files.')
  if (files.length > BLUEPRINT_BUNDLE_LIMITS.files) {
    errors.push(`Bundle has ${files.length} files; the limit is ${BLUEPRINT_BUNDLE_LIMITS.files}.`)
  }
  if (files.some((file) => file.size > BLUEPRINT_BUNDLE_LIMITS.fileBytes)) {
    errors.push(`Every file must be at most ${formatBlueprintBytes(BLUEPRINT_BUNDLE_LIMITS.fileBytes)}.`)
  }
  if (totalBytes > BLUEPRINT_BUNDLE_LIMITS.totalBytes) {
    errors.push(
      `Bundle totals ${formatBlueprintBytes(totalBytes)}; the limit is ${formatBlueprintBytes(BLUEPRINT_BUNDLE_LIMITS.totalBytes)}.`,
    )
  }
  for (const file of files) if (file.pathError) errors.push(`${file.path || '(empty path)'}: ${file.pathError}`)
  if (duplicatePaths.length > 0) errors.push(`Duplicate normalized paths: ${duplicatePaths.join(', ')}.`)
  if (!root) errors.push('Select the root Blueprint explicitly.')
  if (root && !files.some((file) => file.path === root)) errors.push('The selected root is not in the bundle.')
  if (composeSources.includes(root)) errors.push('The root Blueprint cannot be repeated as an additional Compose source.')
  if (new Set(composeSources).size !== composeSources.length) errors.push('Compose sources must be unique.')
  for (const source of composeSources) {
    if (!files.some((file) => file.path === source)) errors.push(`Compose source ${source} is not in the bundle.`)
  }

  const keys = interpolation.map(({ key }) => key)
  for (const key of keys) {
    if (!interpolationKey.test(key)) errors.push(`Interpolation key ${key || '(empty)'} is invalid.`)
  }
  if (new Set(keys).size !== keys.length) errors.push('Interpolation keys must be unique.')
  for (const entry of interpolation) {
    if (!isWellFormedUnicode(entry.value) || entry.value.includes('\0')) {
      errors.push(`Interpolation value for ${entry.key || '(empty key)'} must be valid NUL-free UTF-8.`)
    }
  }
  return errors
}

export function createBlueprintApplyRequest(
  files: InspectedBlueprintFile[],
  root: string,
  additionalComposeSources: string[],
  interpolation: BlueprintInterpolation[],
): BlueprintApplyRequest {
  const orderedFiles = [...files].sort(byPath)
  return {
    manifest: {
      root,
      compose_sources: [root, ...additionalComposeSources],
      interpolation: Object.fromEntries(interpolation.map(({ key, value }) => [key, value])),
      files: orderedFiles.map(({ path, part, size, sha256 }) => ({ path, part, size, sha256 })),
    },
    parts: orderedFiles.map(({ part, content }) => ({ part, content })),
  }
}

export async function createBlueprintTextApplyRequest(document: string): Promise<BlueprintApplyRequest> {
  const content = new File([document], 'blueprint.yaml', { type: 'application/yaml' })
  const files = await inspectBlueprintFiles([content], false)
  const errors = validateBlueprintSelection(files, 'blueprint.yaml', [], [])
  if (errors.length > 0) throw new Error(errors.join(' '))
  return createBlueprintApplyRequest(files, 'blueprint.yaml', [], [])
}

export async function createBlueprintMultipartBody(request: BlueprintApplyRequest) {
  const manifest = textEncoder.encode(JSON.stringify(request.manifest))
  const files = await Promise.all(
    request.parts.map(async ({ part, content }) => ({
      part,
      content: new Uint8Array(await content.arrayBuffer()),
    })),
  )
  const seed = request.manifest.files[0]?.sha256.slice(0, 32) ?? 'empty'
  let sequence = 0
  let boundary = ''
  while (!boundary) {
    const candidate = `groundplane-${seed}-${sequence}`
    const marker = textEncoder.encode(`--${candidate}`)
    if (!includesBytes(manifest, marker) && files.every((file) => !includesBytes(file.content, marker))) {
      boundary = candidate
    }
    sequence += 1
  }

  const chunks: BlobPart[] = [
    `--${boundary}\r\nContent-Disposition: form-data; name="manifest"\r\nContent-Type: application/json\r\n\r\n`,
    manifest,
    '\r\n',
  ]
  for (const file of files) {
    chunks.push(
      `--${boundary}\r\nContent-Disposition: form-data; name="${file.part}"\r\nContent-Type: application/octet-stream\r\n\r\n`,
      file.content,
      '\r\n',
    )
  }
  chunks.push(`--${boundary}--\r\n`)
  return {
    body: new Blob(chunks),
    contentType: `multipart/form-data; boundary=${boundary}`,
  }
}

export function blueprintApplyAudit(request: BlueprintApplyRequest): Omit<BlueprintApplyAudit, 'generation' | 'appliedAt'> {
  return {
    rootPath: request.manifest.root,
    composeSources: [...request.manifest.compose_sources],
    interpolationKeys: Object.keys(request.manifest.interpolation),
    files: request.manifest.files.map((file) => ({ ...file })),
  }
}

export function blueprintActivityParams(request: BlueprintApplyRequest) {
  return {
    root: request.manifest.root,
    compose_sources: JSON.stringify(request.manifest.compose_sources),
    interpolation_keys: JSON.stringify(Object.keys(request.manifest.interpolation)),
    file_count: String(request.manifest.files.length),
  }
}

export function formatBlueprintBytes(bytes: number) {
  if (bytes < 1024) return `${bytes} B`
  return `${(bytes / 1024).toFixed(bytes % 1024 === 0 ? 0 : 1)} KiB`
}
