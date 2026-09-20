import type { operations } from '@/lib/api.generated'

type GeneratedCreate = operations['backing-service.create']['requestBody']['content']['application/json']
type CreateBase = Omit<GeneratedCreate, 'adapter' | 'authentication' | 'image' | 'hooks'>
export type BackingHooks = NonNullable<GeneratedCreate['hooks']>

export type BackingServiceCreateRequest = CreateBase & (
  | { adapter: 'postgres:16'; authentication?: never; image?: never }
  | { adapter: 'valkey:9'; authentication: 'username_password' | 'password' | 'none'; image?: never }
  | { adapter: 'custom'; image: string; authentication?: never; hooks?: BackingHooks }
)

export type BackingServiceCreatedResponse = operations['backing-service.create']['responses'][201]['content']['application/json']
