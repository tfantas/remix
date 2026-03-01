import type { TableRef } from '../adapter.ts'
import type { IndexColumns, KeyColumns } from '../migrations.ts'

export function toTableRef(name: string): TableRef {
  let segments = name.split('.')

  if (segments.length === 1) {
    return { name }
  }

  return {
    schema: segments[0],
    name: segments.slice(1).join('.'),
  }
}

export function normalizeIndexColumns(columns: IndexColumns): string[] {
  return normalizeKeyColumns(columns)
}

export function normalizeKeyColumns(columns: KeyColumns): string[] {
  if (Array.isArray(columns)) {
    return [...columns]
  }

  return [columns]
}
