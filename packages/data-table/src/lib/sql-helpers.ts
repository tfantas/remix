import type { DataManipulationOperation, DataMigrationOperation, TableRef } from './adapter.ts'

export type QuoteIdentifier = (value: string) => string

export function isDataManipulationOperation(
  operation: DataManipulationOperation | DataMigrationOperation,
): operation is DataManipulationOperation {
  return (
    operation.kind === 'select' ||
    operation.kind === 'count' ||
    operation.kind === 'exists' ||
    operation.kind === 'insert' ||
    operation.kind === 'insertMany' ||
    operation.kind === 'update' ||
    operation.kind === 'delete' ||
    operation.kind === 'upsert' ||
    operation.kind === 'raw'
  )
}

export function normalizeJoinType(type: string): string {
  if (type === 'left') {
    return 'left'
  }

  if (type === 'right') {
    return 'right'
  }

  return 'inner'
}

export function collectColumns(rows: Record<string, unknown>[]): string[] {
  let columns: string[] = []
  let seen = new Set<string>()

  for (let row of rows) {
    for (let key in row) {
      if (!Object.prototype.hasOwnProperty.call(row, key)) {
        continue
      }

      if (seen.has(key)) {
        continue
      }

      seen.add(key)
      columns.push(key)
    }
  }

  return columns
}

export function defaultIndexName(columns: string[]): string {
  return columns.join('_') + '_idx'
}

export function quotePath(path: string, quoteIdentifier: QuoteIdentifier): string {
  if (path === '*') {
    return '*'
  }

  return path
    .split('.')
    .map((segment) => {
      if (segment === '*') {
        return '*'
      }

      return quoteIdentifier(segment)
    })
    .join('.')
}

export function quoteTableRef(table: TableRef, quoteIdentifier: QuoteIdentifier): string {
  if (table.schema) {
    return quoteIdentifier(table.schema) + '.' + quoteIdentifier(table.name)
  }

  return quoteIdentifier(table.name)
}

export function quoteLiteral(
  value: unknown,
  options?: {
    booleansAsIntegers?: boolean
  },
): string {
  if (value === null) {
    return 'null'
  }

  if (typeof value === 'number' || typeof value === 'bigint') {
    return String(value)
  }

  if (typeof value === 'boolean') {
    if (options?.booleansAsIntegers) {
      return value ? '1' : '0'
    }

    return value ? 'true' : 'false'
  }

  if (value instanceof Date) {
    return quoteLiteral(value.toISOString(), options)
  }

  return "'" + String(value).replace(/'/g, "''") + "'"
}
