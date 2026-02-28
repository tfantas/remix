import type {
  AdapterCapabilityOverrides,
  DataManipulationRequest,
  DataMigrationRequest,
  DataMigrationResult,
  DataMigrationOperation,
  DataManipulationResult,
  DataManipulationOperation,
  DatabaseAdapter,
  SqlStatement,
  TransactionOptions,
  TransactionToken,
} from '@remix-run/data-table'
import { getTablePrimaryKey } from '@remix-run/data-table'
import {
  compileDataManipulationOperation,
  compileOperationToSql,
} from '@remix-run/data-table/internal/sql-compiler'
import type { Database as BetterSqliteDatabase, RunResult } from 'better-sqlite3'

let sqliteSqlCompilerOptions = {
  dialect: 'sqlite',
  rewriteMigrationOperation: rewriteSqliteMigrationOperation,
} as const

/**
 * Better SQLite3 database handle accepted by the sqlite adapter.
 */
export type SqliteDatabaseConnection = BetterSqliteDatabase

/**
 * Sqlite adapter configuration.
 */
export type SqliteDatabaseAdapterOptions = {
  capabilities?: AdapterCapabilityOverrides
}

/**
 * `DatabaseAdapter` implementation for Better SQLite3.
 */
export class SqliteDatabaseAdapter implements DatabaseAdapter {
  dialect = 'sqlite'
  capabilities

  #database: SqliteDatabaseConnection
  #transactions = new Set<string>()
  #transactionCounter = 0

  constructor(database: SqliteDatabaseConnection, options?: SqliteDatabaseAdapterOptions) {
    this.#database = database
    this.capabilities = {
      returning: options?.capabilities?.returning ?? true,
      savepoints: options?.capabilities?.savepoints ?? true,
      upsert: options?.capabilities?.upsert ?? true,
      transactionalDdl: options?.capabilities?.transactionalDdl ?? true,
      migrationLock: options?.capabilities?.migrationLock ?? false,
    }
  }

  compileSql(operation: DataManipulationOperation | DataMigrationOperation): SqlStatement[] {
    return compileOperationToSql(operation, sqliteSqlCompilerOptions)
  }

  async execute(request: DataManipulationRequest): Promise<DataManipulationResult> {
    if (request.operation.kind === 'insertMany' && request.operation.values.length === 0) {
      return {
        affectedRows: 0,
        insertId: undefined,
        rows: request.operation.returning ? [] : undefined,
      }
    }

    let statement = compileDataManipulationOperation(request.operation, sqliteSqlCompilerOptions)
    let prepared = this.#database.prepare(statement.text)

    if (prepared.reader) {
      let rows = normalizeRows(prepared.all(...statement.values))

      if (request.operation.kind === 'count' || request.operation.kind === 'exists') {
        rows = normalizeCountRows(rows)
      }

      return {
        rows,
        affectedRows: normalizeAffectedRowsForReader(request.operation.kind, rows),
        insertId: normalizeInsertIdForReader(request.operation.kind, request.operation, rows),
      }
    }

    let result = prepared.run(...statement.values)

    return {
      affectedRows: normalizeAffectedRowsForRun(request.operation.kind, result),
      insertId: normalizeInsertIdForRun(request.operation.kind, request.operation, result),
    }
  }

  async migrate(request: DataMigrationRequest): Promise<DataMigrationResult> {
    let statements = this.compileSql(request.operation)

    for (let statement of statements) {
      let prepared = this.#database.prepare(statement.text)
      prepared.run(...statement.values)
    }

    return {
      affectedObjects: statements.length,
    }
  }

  async beginTransaction(options?: TransactionOptions): Promise<TransactionToken> {
    if (options?.isolationLevel === 'read uncommitted') {
      this.#database.pragma('read_uncommitted = true')
    }

    this.#database.exec('begin')

    this.#transactionCounter += 1
    let token = { id: 'tx_' + String(this.#transactionCounter) }
    this.#transactions.add(token.id)

    return token
  }

  async commitTransaction(token: TransactionToken): Promise<void> {
    this.#assertTransaction(token)
    this.#database.exec('commit')
    this.#transactions.delete(token.id)
  }

  async rollbackTransaction(token: TransactionToken): Promise<void> {
    this.#assertTransaction(token)
    this.#database.exec('rollback')
    this.#transactions.delete(token.id)
  }

  async createSavepoint(token: TransactionToken, name: string): Promise<void> {
    this.#assertTransaction(token)
    this.#database.exec('savepoint ' + quoteIdentifier(name))
  }

  async rollbackToSavepoint(token: TransactionToken, name: string): Promise<void> {
    this.#assertTransaction(token)
    this.#database.exec('rollback to savepoint ' + quoteIdentifier(name))
  }

  async releaseSavepoint(token: TransactionToken, name: string): Promise<void> {
    this.#assertTransaction(token)
    this.#database.exec('release savepoint ' + quoteIdentifier(name))
  }

  #assertTransaction(token: TransactionToken): void {
    if (!this.#transactions.has(token.id)) {
      throw new Error('Unknown transaction token: ' + token.id)
    }
  }
}

/**
 * Creates a sqlite `DatabaseAdapter`.
 * @param database Better SQLite3 database instance.
 * @param options Optional adapter capability overrides.
 * @returns A configured sqlite adapter.
 */
export function createSqliteDatabaseAdapter(
  database: SqliteDatabaseConnection,
  options?: SqliteDatabaseAdapterOptions,
): SqliteDatabaseAdapter {
  return new SqliteDatabaseAdapter(database, options)
}

function normalizeRows(rows: unknown[]): Record<string, unknown>[] {
  return rows.map((row) => {
    if (typeof row !== 'object' || row === null) {
      return {}
    }

    return { ...(row as Record<string, unknown>) }
  })
}

function normalizeCountRows(rows: Record<string, unknown>[]): Record<string, unknown>[] {
  return rows.map((row) => {
    let count = row.count

    if (typeof count === 'string') {
      let numeric = Number(count)

      if (!Number.isNaN(numeric)) {
        return {
          ...row,
          count: numeric,
        }
      }
    }

    if (typeof count === 'bigint') {
      return {
        ...row,
        count: Number(count),
      }
    }

    return row
  })
}

function normalizeAffectedRowsForReader(
  kind: DataManipulationRequest['operation']['kind'],
  rows: Record<string, unknown>[],
): number | undefined {
  if (isWriteStatementKind(kind)) {
    return rows.length
  }

  return undefined
}

function normalizeInsertIdForReader(
  kind: DataManipulationRequest['operation']['kind'],
  operation: DataManipulationRequest['operation'],
  rows: Record<string, unknown>[],
): unknown {
  if (!isInsertStatementKind(kind) || !isInsertStatement(operation)) {
    return undefined
  }

  let primaryKey = getTablePrimaryKey(operation.table)

  if (primaryKey.length !== 1) {
    return undefined
  }

  let key = primaryKey[0]
  let row = rows[rows.length - 1]

  return row ? row[key] : undefined
}

function normalizeAffectedRowsForRun(
  kind: DataManipulationRequest['operation']['kind'],
  result: RunResult,
): number | undefined {
  if (kind === 'select' || kind === 'count' || kind === 'exists') {
    return undefined
  }

  return result.changes
}

function normalizeInsertIdForRun(
  kind: DataManipulationRequest['operation']['kind'],
  operation: DataManipulationRequest['operation'],
  result: RunResult,
): unknown {
  if (!isInsertStatementKind(kind) || !isInsertStatement(operation)) {
    return undefined
  }

  if (getTablePrimaryKey(operation.table).length !== 1) {
    return undefined
  }

  return result.lastInsertRowid
}

function quoteIdentifier(value: string): string {
  return '"' + value.replace(/"/g, '""') + '"'
}


function isWriteStatementKind(kind: DataManipulationRequest['operation']['kind']): boolean {
  return (
    kind === 'insert' ||
    kind === 'insertMany' ||
    kind === 'update' ||
    kind === 'delete' ||
    kind === 'upsert'
  )
}

function isInsertStatementKind(kind: DataManipulationRequest['operation']['kind']): boolean {
  return kind === 'insert' || kind === 'insertMany' || kind === 'upsert'
}

function isInsertStatement(
  operation: DataManipulationRequest['operation'],
): operation is Extract<
  DataManipulationRequest['operation'],
  { kind: 'insert' | 'insertMany' | 'upsert' }
> {
  return (
    operation.kind === 'insert' || operation.kind === 'insertMany' || operation.kind === 'upsert'
  )
}

function rewriteSqliteMigrationOperation(operation: DataMigrationOperation): DataMigrationOperation[] {
  if (operation.kind !== 'alterTable') {
    return [operation]
  }

  let changes = operation.changes.filter((change) => change.kind !== 'setTableComment')

  if (changes.length === 0) {
    return []
  }

  return [
    {
      ...operation,
      changes,
    },
  ]
}
