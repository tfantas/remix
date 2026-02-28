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

let mysqlSqlCompilerOptions = {
  dialect: 'mysql',
} as const

/**
 * Row-array response shape for mysql query calls.
 */
export type MysqlQueryRows = Record<string, unknown>[]

/**
 * Metadata shape for mysql write results.
 */
export type MysqlQueryResultHeader = {
  affectedRows: number
  insertId: unknown
}

/**
 * Supported mysql `query()` response tuple.
 */
export type MysqlQueryResponse = [result: unknown, fields?: unknown]

/**
 * Single mysql connection contract used by this adapter.
 */
export type MysqlDatabaseConnection = {
  query(text: string, values?: unknown[]): Promise<MysqlQueryResponse>
  beginTransaction(): Promise<void>
  commit(): Promise<void>
  rollback(): Promise<void>
  release?: () => void
}

/**
 * Mysql pool contract used by this adapter.
 */
export type MysqlDatabasePool = {
  query(text: string, values?: unknown[]): Promise<MysqlQueryResponse>
  getConnection(): Promise<MysqlDatabaseConnection>
}

/**
 * Mysql adapter configuration.
 */
export type MysqlDatabaseAdapterOptions = {
  capabilities?: AdapterCapabilityOverrides
}

type TransactionState = {
  connection: MysqlDatabaseConnection
  releaseOnClose: boolean
}

type MysqlQueryable = MysqlDatabasePool | MysqlDatabaseConnection

/**
 * `DatabaseAdapter` implementation for mysql-compatible clients.
 */
export class MysqlDatabaseAdapter implements DatabaseAdapter {
  dialect = 'mysql'
  capabilities

  #client: MysqlQueryable
  #transactions = new Map<string, TransactionState>()
  #transactionCounter = 0

  constructor(client: MysqlQueryable, options?: MysqlDatabaseAdapterOptions) {
    this.#client = client
    this.capabilities = {
      returning: options?.capabilities?.returning ?? false,
      savepoints: options?.capabilities?.savepoints ?? true,
      upsert: options?.capabilities?.upsert ?? true,
      transactionalDdl: options?.capabilities?.transactionalDdl ?? false,
      migrationLock: options?.capabilities?.migrationLock ?? true,
    }
  }

  compileSql(operation: DataManipulationOperation | DataMigrationOperation): SqlStatement[] {
    return compileOperationToSql(operation, mysqlSqlCompilerOptions)
  }

  async execute(request: DataManipulationRequest): Promise<DataManipulationResult> {
    if (request.operation.kind === 'insertMany' && request.operation.values.length === 0) {
      return {
        affectedRows: 0,
        insertId: undefined,
        rows: request.operation.returning ? [] : undefined,
      }
    }

    let statement = compileDataManipulationOperation(request.operation, mysqlSqlCompilerOptions)
    let client = this.#resolveClient(request.transaction)
    let [result] = await client.query(statement.text, statement.values)

    if (isRowsResult(result)) {
      let rows = normalizeRows(result)

      if (request.operation.kind === 'count' || request.operation.kind === 'exists') {
        rows = normalizeCountRows(rows)
      }

      return { rows }
    }

    let header = normalizeHeader(result)

    return {
      affectedRows: header.affectedRows,
      insertId: normalizeInsertId(request.operation.kind, request.operation, header),
    }
  }

  async migrate(request: DataMigrationRequest): Promise<DataMigrationResult> {
    let statements = this.compileSql(request.operation)
    let client = this.#resolveClient(request.transaction)

    for (let statement of statements) {
      await client.query(statement.text, statement.values)
    }

    return {
      affectedObjects: statements.length,
    }
  }

  async beginTransaction(options?: TransactionOptions): Promise<TransactionToken> {
    let releaseOnClose = false
    let connection: MysqlDatabaseConnection

    if (isMysqlPool(this.#client)) {
      connection = await this.#client.getConnection()
      releaseOnClose = true
    } else {
      connection = this.#client
    }

    if (options?.isolationLevel) {
      await connection.query('set transaction isolation level ' + options.isolationLevel)
    }

    if (options?.readOnly !== undefined) {
      await connection.query(
        options.readOnly ? 'set transaction read only' : 'set transaction read write',
      )
    }

    await connection.beginTransaction()

    this.#transactionCounter += 1
    let token = { id: 'tx_' + String(this.#transactionCounter) }

    this.#transactions.set(token.id, {
      connection,
      releaseOnClose,
    })

    return token
  }

  async commitTransaction(token: TransactionToken): Promise<void> {
    let transaction = this.#transactions.get(token.id)

    if (!transaction) {
      throw new Error('Unknown transaction token: ' + token.id)
    }

    try {
      await transaction.connection.commit()
    } finally {
      this.#transactions.delete(token.id)

      if (transaction.releaseOnClose) {
        transaction.connection.release?.()
      }
    }
  }

  async rollbackTransaction(token: TransactionToken): Promise<void> {
    let transaction = this.#transactions.get(token.id)

    if (!transaction) {
      throw new Error('Unknown transaction token: ' + token.id)
    }

    try {
      await transaction.connection.rollback()
    } finally {
      this.#transactions.delete(token.id)

      if (transaction.releaseOnClose) {
        transaction.connection.release?.()
      }
    }
  }

  async createSavepoint(token: TransactionToken, name: string): Promise<void> {
    let connection = this.#transactionConnection(token)
    await connection.query('savepoint ' + quoteIdentifier(name))
  }

  async rollbackToSavepoint(token: TransactionToken, name: string): Promise<void> {
    let connection = this.#transactionConnection(token)
    await connection.query('rollback to savepoint ' + quoteIdentifier(name))
  }

  async releaseSavepoint(token: TransactionToken, name: string): Promise<void> {
    let connection = this.#transactionConnection(token)
    await connection.query('release savepoint ' + quoteIdentifier(name))
  }

  async acquireMigrationLock(): Promise<void> {
    await this.#client.query('select get_lock(?, 60)', ['data_table_migrations'])
  }

  async releaseMigrationLock(): Promise<void> {
    await this.#client.query('select release_lock(?)', ['data_table_migrations'])
  }

  #resolveClient(token: TransactionToken | undefined): MysqlDatabaseConnection | MysqlDatabasePool {
    if (!token) {
      return this.#client
    }

    return this.#transactionConnection(token)
  }

  #transactionConnection(token: TransactionToken): MysqlDatabaseConnection {
    let transaction = this.#transactions.get(token.id)

    if (!transaction) {
      throw new Error('Unknown transaction token: ' + token.id)
    }

    return transaction.connection
  }
}

/**
 * Creates a mysql `DatabaseAdapter`.
 * @param client Mysql pool or connection.
 * @param options Optional adapter capability overrides.
 * @returns A configured mysql adapter.
 */
export function createMysqlDatabaseAdapter(
  client: MysqlQueryable,
  options?: MysqlDatabaseAdapterOptions,
): MysqlDatabaseAdapter {
  return new MysqlDatabaseAdapter(client, options)
}

function isMysqlPool(client: MysqlQueryable): client is MysqlDatabasePool {
  return typeof (client as MysqlDatabasePool).getConnection === 'function'
}

function isRowsResult(result: unknown): result is MysqlQueryRows {
  return Array.isArray(result) && (result.length === 0 || !Array.isArray(result[0]))
}

function normalizeRows(rows: MysqlQueryRows): Record<string, unknown>[] {
  return rows.map((row) => ({ ...row }))
}

function normalizeHeader(result: unknown): MysqlQueryResultHeader {
  if (typeof result === 'object' && result !== null) {
    let header = result as { affectedRows?: unknown; insertId?: unknown }

    return {
      affectedRows: typeof header.affectedRows === 'number' ? header.affectedRows : 0,
      insertId: header.insertId,
    }
  }

  return {
    affectedRows: 0,
    insertId: undefined,
  }
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

function normalizeInsertId(
  kind: DataManipulationRequest['operation']['kind'],
  operation: DataManipulationRequest['operation'],
  header: MysqlQueryResultHeader,
): unknown {
  if (!isInsertStatementKind(kind) || !isInsertStatement(operation)) {
    return undefined
  }

  if (getTablePrimaryKey(operation.table).length !== 1) {
    return undefined
  }

  return header.insertId
}

function quoteIdentifier(value: string): string {
  return '`' + value.replace(/`/g, '``') + '`'
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
