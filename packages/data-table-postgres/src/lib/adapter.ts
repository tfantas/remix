import type {
  AdapterCapabilityOverrides,
  AdapterMigrateRequest,
  AdapterExecuteRequest,
  DataDefinitionResult,
  DataDefinitionStatement,
  DataManipulationResult,
  DataManipulationStatement,
  DatabaseAdapter,
  ColumnDefinition,
  SqlStatement,
  TableRef,
  TransactionOptions,
  TransactionToken,
} from '@remix-run/data-table'
import { getTablePrimaryKey } from '@remix-run/data-table'

import { compilePostgresStatement } from './sql-compiler.ts'

type Pretty<value> = {
  [key in keyof value]: value[key]
} & {}

/**
 * Result shape returned by postgres client `query()` calls.
 */
export type PostgresQueryResult = {
  rows: unknown[]
  rowCount: number | null
}

/**
 * Minimal postgres client contract used by this adapter.
 */
export type PostgresDatabaseClient = {
  query(text: string, values?: unknown[]): Promise<PostgresQueryResult>
}

/**
 * Postgres transaction client with optional connection release support.
 */
export type PostgresTransactionClient = Pretty<
  PostgresDatabaseClient & {
    release?: () => void
  }
>

/**
 * Postgres pool-like client contract used by this adapter.
 */
export type PostgresDatabasePool = Pretty<
  PostgresDatabaseClient & {
    connect?: () => Promise<PostgresTransactionClient>
  }
>

/**
 * Postgres adapter configuration.
 */
export type PostgresDatabaseAdapterOptions = {
  capabilities?: AdapterCapabilityOverrides
}

type TransactionState = {
  client: PostgresTransactionClient
  releaseOnClose: boolean
}

/**
 * `DatabaseAdapter` implementation for postgres-compatible clients.
 */
export class PostgresDatabaseAdapter implements DatabaseAdapter {
  dialect = 'postgres'
  capabilities

  #client: PostgresDatabasePool
  #transactions = new Map<string, TransactionState>()
  #transactionCounter = 0

  constructor(client: PostgresDatabasePool, options?: PostgresDatabaseAdapterOptions) {
    this.#client = client
    this.capabilities = {
      returning: options?.capabilities?.returning ?? true,
      savepoints: options?.capabilities?.savepoints ?? true,
      upsert: options?.capabilities?.upsert ?? true,
      transactionalDdl: options?.capabilities?.transactionalDdl ?? true,
      migrationLock: options?.capabilities?.migrationLock ?? true,
    }
  }

  compileSql(operation: DataManipulationStatement | DataDefinitionStatement): SqlStatement[] {
    if (isDataManipulationOperation(operation)) {
      let compiled = compilePostgresStatement(operation)
      return [{ text: compiled.text, values: compiled.values }]
    }

    return compilePostgresDefinitionStatements(operation)
  }

  async execute(request: AdapterExecuteRequest): Promise<DataManipulationResult> {
    if (request.operation.kind === 'insertMany' && request.operation.values.length === 0) {
      return {
        affectedRows: 0,
        insertId: undefined,
        rows: request.operation.returning ? [] : undefined,
      }
    }

    let statement = compilePostgresStatement(request.operation)
    let client = this.#resolveClient(request.transaction)
    let result = await client.query(statement.text, statement.values)
    let rows = normalizeRows(result.rows)

    if (request.operation.kind === 'count' || request.operation.kind === 'exists') {
      rows = normalizeCountRows(rows)
    }

    return {
      rows,
      affectedRows: normalizeAffectedRows(request.operation.kind, result.rowCount, rows),
      insertId: normalizeInsertId(request.operation.kind, request.operation, rows),
    }
  }

  async migrate(request: AdapterMigrateRequest): Promise<DataDefinitionResult> {
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
    let transactionClient: PostgresTransactionClient

    if (this.#client.connect) {
      transactionClient = await this.#client.connect()
      releaseOnClose = true
    } else {
      transactionClient = this.#client
    }

    await transactionClient.query('begin')

    if (options?.isolationLevel || options?.readOnly !== undefined) {
      await transactionClient.query(buildSetTransactionStatement(options))
    }

    this.#transactionCounter += 1
    let token = { id: 'tx_' + String(this.#transactionCounter) }

    this.#transactions.set(token.id, {
      client: transactionClient,
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
      await transaction.client.query('commit')
    } finally {
      this.#transactions.delete(token.id)

      if (transaction.releaseOnClose) {
        transaction.client.release?.()
      }
    }
  }

  async rollbackTransaction(token: TransactionToken): Promise<void> {
    let transaction = this.#transactions.get(token.id)

    if (!transaction) {
      throw new Error('Unknown transaction token: ' + token.id)
    }

    try {
      await transaction.client.query('rollback')
    } finally {
      this.#transactions.delete(token.id)

      if (transaction.releaseOnClose) {
        transaction.client.release?.()
      }
    }
  }

  async createSavepoint(token: TransactionToken, name: string): Promise<void> {
    let client = this.#transactionClient(token)
    await client.query('savepoint ' + quoteIdentifier(name))
  }

  async rollbackToSavepoint(token: TransactionToken, name: string): Promise<void> {
    let client = this.#transactionClient(token)
    await client.query('rollback to savepoint ' + quoteIdentifier(name))
  }

  async releaseSavepoint(token: TransactionToken, name: string): Promise<void> {
    let client = this.#transactionClient(token)
    await client.query('release savepoint ' + quoteIdentifier(name))
  }

  async acquireMigrationLock(): Promise<void> {
    await this.#client.query('select pg_advisory_lock(hashtext($1))', ['data_table_migrations'])
  }

  async releaseMigrationLock(): Promise<void> {
    await this.#client.query('select pg_advisory_unlock(hashtext($1))', ['data_table_migrations'])
  }

  #resolveClient(token: TransactionToken | undefined): PostgresDatabaseClient {
    if (!token) {
      return this.#client
    }

    return this.#transactionClient(token)
  }

  #transactionClient(token: TransactionToken): PostgresTransactionClient {
    let transaction = this.#transactions.get(token.id)

    if (!transaction) {
      throw new Error('Unknown transaction token: ' + token.id)
    }

    return transaction.client
  }
}

/**
 * Creates a postgres `DatabaseAdapter`.
 * @param client Postgres pool or client.
 * @param options Optional adapter capability overrides.
 * @returns A configured postgres adapter.
 */
export function createPostgresDatabaseAdapter(
  client: PostgresDatabasePool,
  options?: PostgresDatabaseAdapterOptions,
): PostgresDatabaseAdapter {
  return new PostgresDatabaseAdapter(client, options)
}

function buildSetTransactionStatement(options: TransactionOptions): string {
  let parts = ['set transaction']

  if (options.isolationLevel) {
    parts.push('isolation level ' + options.isolationLevel)
  }

  if (options.readOnly !== undefined) {
    parts.push(options.readOnly ? 'read only' : 'read write')
  }

  return parts.join(' ')
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

function normalizeAffectedRows(
  kind: AdapterExecuteRequest['operation']['kind'],
  rowCount: number | null,
  rows: Record<string, unknown>[],
): number | undefined {
  if (kind === 'select' || kind === 'count' || kind === 'exists') {
    return undefined
  }

  if (rowCount !== null) {
    return rowCount
  }

  if (kind === 'raw') {
    return undefined
  }

  return rows.length
}

function normalizeInsertId(
  kind: AdapterExecuteRequest['operation']['kind'],
  statement: AdapterExecuteRequest['operation'],
  rows: Record<string, unknown>[],
): unknown {
  if (!isInsertStatementKind(kind) || !isInsertStatement(statement)) {
    return undefined
  }

  let primaryKey = getTablePrimaryKey(statement.table)

  if (primaryKey.length !== 1) {
    return undefined
  }

  let key = primaryKey[0]
  let row = rows[rows.length - 1]

  return row ? row[key] : undefined
}

function quoteIdentifier(value: string): string {
  return '"' + value.replace(/"/g, '""') + '"'
}

function isInsertStatementKind(kind: AdapterExecuteRequest['operation']['kind']): boolean {
  return kind === 'insert' || kind === 'insertMany' || kind === 'upsert'
}

function isInsertStatement(
  statement: AdapterExecuteRequest['operation'],
): statement is Extract<
  AdapterExecuteRequest['operation'],
  { kind: 'insert' | 'insertMany' | 'upsert' }
> {
  return (
    statement.kind === 'insert' || statement.kind === 'insertMany' || statement.kind === 'upsert'
  )
}

function isDataManipulationOperation(
  operation: DataManipulationStatement | DataDefinitionStatement,
): operation is DataManipulationStatement {
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

function compilePostgresDefinitionStatements(statement: DataDefinitionStatement): SqlStatement[] {
  if (statement.kind === 'raw') {
    return [{ text: statement.sql.text, values: [...statement.sql.values] }]
  }

  if (statement.kind === 'createTable') {
    let columns = Object.keys(statement.columns).map(
      (columnName) => quoteIdentifier(columnName) + ' ' + compilePostgresColumn(statement.columns[columnName]),
    )
    let tableConstraints: string[] = []

    if (statement.primaryKey) {
      tableConstraints.push(
        'primary key (' +
          statement.primaryKey.columns.map((column) => quoteIdentifier(column)).join(', ') +
          ')',
      )
    }

    for (let unique of statement.uniques ?? []) {
      tableConstraints.push(
        (unique.name ? 'constraint ' + quoteIdentifier(unique.name) + ' ' : '') +
          'unique (' +
          unique.columns.map((column) => quoteIdentifier(column)).join(', ') +
          ')',
      )
    }

    for (let check of statement.checks ?? []) {
      tableConstraints.push(
        (check.name ? 'constraint ' + quoteIdentifier(check.name) + ' ' : '') +
          'check (' +
          check.expression +
          ')',
      )
    }

    for (let foreignKey of statement.foreignKeys ?? []) {
      let clause =
        (foreignKey.name ? 'constraint ' + quoteIdentifier(foreignKey.name) + ' ' : '') +
        'foreign key (' +
        foreignKey.columns.map((column) => quoteIdentifier(column)).join(', ') +
        ') references ' +
        quoteTableRef(foreignKey.references.table) +
        ' (' +
        foreignKey.references.columns.map((column) => quoteIdentifier(column)).join(', ') +
        ')'

      if (foreignKey.onDelete) {
        clause += ' on delete ' + foreignKey.onDelete
      }

      if (foreignKey.onUpdate) {
        clause += ' on update ' + foreignKey.onUpdate
      }

      tableConstraints.push(clause)
    }

    let sql =
      'create table ' +
      (statement.ifNotExists ? 'if not exists ' : '') +
      quoteTableRef(statement.table) +
      ' (' +
      [...columns, ...tableConstraints].join(', ') +
      ')'
    let statements: SqlStatement[] = [{ text: sql, values: [] }]

    if (statement.comment) {
      statements.push({
        text: 'comment on table ' + quoteTableRef(statement.table) + ' is ' + quoteLiteral(statement.comment),
        values: [],
      })
    }

    return statements
  }

  if (statement.kind === 'alterTable') {
    let sqlStatements: SqlStatement[] = []

    for (let change of statement.changes) {
      let sql = 'alter table ' + quoteTableRef(statement.table) + ' '

      if (change.kind === 'addColumn') {
        sql +=
          'add column ' +
          quoteIdentifier(change.column) +
          ' ' +
          compilePostgresColumn(change.definition)
      } else if (change.kind === 'changeColumn') {
        let typeSql = compilePostgresColumnType(change.definition)
        sql +=
          'alter column ' +
          quoteIdentifier(change.column) +
          ' type ' +
          typeSql
      } else if (change.kind === 'renameColumn') {
        sql +=
          'rename column ' +
          quoteIdentifier(change.from) +
          ' to ' +
          quoteIdentifier(change.to)
      } else if (change.kind === 'dropColumn') {
        sql +=
          'drop column ' +
          (change.ifExists ? 'if exists ' : '') +
          quoteIdentifier(change.column)
      } else if (change.kind === 'addPrimaryKey') {
        sql +=
          'add ' +
          (change.constraint.name
            ? 'constraint ' + quoteIdentifier(change.constraint.name) + ' '
            : '') +
          'primary key (' +
          change.constraint.columns.map((column) => quoteIdentifier(column)).join(', ') +
          ')'
      } else if (change.kind === 'dropPrimaryKey') {
        sql += 'drop constraint ' + quoteIdentifier(change.name ?? 'PRIMARY')
      } else if (change.kind === 'addUnique') {
        sql +=
          'add ' +
          (change.constraint.name
            ? 'constraint ' + quoteIdentifier(change.constraint.name) + ' '
            : '') +
          'unique (' +
          change.constraint.columns.map((column) => quoteIdentifier(column)).join(', ') +
          ')'
      } else if (change.kind === 'dropUnique') {
        sql += 'drop constraint ' + quoteIdentifier(change.name)
      } else if (change.kind === 'addForeignKey') {
        sql +=
          'add ' +
          (change.constraint.name
            ? 'constraint ' + quoteIdentifier(change.constraint.name) + ' '
            : '') +
          'foreign key (' +
          change.constraint.columns.map((column) => quoteIdentifier(column)).join(', ') +
          ') references ' +
          quoteTableRef(change.constraint.references.table) +
          ' (' +
          change.constraint.references.columns.map((column) => quoteIdentifier(column)).join(', ') +
          ')'
      } else if (change.kind === 'dropForeignKey') {
        sql += 'drop constraint ' + quoteIdentifier(change.name)
      } else if (change.kind === 'addCheck') {
        sql +=
          'add ' +
          (change.constraint.name
            ? 'constraint ' + quoteIdentifier(change.constraint.name) + ' '
            : '') +
          'check (' +
          change.constraint.expression +
          ')'
      } else if (change.kind === 'dropCheck') {
        sql += 'drop constraint ' + quoteIdentifier(change.name)
      } else if (change.kind === 'setTableComment') {
        sqlStatements.push({
          text:
            'comment on table ' +
            quoteTableRef(statement.table) +
            ' is ' +
            quoteLiteral(change.comment),
          values: [],
        })
        continue
      } else {
        continue
      }

      sqlStatements.push({ text: sql, values: [] })
    }

    return sqlStatements
  }

  if (statement.kind === 'renameTable') {
    return [
      {
        text:
          'alter table ' +
          quoteTableRef(statement.from) +
          ' rename to ' +
          quoteIdentifier(statement.to.name),
        values: [],
      },
    ]
  }

  if (statement.kind === 'dropTable') {
    return [
      {
        text:
          'drop table ' +
          (statement.ifExists ? 'if exists ' : '') +
          quoteTableRef(statement.table) +
          (statement.cascade ? ' cascade' : ''),
        values: [],
      },
    ]
  }

  if (statement.kind === 'createIndex') {
    return [
      {
        text:
          'create ' +
          (statement.index.unique ? 'unique ' : '') +
          'index ' +
          (statement.ifNotExists ? 'if not exists ' : '') +
          quoteIdentifier(statement.index.name ?? defaultIndexName(statement.index.columns)) +
          ' on ' +
          quoteTableRef(statement.index.table) +
          (statement.index.using ? ' using ' + statement.index.using : '') +
          ' (' +
          statement.index.columns.map((column) => quoteIdentifier(column)).join(', ') +
          ')' +
          (statement.index.where ? ' where ' + statement.index.where : ''),
        values: [],
      },
    ]
  }

  if (statement.kind === 'dropIndex') {
    return [
      {
        text: 'drop index ' + (statement.ifExists ? 'if exists ' : '') + quoteIdentifier(statement.name),
        values: [],
      },
    ]
  }

  if (statement.kind === 'renameIndex') {
    return [
      {
        text:
          'alter index ' +
          quoteIdentifier(statement.from) +
          ' rename to ' +
          quoteIdentifier(statement.to),
        values: [],
      },
    ]
  }

  if (statement.kind === 'addForeignKey') {
    return [
      {
        text:
          'alter table ' +
          quoteTableRef(statement.table) +
          ' add ' +
          (statement.constraint.name
            ? 'constraint ' + quoteIdentifier(statement.constraint.name) + ' '
            : '') +
          'foreign key (' +
          statement.constraint.columns.map((column) => quoteIdentifier(column)).join(', ') +
          ') references ' +
          quoteTableRef(statement.constraint.references.table) +
          ' (' +
          statement.constraint.references.columns.map((column) => quoteIdentifier(column)).join(', ') +
          ')' +
          (statement.constraint.onDelete ? ' on delete ' + statement.constraint.onDelete : '') +
          (statement.constraint.onUpdate ? ' on update ' + statement.constraint.onUpdate : ''),
        values: [],
      },
    ]
  }

  if (statement.kind === 'dropForeignKey') {
    return [
      {
        text:
          'alter table ' +
          quoteTableRef(statement.table) +
          ' drop constraint ' +
          quoteIdentifier(statement.name),
        values: [],
      },
    ]
  }

  if (statement.kind === 'addCheck') {
    return [
      {
        text:
          'alter table ' +
          quoteTableRef(statement.table) +
          ' add ' +
          (statement.constraint.name
            ? 'constraint ' + quoteIdentifier(statement.constraint.name) + ' '
            : '') +
          'check (' +
          statement.constraint.expression +
          ')',
        values: [],
      },
    ]
  }

  if (statement.kind === 'dropCheck') {
    return [
      {
        text:
          'alter table ' +
          quoteTableRef(statement.table) +
          ' drop constraint ' +
          quoteIdentifier(statement.name),
        values: [],
      },
    ]
  }

  throw new Error('Unsupported data definition statement kind')
}

function compilePostgresColumn(definition: ColumnDefinition): string {
  let parts = [compilePostgresColumnType(definition)]

  if (definition.nullable === false) {
    parts.push('not null')
  }

  if (definition.default) {
    if (definition.default.kind === 'now') {
      parts.push('default now()')
    } else if (definition.default.kind === 'sql') {
      parts.push('default ' + definition.default.expression)
    } else {
      parts.push('default ' + quoteLiteral(definition.default.value))
    }
  }

  if (definition.primaryKey) {
    parts.push('primary key')
  }

  if (definition.unique) {
    parts.push('unique')
  }

  if (definition.computed) {
    if (!definition.computed.stored) {
      throw new Error('Postgres only supports stored computed/generated columns')
    }

    parts.push('generated always as (' + definition.computed.expression + ') stored')
  }

  if (definition.references) {
    let clause =
      'references ' +
      quoteTableRef(definition.references.table) +
      ' (' +
      definition.references.columns.map((column) => quoteIdentifier(column)).join(', ') +
      ')'

    if (definition.references.onDelete) {
      clause += ' on delete ' + definition.references.onDelete
    }

    if (definition.references.onUpdate) {
      clause += ' on update ' + definition.references.onUpdate
    }

    parts.push(clause)
  }

  if (definition.checks && definition.checks.length > 0) {
    for (let check of definition.checks) {
      parts.push('check (' + check.expression + ')')
    }
  }

  return parts.join(' ')
}

function compilePostgresColumnType(definition: ColumnDefinition): string {
  if (definition.type === 'varchar') {
    return 'varchar(' + String(definition.length ?? 255) + ')'
  }

  if (definition.type === 'text') {
    return 'text'
  }

  if (definition.type === 'integer') {
    return 'integer'
  }

  if (definition.type === 'bigint') {
    return 'bigint'
  }

  if (definition.type === 'decimal') {
    if (definition.precision !== undefined && definition.scale !== undefined) {
      return 'decimal(' + String(definition.precision) + ', ' + String(definition.scale) + ')'
    }

    return 'decimal'
  }

  if (definition.type === 'boolean') {
    return 'boolean'
  }

  if (definition.type === 'uuid') {
    return 'uuid'
  }

  if (definition.type === 'date') {
    return 'date'
  }

  if (definition.type === 'time') {
    return definition.withTimezone ? 'time with time zone' : 'time without time zone'
  }

  if (definition.type === 'timestamp') {
    return definition.withTimezone ? 'timestamp with time zone' : 'timestamp without time zone'
  }

  if (definition.type === 'json') {
    return 'jsonb'
  }

  if (definition.type === 'binary') {
    return 'bytea'
  }

  if (definition.type === 'enum') {
    return 'text'
  }

  return 'text'
}

function quoteTableRef(table: TableRef): string {
  if (table.schema) {
    return quoteIdentifier(table.schema) + '.' + quoteIdentifier(table.name)
  }

  return quoteIdentifier(table.name)
}

function quoteLiteral(value: unknown): string {
  if (value === null) {
    return 'null'
  }

  if (typeof value === 'number' || typeof value === 'bigint') {
    return String(value)
  }

  if (typeof value === 'boolean') {
    return value ? 'true' : 'false'
  }

  if (value instanceof Date) {
    return quoteLiteral(value.toISOString())
  }

  return "'" + String(value).replace(/'/g, "''") + "'"
}

function defaultIndexName(columns: string[]): string {
  return columns.join('_') + '_idx'
}
