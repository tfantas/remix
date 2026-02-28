import type {
  AdapterCapabilityOverrides,
  AdapterExecuteRequest,
  AdapterMigrateRequest,
  DataDefinitionResult,
  DataDefinitionOperation,
  DataManipulationResult,
  DataManipulationOperation,
  DatabaseAdapter,
  ColumnDefinition,
  SqlStatement,
  TableRef,
  TransactionOptions,
  TransactionToken,
} from '@remix-run/data-table'
import { getTablePrimaryKey } from '@remix-run/data-table'

import { compileMysqlStatement } from './sql-compiler.ts'

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

  compileSql(operation: DataManipulationOperation | DataDefinitionOperation): SqlStatement[] {
    if (isDataManipulationOperation(operation)) {
      let compiled = compileMysqlStatement(operation)
      return [{ text: compiled.text, values: compiled.values }]
    }

    return compileMysqlDefinitionStatements(operation)
  }

  async execute(request: AdapterExecuteRequest): Promise<DataManipulationResult> {
    if (request.operation.kind === 'insertMany' && request.operation.values.length === 0) {
      return {
        affectedRows: 0,
        insertId: undefined,
        rows: request.operation.returning ? [] : undefined,
      }
    }

    let statements = this.compileSql(request.operation)
    let statement = statements[0]
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
  kind: AdapterExecuteRequest['operation']['kind'],
  statement: AdapterExecuteRequest['operation'],
  header: MysqlQueryResultHeader,
): unknown {
  if (!isInsertStatementKind(kind) || !isInsertStatement(statement)) {
    return undefined
  }

  if (getTablePrimaryKey(statement.table).length !== 1) {
    return undefined
  }

  return header.insertId
}

function quoteIdentifier(value: string): string {
  return '`' + value.replace(/`/g, '``') + '`'
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

  return '\'' + String(value).replace(/'/g, "''") + '\''
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
  operation: DataManipulationOperation | DataDefinitionOperation,
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

function compileMysqlDefinitionStatements(statement: DataDefinitionOperation): SqlStatement[] {
  if (statement.kind === 'raw') {
    return [{ text: statement.sql.text, values: [...statement.sql.values] }]
  }

  if (statement.kind === 'createTable') {
    let columns = Object.keys(statement.columns).map(
      (columnName) => quoteIdentifier(columnName) + ' ' + compileMysqlColumn(statement.columns[columnName]),
    )
    let constraints: string[] = []

    if (statement.primaryKey) {
      constraints.push(
        'primary key (' +
          statement.primaryKey.columns.map((column) => quoteIdentifier(column)).join(', ') +
          ')',
      )
    }

    for (let unique of statement.uniques ?? []) {
      constraints.push(
        (unique.name ? 'constraint ' + quoteIdentifier(unique.name) + ' ' : '') +
          'unique (' +
          unique.columns.map((column) => quoteIdentifier(column)).join(', ') +
          ')',
      )
    }

    for (let check of statement.checks ?? []) {
      constraints.push(
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

      constraints.push(clause)
    }

    let sql =
      'create table ' +
      (statement.ifNotExists ? 'if not exists ' : '') +
      quoteTableRef(statement.table) +
      ' (' +
      [...columns, ...constraints].join(', ') +
      ')'

    let statements: SqlStatement[] = [{ text: sql, values: [] }]

    if (statement.comment) {
      statements.push({
        text: 'alter table ' + quoteTableRef(statement.table) + ' comment = ' + quoteLiteral(statement.comment),
        values: [],
      })
    }

    return statements
  }

  if (statement.kind === 'alterTable') {
    let statements: SqlStatement[] = []

    for (let change of statement.changes) {
      let sql = 'alter table ' + quoteTableRef(statement.table) + ' '

      if (change.kind === 'addColumn') {
        sql += 'add column ' + quoteIdentifier(change.column) + ' ' + compileMysqlColumn(change.definition)
      } else if (change.kind === 'changeColumn') {
        sql +=
          'modify column ' + quoteIdentifier(change.column) + ' ' + compileMysqlColumn(change.definition)
      } else if (change.kind === 'renameColumn') {
        sql +=
          'rename column ' + quoteIdentifier(change.from) + ' to ' + quoteIdentifier(change.to)
      } else if (change.kind === 'dropColumn') {
        sql += 'drop column ' + quoteIdentifier(change.column)
      } else if (change.kind === 'addPrimaryKey') {
        sql +=
          'add primary key (' +
          change.constraint.columns.map((column) => quoteIdentifier(column)).join(', ') +
          ')'
      } else if (change.kind === 'dropPrimaryKey') {
        sql += 'drop primary key'
      } else if (change.kind === 'addUnique') {
        sql +=
          'add ' +
          (change.constraint.name ? 'constraint ' + quoteIdentifier(change.constraint.name) + ' ' : '') +
          'unique (' +
          change.constraint.columns.map((column) => quoteIdentifier(column)).join(', ') +
          ')'
      } else if (change.kind === 'dropUnique') {
        sql += 'drop index ' + quoteIdentifier(change.name)
      } else if (change.kind === 'addForeignKey') {
        sql +=
          'add ' +
          (change.constraint.name ? 'constraint ' + quoteIdentifier(change.constraint.name) + ' ' : '') +
          'foreign key (' +
          change.constraint.columns.map((column) => quoteIdentifier(column)).join(', ') +
          ') references ' +
          quoteTableRef(change.constraint.references.table) +
          ' (' +
          change.constraint.references.columns.map((column) => quoteIdentifier(column)).join(', ') +
          ')'
      } else if (change.kind === 'dropForeignKey') {
        sql += 'drop foreign key ' + quoteIdentifier(change.name)
      } else if (change.kind === 'addCheck') {
        sql +=
          'add ' +
          (change.constraint.name ? 'constraint ' + quoteIdentifier(change.constraint.name) + ' ' : '') +
          'check (' +
          change.constraint.expression +
          ')'
      } else if (change.kind === 'dropCheck') {
        sql += 'drop check ' + quoteIdentifier(change.name)
      } else if (change.kind === 'setTableComment') {
        sql += 'comment = ' + quoteLiteral(change.comment)
      } else {
        continue
      }

      statements.push({ text: sql, values: [] })
    }

    return statements
  }

  if (statement.kind === 'renameTable') {
    return [
      {
        text: 'rename table ' + quoteTableRef(statement.from) + ' to ' + quoteTableRef(statement.to),
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
          quoteTableRef(statement.table),
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
        text: 'drop index ' + quoteIdentifier(statement.name) + ' on ' + quoteTableRef(statement.table),
        values: [],
      },
    ]
  }

  if (statement.kind === 'renameIndex') {
    return [
      {
        text:
          'alter table ' +
          quoteTableRef(statement.table) +
          ' rename index ' +
          quoteIdentifier(statement.from) +
          ' to ' +
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
          ' drop foreign key ' +
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
          ' drop check ' +
          quoteIdentifier(statement.name),
        values: [],
      },
    ]
  }

  throw new Error('Unsupported data definition statement kind')
}

function compileMysqlColumn(definition: ColumnDefinition): string {
  let parts = [compileMysqlColumnType(definition)]

  if (definition.nullable === false) {
    parts.push('not null')
  }

  if (definition.default) {
    if (definition.default.kind === 'now') {
      parts.push('default current_timestamp')
    } else if (definition.default.kind === 'sql') {
      parts.push('default ' + definition.default.expression)
    } else {
      parts.push('default ' + quoteLiteral(definition.default.value))
    }
  }

  if (definition.autoIncrement) {
    parts.push('auto_increment')
  }

  if (definition.primaryKey) {
    parts.push('primary key')
  }

  if (definition.unique) {
    parts.push('unique')
  }

  if (definition.computed) {
    parts.push('generated always as (' + definition.computed.expression + ')')
    parts.push(definition.computed.stored ? 'stored' : 'virtual')
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

function compileMysqlColumnType(definition: ColumnDefinition): string {
  if (definition.type === 'varchar') {
    return 'varchar(' + String(definition.length ?? 255) + ')'
  }

  if (definition.type === 'text') {
    return 'text'
  }

  if (definition.type === 'integer') {
    return definition.unsigned ? 'int unsigned' : 'int'
  }

  if (definition.type === 'bigint') {
    return definition.unsigned ? 'bigint unsigned' : 'bigint'
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
    return 'char(36)'
  }

  if (definition.type === 'date') {
    return 'date'
  }

  if (definition.type === 'time') {
    return 'time'
  }

  if (definition.type === 'timestamp') {
    return 'timestamp'
  }

  if (definition.type === 'json') {
    return 'json'
  }

  if (definition.type === 'binary') {
    return 'blob'
  }

  if (definition.type === 'enum') {
    if (definition.enumValues && definition.enumValues.length > 0) {
      return 'enum(' + definition.enumValues.map((value) => quoteLiteral(value)).join(', ') + ')'
    }

    return 'text'
  }

  return 'text'
}

function defaultIndexName(columns: string[]): string {
  return columns.join('_') + '_idx'
}
