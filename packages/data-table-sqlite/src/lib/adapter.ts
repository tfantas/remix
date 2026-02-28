import type {
  AdapterCapabilityOverrides,
  AdapterExecuteRequest,
  AdapterMigrateRequest,
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
import type { Database as BetterSqliteDatabase, RunResult } from 'better-sqlite3'

import { compileSqliteStatement } from './sql-compiler.ts'

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

  compileSql(operation: DataManipulationStatement | DataDefinitionStatement): SqlStatement[] {
    if (isDataManipulationOperation(operation)) {
      let compiled = compileSqliteStatement(operation)
      return [{ text: compiled.text, values: compiled.values }]
    }

    return compileSqliteDefinitionStatements(operation)
  }

  async execute(request: AdapterExecuteRequest): Promise<DataManipulationResult> {
    if (request.operation.kind === 'insertMany' && request.operation.values.length === 0) {
      return {
        affectedRows: 0,
        insertId: undefined,
        rows: request.operation.returning ? [] : undefined,
      }
    }

    let statement = this.compileSql(request.operation)[0]
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

  async migrate(request: AdapterMigrateRequest): Promise<DataDefinitionResult> {
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
  kind: AdapterExecuteRequest['operation']['kind'],
  rows: Record<string, unknown>[],
): number | undefined {
  if (isWriteStatementKind(kind)) {
    return rows.length
  }

  return undefined
}

function normalizeInsertIdForReader(
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

function normalizeAffectedRowsForRun(
  kind: AdapterExecuteRequest['operation']['kind'],
  result: RunResult,
): number | undefined {
  if (kind === 'select' || kind === 'count' || kind === 'exists') {
    return undefined
  }

  return result.changes
}

function normalizeInsertIdForRun(
  kind: AdapterExecuteRequest['operation']['kind'],
  statement: AdapterExecuteRequest['operation'],
  result: RunResult,
): unknown {
  if (!isInsertStatementKind(kind) || !isInsertStatement(statement)) {
    return undefined
  }

  if (getTablePrimaryKey(statement.table).length !== 1) {
    return undefined
  }

  return result.lastInsertRowid
}

function quoteIdentifier(value: string): string {
  return '"' + value.replace(/"/g, '""') + '"'
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
    return value ? '1' : '0'
  }

  if (value instanceof Date) {
    return quoteLiteral(value.toISOString())
  }

  return '\'' + String(value).replace(/'/g, "''") + '\''
}

function isWriteStatementKind(kind: AdapterExecuteRequest['operation']['kind']): boolean {
  return (
    kind === 'insert' ||
    kind === 'insertMany' ||
    kind === 'update' ||
    kind === 'delete' ||
    kind === 'upsert'
  )
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

function compileSqliteDefinitionStatements(statement: DataDefinitionStatement): SqlStatement[] {
  if (statement.kind === 'raw') {
    return [{ text: statement.sql.text, values: [...statement.sql.values] }]
  }

  if (statement.kind === 'createTable') {
    let columns = Object.keys(statement.columns).map(
      (columnName) => quoteIdentifier(columnName) + ' ' + compileSqliteColumn(statement.columns[columnName]),
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

    return [
      {
        text:
          'create table ' +
          (statement.ifNotExists ? 'if not exists ' : '') +
          quoteTableRef(statement.table) +
          ' (' +
          [...columns, ...constraints].join(', ') +
          ')',
        values: [],
      },
    ]
  }

  if (statement.kind === 'alterTable') {
    let statements: SqlStatement[] = []

    for (let change of statement.changes) {
      let sql = 'alter table ' + quoteTableRef(statement.table) + ' '

      if (change.kind === 'addColumn') {
        sql +=
          'add column ' + quoteIdentifier(change.column) + ' ' + compileSqliteColumn(change.definition)
      } else if (change.kind === 'changeColumn') {
        sql +=
          'alter column ' +
          quoteIdentifier(change.column) +
          ' type ' +
          compileSqliteColumnType(change.definition)
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
        sql += 'drop constraint ' + quoteIdentifier(change.name)
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
        sql += 'drop constraint ' + quoteIdentifier(change.name)
      } else if (change.kind === 'addCheck') {
        sql +=
          'add ' +
          (change.constraint.name ? 'constraint ' + quoteIdentifier(change.constraint.name) + ' ' : '') +
          'check (' +
          change.constraint.expression +
          ')'
      } else if (change.kind === 'dropCheck') {
        sql += 'drop constraint ' + quoteIdentifier(change.name)
      } else if (change.kind === 'setTableComment') {
        continue
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
        text:
          'alter table ' + quoteTableRef(statement.from) + ' rename to ' + quoteIdentifier(statement.to.name),
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
          (statement.ifNotExists ? 'if not exists ' : '') +
          quoteIdentifier(statement.index.name ?? defaultIndexName(statement.index.columns)) +
          ' on ' +
          quoteTableRef(statement.index.table) +
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

function compileSqliteColumn(definition: ColumnDefinition): string {
  let parts = [compileSqliteColumnType(definition)]

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

function compileSqliteColumnType(definition: ColumnDefinition): string {
  if (definition.type === 'varchar') {
    return 'text'
  }

  if (definition.type === 'text') {
    return 'text'
  }

  if (definition.type === 'integer') {
    return 'integer'
  }

  if (definition.type === 'bigint') {
    return 'integer'
  }

  if (definition.type === 'decimal') {
    return 'numeric'
  }

  if (definition.type === 'boolean') {
    return 'integer'
  }

  if (definition.type === 'uuid') {
    return 'text'
  }

  if (definition.type === 'date') {
    return 'text'
  }

  if (definition.type === 'time') {
    return 'text'
  }

  if (definition.type === 'timestamp') {
    return 'text'
  }

  if (definition.type === 'json') {
    return 'text'
  }

  if (definition.type === 'binary') {
    return 'blob'
  }

  if (definition.type === 'enum') {
    return 'text'
  }

  return 'text'
}

function defaultIndexName(columns: string[]): string {
  return columns.join('_') + '_idx'
}
