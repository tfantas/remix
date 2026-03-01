import type { Database } from '../database.ts'
import type {
  AlterTableChange,
  CheckConstraint,
  ColumnDefinition,
  CreateTableOperation,
  DataMigrationOperation,
  ForeignKeyAction,
  ForeignKeyConstraint,
  IndexDefinition,
  PrimaryKeyConstraint,
  TableRef,
  UniqueConstraint,
} from '../adapter.ts'
import { rawSql } from '../sql.ts'
import {
  getTableColumnDefinitions,
  getTableName,
  getTablePrimaryKey,
} from '../table.ts'
import type { AnyTable } from '../table.ts'
import type { AlterTableBuilder, MigrationOperations, TableInput } from '../migrations.ts'

import { ColumnBuilder } from '../column.ts'
import { normalizeIndexColumns, toTableRef } from './helpers.ts'

function asColumnDefinition(definition: ColumnDefinition | ColumnBuilder): ColumnDefinition {
  if (definition instanceof ColumnBuilder) {
    return definition.build()
  }

  return definition
}

function asTableName(value: TableInput): string {
  if (typeof value === 'string') {
    return value
  }

  return getTableName(value)
}

function asTableRef(value: TableInput): TableRef {
  return toTableRef(asTableName(value))
}

function normalizeTableIdentifier(value: string): string {
  let normalized = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')

  if (normalized.length === 0) {
    return 'table'
  }

  return normalized
}

function createPrimaryKeyConstraintName(table: TableRef): string {
  let qualifiedName = table.schema ? table.schema + '_' + table.name : table.name
  return normalizeTableIdentifier(qualifiedName) + '_pk'
}

function lowerTableForCreate(table: AnyTable): CreateTableOperation {
  let tableRef = toTableRef(getTableName(table))
  let sourceColumnDefinitions = getTableColumnDefinitions(table)
  let columns: Record<string, ColumnDefinition> = {}
  let uniques: UniqueConstraint[] = []
  let checks: CheckConstraint[] = []
  let foreignKeys: ForeignKeyConstraint[] = []

  for (let columnName in sourceColumnDefinitions) {
    if (!Object.prototype.hasOwnProperty.call(sourceColumnDefinitions, columnName)) {
      continue
    }

    let sourceDefinition = sourceColumnDefinitions[columnName]
    let columnDefinition: ColumnDefinition = {
      ...sourceDefinition,
      checks: undefined,
      references: undefined,
      primaryKey: undefined,
    }

    let unique = sourceDefinition.unique

    if (unique && typeof unique === 'object' && unique.name) {
      uniques.push({
        name: unique.name,
        columns: [columnName],
      })
      columnDefinition.unique = undefined
    }

    if (sourceDefinition.checks) {
      for (let check of sourceDefinition.checks) {
        checks.push({
          name: check.name,
          expression: check.expression,
        })
      }
    }

    if (sourceDefinition.references) {
      foreignKeys.push({
        name: sourceDefinition.references.name,
        columns: [columnName],
        references: {
          table: { ...sourceDefinition.references.table },
          columns: [...sourceDefinition.references.columns],
        },
        onDelete: sourceDefinition.references.onDelete,
        onUpdate: sourceDefinition.references.onUpdate,
      })
    }

    columns[columnName] = columnDefinition
  }

  let primaryKeyColumns = [...getTablePrimaryKey(table)]
  let primaryKey: PrimaryKeyConstraint | undefined

  if (primaryKeyColumns.length > 0) {
    primaryKey = {
      columns: primaryKeyColumns,
      name: createPrimaryKeyConstraintName(tableRef),
    }
  }

  return {
    kind: 'createTable',
    table: tableRef,
    columns,
    primaryKey,
    uniques: uniques.length > 0 ? uniques : undefined,
    checks: checks.length > 0 ? checks : undefined,
    foreignKeys: foreignKeys.length > 0 ? foreignKeys : undefined,
  }
}

class AlterTableBuilderRuntime implements AlterTableBuilder {
  alterChanges: AlterTableChange[] = []
  extraStatements: DataMigrationOperation[] = []
  table: TableRef

  constructor(table: TableRef) {
    this.table = table
  }

  addColumn(name: string, definition: ColumnDefinition | ColumnBuilder): void {
    this.alterChanges.push({ kind: 'addColumn', column: name, definition: asColumnDefinition(definition) })
  }

  changeColumn(name: string, definition: ColumnDefinition | ColumnBuilder): void {
    this.alterChanges.push({
      kind: 'changeColumn',
      column: name,
      definition: asColumnDefinition(definition),
    })
  }

  renameColumn(from: string, to: string): void {
    this.alterChanges.push({ kind: 'renameColumn', from, to })
  }

  dropColumn(name: string, options?: { ifExists?: boolean }): void {
    this.alterChanges.push({ kind: 'dropColumn', column: name, ifExists: options?.ifExists })
  }

  addPrimaryKey(name: string, columns: string[]): void {
    this.alterChanges.push({
      kind: 'addPrimaryKey',
      constraint: { columns: [...columns], name },
    })
  }

  dropPrimaryKey(name: string): void {
    this.alterChanges.push({ kind: 'dropPrimaryKey', name })
  }

  addUnique(name: string, columns: string[]): void {
    this.alterChanges.push({
      kind: 'addUnique',
      constraint: { columns: [...columns], name },
    })
  }

  dropUnique(name: string): void {
    this.alterChanges.push({ kind: 'dropUnique', name })
  }

  addForeignKey(
    name: string,
    columns: string[],
    refTable: TableInput,
    refColumns?: string[],
    options?: { onDelete?: ForeignKeyAction; onUpdate?: ForeignKeyAction },
  ): void {
    this.alterChanges.push({
      kind: 'addForeignKey',
      constraint: {
        columns: [...columns],
        references: {
          table: asTableRef(refTable),
          columns: refColumns ? [...refColumns] : ['id'],
        },
        name,
        onDelete: options?.onDelete,
        onUpdate: options?.onUpdate,
      },
    })
  }

  dropForeignKey(name: string): void {
    this.alterChanges.push({ kind: 'dropForeignKey', name })
  }

  addCheck(name: string, expression: string): void {
    this.alterChanges.push({
      kind: 'addCheck',
      constraint: { expression, name },
    })
  }

  dropCheck(name: string): void {
    this.alterChanges.push({ kind: 'dropCheck', name })
  }

  addIndex(
    name: string,
    columns: string | string[],
    options?: Omit<IndexDefinition, 'table' | 'name' | 'columns'>,
  ): void {
    this.extraStatements.push({
      kind: 'createIndex',
      index: {
        table: this.table,
        name,
        columns: normalizeIndexColumns(columns),
        ...options,
      },
    })
  }

  dropIndex(name: string): void {
    this.extraStatements.push({
      kind: 'dropIndex',
      table: this.table,
      name,
    })
  }

  comment(text: string): void {
    this.alterChanges.push({ kind: 'setTableComment', comment: text })
  }
}

export function createSchemaApi(
  db: Database,
  emit: (operation: DataMigrationOperation) => Promise<void>,
): MigrationOperations {
  return {
    async createTable(table, options) {
      let operation = lowerTableForCreate(table)
      operation.ifNotExists = options?.ifNotExists
      await emit(operation)
    },
    async alterTable(input, migrate, options) {
      let tableRef = asTableRef(input)
      let builder = new AlterTableBuilderRuntime(tableRef)
      migrate(builder)

      if (builder.alterChanges.length > 0) {
        await emit({
          kind: 'alterTable',
          table: tableRef,
          changes: builder.alterChanges,
          ifExists: options?.ifExists,
        })
      }

      for (let operation of builder.extraStatements) {
        await emit(operation)
      }
    },
    async renameTable(from, to) {
      await emit({ kind: 'renameTable', from: asTableRef(from), to: asTableRef(to) })
    },
    async dropTable(table, options) {
      await emit({
        kind: 'dropTable',
        table: asTableRef(table),
        ifExists: options?.ifExists,
        cascade: options?.cascade,
      })
    },
    async createIndex(table, name, columns, options) {
      await emit({
        kind: 'createIndex',
        index: {
          table: asTableRef(table),
          name,
          columns: normalizeIndexColumns(columns),
          ...options,
        },
      })
    },
    async dropIndex(table, name, options) {
      await emit({
        kind: 'dropIndex',
        table: asTableRef(table),
        name,
        ifExists: options?.ifExists,
      })
    },
    async renameIndex(table, from, to) {
      await emit({
        kind: 'renameIndex',
        table: asTableRef(table),
        from,
        to,
      })
    },
    async addForeignKey(table, name, columns, refTable, refColumns, options) {
      await emit({
        kind: 'addForeignKey',
        table: asTableRef(table),
        constraint: {
          columns: [...columns],
          references: {
            table: asTableRef(refTable),
            columns: refColumns ? [...refColumns] : ['id'],
          },
          name,
          onDelete: options?.onDelete,
          onUpdate: options?.onUpdate,
        },
      })
    },
    async dropForeignKey(table, name) {
      await emit({
        kind: 'dropForeignKey',
        table: asTableRef(table),
        name,
      })
    },
    async addCheck(table, name, expression) {
      await emit({
        kind: 'addCheck',
        table: asTableRef(table),
        constraint: {
          expression,
          name,
        },
      })
    },
    async dropCheck(table, name) {
      await emit({
        kind: 'dropCheck',
        table: asTableRef(table),
        name,
      })
    },
    async plan(sql) {
      let statement = typeof sql === 'string' ? rawSql(sql) : sql
      await emit({
        kind: 'raw',
        sql: statement,
      })
    },
    async hasTable(table) {
      let tableName = asTableName(table)
      try {
        await db.exec(rawSql('select 1 from ' + tableName + ' limit 1'))
        return true
      } catch {
        return false
      }
    },
    async hasColumn(table, columnName) {
      let tableName = asTableName(table)
      try {
        await db.exec(rawSql('select ' + columnName + ' from ' + tableName + ' where 1 = 0'))
        return true
      } catch {
        return false
      }
    },
  }
}
