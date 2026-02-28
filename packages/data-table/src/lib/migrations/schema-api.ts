import type { Database } from '../database.ts'
import type {
  AlterTableChange,
  CheckConstraint,
  ColumnDefinition,
  DataMigrationOperation,
  ForeignKeyAction,
  ForeignKeyConstraint,
  IndexDefinition,
  PrimaryKeyConstraint,
  TableRef,
  UniqueConstraint,
} from '../adapter.ts'
import { rawSql } from '../sql.ts'
import type { AlterTableBuilder, CreateTableBuilder, MigrationSchemaApi } from '../migrations.ts'

import { ColumnBuilder } from './column-builder.ts'
import { normalizeIndexColumns, toTableRef } from './helpers.ts'

function asColumnDefinition(definition: ColumnDefinition | ColumnBuilder): ColumnDefinition {
  if (definition instanceof ColumnBuilder) {
    return definition.build()
  }

  return definition
}

class CreateTableBuilderRuntime implements CreateTableBuilder {
  columns: Record<string, ColumnDefinition> = {}
  primaryKey: PrimaryKeyConstraint | undefined
  uniques: UniqueConstraint[] = []
  checks: CheckConstraint[] = []
  foreignKeys: ForeignKeyConstraint[] = []
  indexes: Omit<IndexDefinition, 'table'>[] = []
  tableComment: string | undefined

  addColumn(name: string, definition: ColumnDefinition | ColumnBuilder): void {
    this.columns[name] = asColumnDefinition(definition)
  }

  addPrimaryKey(columns: string[], options?: { name?: string }): void {
    this.primaryKey = { columns: [...columns], name: options?.name }
  }

  addUnique(columns: string[], options?: { name?: string }): void {
    this.uniques.push({ columns: [...columns], name: options?.name })
  }

  addForeignKey(
    columns: string[],
    refTable: string,
    refColumns?: string[],
    options?: { name?: string; onDelete?: ForeignKeyAction; onUpdate?: ForeignKeyAction },
  ): void {
    this.foreignKeys.push({
      columns: [...columns],
      references: {
        table: toTableRef(refTable),
        columns: refColumns ? [...refColumns] : ['id'],
      },
      name: options?.name,
      onDelete: options?.onDelete,
      onUpdate: options?.onUpdate,
    })
  }

  addCheck(name: string, expression: string): void {
    this.checks.push({ expression, name })
  }

  addIndex(
    name: string,
    columns: string | string[],
    options?: Omit<IndexDefinition, 'table' | 'name' | 'columns'>,
  ): void {
    this.indexes.push({
      name,
      columns: normalizeIndexColumns(columns),
      ...options,
    })
  }

  comment(text: string): void {
    this.tableComment = text
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

  addPrimaryKey(columns: string[], options?: { name?: string }): void {
    this.alterChanges.push({
      kind: 'addPrimaryKey',
      constraint: { columns: [...columns], name: options?.name },
    })
  }

  dropPrimaryKey(name?: string): void {
    this.alterChanges.push({ kind: 'dropPrimaryKey', name })
  }

  addUnique(columns: string[], options?: { name?: string }): void {
    this.alterChanges.push({
      kind: 'addUnique',
      constraint: { columns: [...columns], name: options?.name },
    })
  }

  dropUnique(name: string): void {
    this.alterChanges.push({ kind: 'dropUnique', name })
  }

  addForeignKey(
    columns: string[],
    refTable: string,
    refColumns?: string[],
    options?: { name?: string; onDelete?: ForeignKeyAction; onUpdate?: ForeignKeyAction },
  ): void {
    this.alterChanges.push({
      kind: 'addForeignKey',
      constraint: {
        columns: [...columns],
        references: {
          table: toTableRef(refTable),
          columns: refColumns ? [...refColumns] : ['id'],
        },
        name: options?.name,
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
): MigrationSchemaApi {
  return {
    async createTable(name, migrate, options) {
      let builder = new CreateTableBuilderRuntime()
      migrate(builder)

      await emit({
        kind: 'createTable',
        table: toTableRef(name),
        ifNotExists: options?.ifNotExists,
        columns: builder.columns,
        primaryKey: builder.primaryKey,
        uniques: builder.uniques,
        checks: builder.checks,
        foreignKeys: builder.foreignKeys,
        comment: builder.tableComment,
      })

      for (let index of builder.indexes) {
        await emit({
          kind: 'createIndex',
          index: {
            table: toTableRef(name),
            ...index,
          },
        })
      }
    },
    async alterTable(name, migrate, options) {
      let table = toTableRef(name)
      let builder = new AlterTableBuilderRuntime(table)
      migrate(builder)

      if (builder.alterChanges.length > 0) {
        await emit({
          kind: 'alterTable',
          table,
          changes: builder.alterChanges,
          ifExists: options?.ifExists,
        })
      }

      for (let operation of builder.extraStatements) {
        await emit(operation)
      }
    },
    async renameTable(from, to) {
      await emit({ kind: 'renameTable', from: toTableRef(from), to: toTableRef(to) })
    },
    async dropTable(name, options) {
      await emit({
        kind: 'dropTable',
        table: toTableRef(name),
        ifExists: options?.ifExists,
        cascade: options?.cascade,
      })
    },
    async createIndex(table, columns, options) {
      await emit({
        kind: 'createIndex',
        index: {
          table: toTableRef(table),
          columns: normalizeIndexColumns(columns),
          ...options,
        },
      })
    },
    async dropIndex(table, name, options) {
      await emit({
        kind: 'dropIndex',
        table: toTableRef(table),
        name,
        ifExists: options?.ifExists,
      })
    },
    async renameIndex(table, from, to) {
      await emit({
        kind: 'renameIndex',
        table: toTableRef(table),
        from,
        to,
      })
    },
    async addForeignKey(table, columns, refTable, refColumns, options) {
      await emit({
        kind: 'addForeignKey',
        table: toTableRef(table),
        constraint: {
          columns: [...columns],
          references: {
            table: toTableRef(refTable),
            columns: refColumns ? [...refColumns] : ['id'],
          },
          name: options?.name,
          onDelete: options?.onDelete,
          onUpdate: options?.onUpdate,
        },
      })
    },
    async dropForeignKey(table, name) {
      await emit({
        kind: 'dropForeignKey',
        table: toTableRef(table),
        name,
      })
    },
    async addCheck(table, name, expression) {
      await emit({
        kind: 'addCheck',
        table: toTableRef(table),
        constraint: {
          expression,
          name,
        },
      })
    },
    async dropCheck(table, name) {
      await emit({
        kind: 'dropCheck',
        table: toTableRef(table),
        name,
      })
    },
    async raw(sql) {
      await emit({
        kind: 'raw',
        sql: rawSql(sql),
      })
    },
    async tableExists(name) {
      try {
        await db.exec(rawSql('select 1 from ' + name + ' limit 1'))
        return true
      } catch {
        return false
      }
    },
    async columnExists(table, columnName) {
      try {
        await db.exec(rawSql('select ' + columnName + ' from ' + table + ' where 1 = 0'))
        return true
      } catch {
        return false
      }
    },
  }
}
