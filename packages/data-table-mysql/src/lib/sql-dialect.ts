import type { ColumnDefinition } from '@remix-run/data-table'
import type { SqlCompilerDialect } from '@remix-run/data-table/internal/sql-compiler'

export let mysqlSqlDialect: SqlCompilerDialect = {
  name: 'mysql',
  quoteIdentifier(value) {
    return '`' + value.replace(/`/g, '``') + '`'
  },
  quoteLiteral(value) {
    return quoteSqlLiteral(value, false)
  },
  placeholder() {
    return '?'
  },
  normalizeBoundValue(value) {
    return value
  },
  compileColumnType(definition, tools) {
    return compileMysqlColumnType(definition, tools)
  },
  defaultInsertValuesClause: ' () values ()',
  nowExpression: 'current_timestamp',
  supportsReturning: false,
  supportsIlikeOperator: false,
  rewriteRawQuestionPlaceholders: false,
  upsertKind: 'onDuplicateKeyUpdate',
  migration: {
    createTableCommentStyle: 'alterTableComment',
    setTableCommentStyle: 'alterTableComment',
    changeColumnStyle: 'modifyColumn',
    dropColumnSupportsIfExists: false,
    addPrimaryKeyConstraintName: false,
    dropPrimaryKeyStyle: 'dropPrimaryKey',
    dropUniqueStyle: 'dropIndex',
    dropForeignKeyStyle: 'dropForeignKey',
    dropCheckStyle: 'dropCheck',
    renameTableStyle: 'renameTable',
    dropTableSupportsCascade: false,
    createIndexSupportsIfNotExists: false,
    createIndexSupportsUsing: true,
    dropIndexStyle: 'dropIndexOnTable',
    renameIndexStyle: 'alterTableRenameIndex',
    computedColumnStyle: 'storedOrVirtual',
    virtualComputedKeyword: 'virtual',
    computedStoredOnlyError: 'MySQL supports virtual and stored computed/generated columns',
  },
}

function compileMysqlColumnType(
  definition: ColumnDefinition,
  tools: { quoteLiteral: (value: unknown) => string },
): string {
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
      return 'enum(' + definition.enumValues.map((value) => tools.quoteLiteral(value)).join(', ') + ')'
    }

    return 'text'
  }

  return 'text'
}

function quoteSqlLiteral(value: unknown, booleanAsInteger: boolean): string {
  if (value === null) {
    return 'null'
  }

  if (typeof value === 'number' || typeof value === 'bigint') {
    return String(value)
  }

  if (typeof value === 'boolean') {
    if (booleanAsInteger) {
      return value ? '1' : '0'
    }

    return value ? 'true' : 'false'
  }

  if (value instanceof Date) {
    return quoteSqlLiteral(value.toISOString(), booleanAsInteger)
  }

  return "'" + String(value).replace(/'/g, "''") + "'"
}
