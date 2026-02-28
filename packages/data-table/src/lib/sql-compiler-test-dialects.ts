import type { ColumnDefinition, DataMigrationOperation } from './adapter.ts'
import type { SqlCompilerDialect, SqlCompilerOptions } from './sql-compiler.ts'

export let postgresDialect: SqlCompilerDialect = {
  name: 'postgres',
  quoteIdentifier(value) {
    return '"' + value.replace(/"/g, '""') + '"'
  },
  quoteLiteral(value) {
    return quoteSqlLiteral(value, false)
  },
  placeholder(index) {
    return '$' + String(index)
  },
  normalizeBoundValue(value) {
    return value
  },
  compileColumnType(definition) {
    return compilePostgresColumnType(definition)
  },
  defaultInsertValuesClause: ' default values',
  nowExpression: 'now()',
  supportsReturning: true,
  supportsIlikeOperator: true,
  rewriteRawQuestionPlaceholders: true,
  upsertKind: 'onConflict',
  migration: {
    createTableCommentStyle: 'commentOnTable',
    setTableCommentStyle: 'commentOnTable',
    changeColumnStyle: 'alterType',
    dropColumnSupportsIfExists: true,
    addPrimaryKeyConstraintName: true,
    dropPrimaryKeyStyle: 'dropConstraint',
    dropUniqueStyle: 'dropConstraint',
    dropForeignKeyStyle: 'dropConstraint',
    dropCheckStyle: 'dropConstraint',
    renameTableStyle: 'alterTableRename',
    dropTableSupportsCascade: true,
    createIndexSupportsIfNotExists: true,
    createIndexSupportsUsing: true,
    dropIndexStyle: 'dropIndex',
    renameIndexStyle: 'alterIndexRename',
    computedColumnStyle: 'storedOnly',
    virtualComputedKeyword: 'virtual',
    computedStoredOnlyError: 'Postgres only supports stored computed/generated columns',
  },
}

export let mysqlDialect: SqlCompilerDialect = {
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

export let sqliteDialect: SqlCompilerDialect = {
  name: 'sqlite',
  quoteIdentifier(value) {
    return '"' + value.replace(/"/g, '""') + '"'
  },
  quoteLiteral(value) {
    return quoteSqlLiteral(value, true)
  },
  placeholder() {
    return '?'
  },
  normalizeBoundValue(value) {
    if (typeof value === 'boolean') {
      return value ? 1 : 0
    }

    return value
  },
  compileColumnType(definition) {
    return compileSqliteColumnType(definition)
  },
  defaultInsertValuesClause: ' default values',
  nowExpression: 'current_timestamp',
  supportsReturning: true,
  supportsIlikeOperator: false,
  rewriteRawQuestionPlaceholders: false,
  upsertKind: 'onConflict',
  migration: {
    createTableCommentStyle: 'none',
    setTableCommentStyle: 'none',
    changeColumnStyle: 'alterType',
    dropColumnSupportsIfExists: false,
    addPrimaryKeyConstraintName: false,
    dropPrimaryKeyStyle: 'dropPrimaryKey',
    dropUniqueStyle: 'dropConstraint',
    dropForeignKeyStyle: 'dropConstraint',
    dropCheckStyle: 'dropConstraint',
    renameTableStyle: 'alterTableRename',
    dropTableSupportsCascade: false,
    createIndexSupportsIfNotExists: true,
    createIndexSupportsUsing: false,
    dropIndexStyle: 'dropIndex',
    renameIndexStyle: 'alterTableRenameIndex',
    computedColumnStyle: 'storedOrVirtual',
    virtualComputedKeyword: 'virtual',
    computedStoredOnlyError: 'SQLite supports virtual and stored computed/generated columns',
  },
}

export let postgresCompilerOptions: SqlCompilerOptions = {
  dialect: postgresDialect,
}

export let mysqlCompilerOptions: SqlCompilerOptions = {
  dialect: mysqlDialect,
}

export let sqliteCompilerOptions: SqlCompilerOptions = {
  dialect: sqliteDialect,
}

export function sqliteCompilerOptionsWithRewrite(
  rewriteMigrationOperation: (operation: DataMigrationOperation) => DataMigrationOperation[],
): SqlCompilerOptions {
  return {
    dialect: sqliteDialect,
    rewriteMigrationOperation,
  }
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
    return definition.withTimezone
      ? 'timestamp with time zone'
      : 'timestamp without time zone'
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
