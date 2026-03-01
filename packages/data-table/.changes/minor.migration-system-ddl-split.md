BREAKING CHANGE: Rename adapter DML contracts to `DataManipulationOperation`/`operation`, add first-class `DataMigrationOperation` DDL contracts, and introduce the new migration system (`remix/data-table/migrations`) with `createMigration`, chainable `column` builders, schema/table planners, registry APIs, runner APIs, and optional Node file discovery from `remix/data-table/migrations/node`.

BREAKING CHANGE: Rename the top-level table-definition helper from `createTable(...)` to `table(...)`, move table definitions to column-builder inputs, remove per-column `.validate(...)` in favor of table-level `validate({ operation, tableName, value })`, remove `~standard` table-schema compatibility and `getTableValidationSchemas(...)`, and stop runtime predicate-value validation/coercion.

BREAKING CHANGE: Migration authoring/runtime APIs were renamed for clarity: `db.raw(...)` -> `db.plan(...)`, `db.tableExists(...)` -> `db.hasTable(...)`, `db.columnExists(...)` -> `db.hasColumn(...)`, `MigrationRunnerOptions.tableName` -> `MigrationRunnerOptions.journalTable`, and `DataMigrationResult.affectedObjects` -> `DataMigrationResult.affectedOperations`.

BREAKING CHANGE: SQL compilation remains adapter-owned and `@remix-run/data-table` exposes shared SQL compiler helpers at `remix/data-table/sql-helpers`.
