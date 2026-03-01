BREAKING CHANGE: Replace the existing adapter contract naming/shape with operation-based contracts (`AdapterStatement` -> `DataManipulationOperation`, `statement` -> `operation`) and split adapter execution into `execute` (DML operations) plus `migrate` (migration/DDL operations).

BREAKING CHANGE: Rename the top-level table-definition helper from `createTable(...)` to `table(...)`, move table definitions to column-builder inputs, remove per-column `.validate(...)` in favor of table-level `validate({ operation, tableName, value })`, remove `~standard` table-schema compatibility and `getTableValidationSchemas(...)`, and stop runtime predicate-value validation/coercion.

Add a first-class migration system under `remix/data-table/migrations` with `createMigration`, chainable `column` builders, schema/table migration planners, migration registry/runner APIs, and optional Node file discovery from `remix/data-table/migrations/node`.

SQL compilation remains adapter-owned and `@remix-run/data-table` now exposes shared SQL compiler helpers at `remix/data-table/sql-helpers`.
