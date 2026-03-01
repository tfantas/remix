BREAKING CHANGE: Rename the top-level table-definition helper from `createTable(...)` to `table(...)`, move table definitions to column-builder inputs, add table-level `validate({ operation, tableName, value })`, remove `~standard` table-schema compatibility and `getTableValidationSchemas(...)`, and stop runtime predicate-value validation/coercion.

BREAKING CHANGE: Migration callback context now separates schema operations from data operations:
callbacks receive `{ db, schema }` where `db` is DML-only and `schema` owns migration DDL/planning methods.
Use `db.adapter.dialect` when dialect-specific branching is needed in migrations.
