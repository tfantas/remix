BREAKING CHANGE: Rename the top-level table-definition helper from `createTable(...)` to `table(...)`, move table definitions to column-builder inputs, remove per-column `.validate(...)` in favor of table-level `validate({ operation, tableName, value })`, remove `~standard` table-schema compatibility and `getTableValidationSchemas(...)`, and stop runtime predicate-value validation/coercion.

Add optional table lifecycle callbacks for write/delete/read flows: `beforeWrite`, `afterWrite`, `beforeDelete`, `afterDelete`, and `afterRead`.
