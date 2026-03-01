BREAKING CHANGE: Update the sqlite adapter to the split DML/DDL contract (`execute` for `DataManipulationOperation`, `migrate` for `DataMigrationOperation`), rename request payload field `statement` to `operation`, align compiler/runtime naming around operations, and keep adapter-level DDL execution support for migrations.

SQL compilation remains adapter-owned while sharing common helpers from `remix/data-table/sql-helpers`.

Adapter introspection hooks now support migration transaction tokens
(`hasTable(table, transaction?)`, `hasColumn(table, column, transaction?)`) and validate supplied tokens.
