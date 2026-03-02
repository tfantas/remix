BREAKING CHANGE: Update the postgres adapter to the split DML/DDL contract (`execute` for `DataManipulationOperation`, `migrate` for `DataMigrationOperation`), rename request payload field `statement` to `operation`, align compiler/runtime naming around operations, and keep adapter-level migration locking support.

SQL compilation remains adapter-owned while sharing common helpers from `remix/data-table/sql-helpers`.
