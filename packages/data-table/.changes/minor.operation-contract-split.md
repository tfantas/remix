BREAKING CHANGE: Replace the existing adapter contract naming/shape with operation-based contracts
(`AdapterStatement` -> `DataManipulationOperation`, `statement` -> `operation`).

Add separate adapter execution methods for DML and migration/DDL operations:
`execute` (DML operations) and `migrate` (migration/DDL operations).

Add adapter introspection methods with optional transaction context:
`hasTable(table, transaction?)` and `hasColumn(table, column, transaction?)`.
