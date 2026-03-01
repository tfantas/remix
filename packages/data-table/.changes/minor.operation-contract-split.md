BREAKING CHANGE: Replace the existing adapter contract naming/shape with operation-based contracts
(`AdapterStatement` -> `DataManipulationOperation`, `statement` -> `operation`) and split adapter
execution into `execute` (DML operations) plus `migrate` (migration/DDL operations).

BREAKING CHANGE: Make adapter introspection methods required and transaction-aware:
`hasTable(table, transaction?)` and `hasColumn(table, column, transaction?)`.
