USE [Northwind_BI_1404_05_DW];
GO

SET NOCOUNT ON;
GO

PRINT N'===== DATABASE =====';

SELECT
    DB_NAME() AS database_name,
    DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS collation_name,
    compatibility_level
FROM sys.databases
WHERE name = DB_NAME();
GO

PRINT N'===== TABLES AND ROW COUNTS =====';

SELECT
    s.name AS schema_name,
    t.name AS table_name,
    SUM
    (
        CASE
            WHEN p.index_id IN (0, 1) THEN p.rows
            ELSE 0
        END
    ) AS row_count
FROM sys.tables AS t
INNER JOIN sys.schemas AS s
    ON s.schema_id = t.schema_id
LEFT JOIN sys.partitions AS p
    ON p.object_id = t.object_id
GROUP BY
    s.name,
    t.name
ORDER BY
    s.name,
    t.name;
GO

PRINT N'===== COLUMNS =====';

SELECT
    s.name AS schema_name,
    t.name AS table_name,
    c.column_id,
    c.name AS column_name,
    ty.name AS data_type,
    CASE
        WHEN ty.name IN (N'nvarchar', N'nchar')
            AND c.max_length > 0
            THEN c.max_length / 2
        ELSE c.max_length
    END AS max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    c.is_identity
FROM sys.tables AS t
INNER JOIN sys.schemas AS s
    ON s.schema_id = t.schema_id
INNER JOIN sys.columns AS c
    ON c.object_id = t.object_id
INNER JOIN sys.types AS ty
    ON ty.user_type_id = c.user_type_id
ORDER BY
    s.name,
    t.name,
    c.column_id;
GO

PRINT N'===== PRIMARY KEYS =====';

SELECT
    s.name AS schema_name,
    t.name AS table_name,
    kc.name AS primary_key_name,
    ic.key_ordinal,
    c.name AS column_name
FROM sys.key_constraints AS kc
INNER JOIN sys.tables AS t
    ON t.object_id = kc.parent_object_id
INNER JOIN sys.schemas AS s
    ON s.schema_id = t.schema_id
INNER JOIN sys.index_columns AS ic
    ON ic.object_id = t.object_id
    AND ic.index_id = kc.unique_index_id
INNER JOIN sys.columns AS c
    ON c.object_id = t.object_id
    AND c.column_id = ic.column_id
WHERE kc.type = N'PK'
ORDER BY
    s.name,
    t.name,
    ic.key_ordinal;
GO

PRINT N'===== FOREIGN KEYS =====';

SELECT
    fk.name AS foreign_key_name,
    parent_schema.name AS parent_schema,
    parent_table.name AS parent_table,
    parent_column.name AS parent_column,
    referenced_schema.name AS referenced_schema,
    referenced_table.name AS referenced_table,
    referenced_column.name AS referenced_column
FROM sys.foreign_keys AS fk
INNER JOIN sys.foreign_key_columns AS fkc
    ON fkc.constraint_object_id = fk.object_id
INNER JOIN sys.tables AS parent_table
    ON parent_table.object_id = fk.parent_object_id
INNER JOIN sys.schemas AS parent_schema
    ON parent_schema.schema_id = parent_table.schema_id
INNER JOIN sys.columns AS parent_column
    ON parent_column.object_id = parent_table.object_id
    AND parent_column.column_id = fkc.parent_column_id
INNER JOIN sys.tables AS referenced_table
    ON referenced_table.object_id = fk.referenced_object_id
INNER JOIN sys.schemas AS referenced_schema
    ON referenced_schema.schema_id = referenced_table.schema_id
INNER JOIN sys.columns AS referenced_column
    ON referenced_column.object_id = referenced_table.object_id
    AND referenced_column.column_id = fkc.referenced_column_id
ORDER BY
    parent_schema.name,
    parent_table.name,
    fk.name;
GO

PRINT N'===== INDEXES =====';

SELECT
    s.name AS schema_name,
    t.name AS table_name,
    i.name AS index_name,
    i.type_desc,
    i.is_unique,
    i.is_primary_key,
    ic.key_ordinal,
    c.name AS column_name,
    ic.is_included_column
FROM sys.indexes AS i
INNER JOIN sys.tables AS t
    ON t.object_id = i.object_id
INNER JOIN sys.schemas AS s
    ON s.schema_id = t.schema_id
INNER JOIN sys.index_columns AS ic
    ON ic.object_id = i.object_id
    AND ic.index_id = i.index_id
INNER JOIN sys.columns AS c
    ON c.object_id = ic.object_id
    AND c.column_id = ic.column_id
WHERE i.name IS NOT NULL
ORDER BY
    s.name,
    t.name,
    i.name,
    ic.key_ordinal,
    c.column_id;
GO

PRINT N'===== VIEWS =====';

SELECT
    s.name AS schema_name,
    v.name AS view_name
FROM sys.views AS v
INNER JOIN sys.schemas AS s
    ON s.schema_id = v.schema_id
ORDER BY
    s.name,
    v.name;
GO
