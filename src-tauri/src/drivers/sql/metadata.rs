use crate::error::{AppError, AppResult};
use crate::models::{
    ColumnMetadata, DatabaseMetadata, ForeignKeyMetadata, IndexMetadata, MetadataSnapshot,
    ObjectStat, TableMetadata,
};
use sqlx::{postgres::types::Oid, MySqlPool, PgPool, Row};
use std::collections::BTreeMap;

// information_schema columns are binary-collated / unsigned in MySQL 8 and domain-typed in
// PostgreSQL, which sqlx refuses to decode as String/i64. Every query therefore casts to plain
// text and signed integers under a fixed lowercase alias, and rows are read with try_get so a
// server-specific type never panics the command.

macro_rules! catalog_row {
    ($row:expr) => {
        CatalogRow {
            kind: $row.try_get("kind")?,
            table: $row.try_get("tbl")?,
            a: $row.try_get("a")?,
            b: $row.try_get("b")?,
            c: $row.try_get("c")?,
            d: $row.try_get("d")?,
            e: $row.try_get("e")?,
            f: $row.try_get("f")?,
            g: $row.try_get("g")?,
            n: $row.try_get("n")?,
        }
    };
}

pub async fn mysql_metadata(
    pool: &MySqlPool,
    schema_filter: Option<&str>,
    table: Option<&str>,
    include_tables: bool,
    include_columns: bool,
) -> AppResult<MetadataSnapshot> {
    let located;
    let schema_filter = match (schema_filter, table) {
        (None, Some(table)) => {
            // A bare table name resolves to the current database first, then any other one.
            located = sqlx::query_scalar::<_, String>(
                "SELECT CAST(table_schema AS CHAR) FROM information_schema.tables WHERE table_name = ? \
                 AND table_schema NOT IN ('information_schema','mysql','performance_schema','sys') \
                 ORDER BY table_schema = DATABASE() DESC, table_schema LIMIT 1",
            )
            .bind(table)
            .fetch_optional(pool)
            .await?;
            match located.as_deref() {
                Some(schema) => Some(schema),
                None => return Ok(MetadataSnapshot { databases: Vec::new() }),
            }
        }
        (schema, _) => schema,
    };
    if let (Some(schema), true) = (schema_filter, table.is_some() || include_columns) {
        let rows = sqlx::query(MYSQL_DETAIL_SQL)
            .bind(schema)
            .bind(table)
            .bind(table)
            .bind(schema)
            .bind(table)
            .bind(table)
            .bind(schema)
            .bind(table)
            .bind(table)
            .bind(schema)
            .bind(table)
            .bind(table)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| Ok(catalog_row!(row)))
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        return Ok(snapshot_from_catalog(schema, rows));
    }

    if let (Some(schema), true) = (schema_filter, include_tables) {
        // The tree already knows the database exists; listing its tables is a single round trip.
        let tables: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT CAST(table_schema AS CHAR) AS table_schema, CAST(table_name AS CHAR) AS table_name, \
             CAST(table_type AS CHAR) AS table_type FROM information_schema.tables \
             WHERE table_schema = ? ORDER BY table_name",
        )
        .bind(schema)
        .fetch_all(pool)
        .await?;
        return Ok(group_table_list(vec![schema.to_string()], tables));
    }
    let schemas: Vec<(String,)> = sqlx::query_as(
        "SELECT CAST(schema_name AS CHAR) AS schema_name FROM information_schema.schemata \
         WHERE schema_name NOT IN ('information_schema','mysql','performance_schema','sys') \
         AND (? IS NULL OR schema_name = ?) \
         ORDER BY schema_name",
    )
    .bind(schema_filter)
    .bind(schema_filter)
    .fetch_all(pool)
    .await?;
    if !include_tables {
        return Ok(MetadataSnapshot {
            databases: schemas
                .into_iter()
                .map(|(name,)| DatabaseMetadata { name, tables: Vec::new() })
                .collect(),
        });
    }
    let tables: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT CAST(table_schema AS CHAR) AS table_schema, CAST(table_name AS CHAR) AS table_name, \
         CAST(table_type AS CHAR) AS table_type FROM information_schema.tables \
         WHERE table_schema NOT IN ('information_schema','mysql','performance_schema','sys') \
         AND (? IS NULL OR table_schema = ?) ORDER BY table_schema, table_name",
    )
    .bind(schema_filter)
    .bind(schema_filter)
    .fetch_all(pool)
    .await?;
    Ok(group_table_list(schemas.into_iter().map(|(name,)| name).collect(), tables))
}

// One round trip returns the table type, columns, index parts and foreign key parts for a single
// table or a whole schema. Every text column is cast to CHAR (see the note at the top) and `s`/`n`
// give each kind its natural order.
const MYSQL_DETAIL_SQL: &str = "\
SELECT 't' AS kind, CAST(table_name AS CHAR) AS tbl, CAST(table_type AS CHAR) AS a, CAST(NULL AS CHAR) AS b, \
       CAST(NULL AS CHAR) AS c, CAST(NULL AS CHAR) AS d, CAST(NULL AS CHAR) AS e, CAST(NULL AS CHAR) AS f, \
       CAST(NULL AS CHAR) AS g, CAST('' AS CHAR) AS s, CAST(0 AS SIGNED) AS n \
  FROM information_schema.tables WHERE table_schema = ? AND (? IS NULL OR table_name = ?) \
UNION ALL \
SELECT 'c', CAST(table_name AS CHAR), CAST(column_name AS CHAR), CAST(column_type AS CHAR), \
       CAST(is_nullable AS CHAR), CAST(column_default AS CHAR), CAST(column_key AS CHAR), \
       CAST(column_comment AS CHAR), CAST(extra AS CHAR), CAST('' AS CHAR), CAST(ordinal_position AS SIGNED) \
  FROM information_schema.columns WHERE table_schema = ? AND (? IS NULL OR table_name = ?) \
UNION ALL \
SELECT 'i', CAST(table_name AS CHAR), CAST(index_name AS CHAR), CAST(column_name AS CHAR), \
       CAST(non_unique AS CHAR), CAST(NULL AS CHAR), CAST(NULL AS CHAR), CAST(NULL AS CHAR), CAST(NULL AS CHAR), \
       CAST(index_name AS CHAR), CAST(seq_in_index AS SIGNED) \
  FROM information_schema.statistics WHERE table_schema = ? AND (? IS NULL OR table_name = ?) \
UNION ALL \
SELECT 'f', CAST(table_name AS CHAR), CAST(constraint_name AS CHAR), CAST(column_name AS CHAR), \
       CAST(referenced_table_name AS CHAR), CAST(referenced_column_name AS CHAR), CAST(NULL AS CHAR), \
       CAST(NULL AS CHAR), CAST(NULL AS CHAR), CAST(constraint_name AS CHAR), CAST(ordinal_position AS SIGNED) \
  FROM information_schema.key_column_usage WHERE table_schema = ? AND (? IS NULL OR table_name = ?) \
   AND referenced_table_name IS NOT NULL \
ORDER BY tbl, kind, s, n";

pub async fn postgres_metadata(
    pool: &PgPool,
    schema_filter: Option<&str>,
    table: Option<&str>,
    include_tables: bool,
    include_columns: bool,
) -> AppResult<MetadataSnapshot> {
    let located;
    let schema_filter = match (schema_filter, table) {
        (None, Some(table)) => {
            located = sqlx::query_scalar::<_, String>(
                "SELECT n.nspname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relname = $1 AND c.relkind IN ('r','p','v','m','f') \
                 AND n.nspname NOT IN ('pg_catalog','information_schema') \
                 ORDER BY n.nspname = current_schema() DESC, n.nspname LIMIT 1",
            )
            .bind(table)
            .fetch_optional(pool)
            .await?;
            match located.as_deref() {
                Some(schema) => Some(schema),
                None => return Ok(MetadataSnapshot { databases: Vec::new() }),
            }
        }
        (schema, _) => schema,
    };
    if let (Some(schema), true) = (schema_filter, table.is_some() || include_columns) {
        let rows = sqlx::query(POSTGRES_DETAIL_SQL)
            .bind(schema)
            .bind(table)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| Ok(catalog_row!(row)))
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        return Ok(snapshot_from_catalog(schema, rows));
    }

    if let (Some(schema), true) = (schema_filter, include_tables) {
        let tables: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT n.nspname::text, c.relname::text, \
             CASE c.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' \
                  WHEN 'f' THEN 'FOREIGN TABLE' ELSE 'BASE TABLE' END \
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind IN ('r','p','v','m','f') AND n.nspname = $1 ORDER BY c.relname",
        )
        .bind(schema)
        .fetch_all(pool)
        .await?;
        return Ok(group_table_list(vec![schema.to_string()], tables));
    }
    let schemas: Vec<(String,)> = sqlx::query_as(
        "SELECT nspname::text FROM pg_namespace \
         WHERE nspname NOT IN ('pg_catalog','information_schema') \
         AND nspname NOT LIKE 'pg_toast%' AND nspname NOT LIKE 'pg_temp%' \
         AND ($1::text IS NULL OR nspname = $1) \
         ORDER BY nspname",
    )
    .bind(schema_filter)
    .fetch_all(pool)
    .await?;
    if !include_tables {
        return Ok(MetadataSnapshot {
            databases: schemas
                .into_iter()
                .map(|(name,)| DatabaseMetadata { name, tables: Vec::new() })
                .collect(),
        });
    }
    let tables: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT n.nspname::text, c.relname::text, \
         CASE c.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' \
              WHEN 'f' THEN 'FOREIGN TABLE' ELSE 'BASE TABLE' END \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind IN ('r','p','v','m','f') \
         AND n.nspname NOT IN ('pg_catalog','information_schema') \
         AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%' \
         AND ($1::text IS NULL OR n.nspname = $1) \
         ORDER BY n.nspname, c.relname",
    )
    .bind(schema_filter)
    .fetch_all(pool)
    .await?;
    Ok(group_table_list(schemas.into_iter().map(|(name,)| name).collect(), tables))
}

const POSTGRES_DETAIL_SQL: &str = "\
SELECT 't'::text AS kind, c.relname::text AS tbl, \
       CASE c.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' WHEN 'f' THEN 'FOREIGN TABLE' ELSE 'BASE TABLE' END AS a, \
       NULL::text AS b, NULL::text AS c, NULL::text AS d, NULL::text AS e, NULL::text AS f, NULL::text AS g, \
       ''::text AS s, 0::int8 AS n \
  FROM pg_class c JOIN pg_namespace ns ON ns.oid = c.relnamespace \
 WHERE ns.nspname = $1 AND c.relkind IN ('r','p','v','m','f') AND ($2::text IS NULL OR c.relname = $2) \
UNION ALL \
SELECT 'c', c.relname::text, a.attname::text, format_type(a.atttypid, a.atttypmod), \
       CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END, pg_get_expr(d.adbin, d.adrelid), \
       CASE WHEN EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary AND a.attnum = ANY(i.indkey)) THEN 'PRI' END, \
       col_description(c.oid, a.attnum), \
       CASE WHEN a.attidentity IN ('a', 'd') THEN 'identity' WHEN a.attgenerated = 's' THEN 'generated' END, \
       '', a.attnum::int8 \
  FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace ns ON ns.oid = c.relnamespace \
  LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
 WHERE ns.nspname = $1 AND c.relkind IN ('r','p','v','m','f') AND ($2::text IS NULL OR c.relname = $2) \
   AND a.attnum > 0 AND NOT a.attisdropped \
UNION ALL \
SELECT 'i', t.relname::text, i.relname::text, \
       COALESCE(a.attname::text, pg_get_indexdef(ix.indexrelid, k.ord::int, true)), \
       CASE WHEN ix.indisunique THEN '0' ELSE '1' END, NULL, NULL, NULL, NULL, i.relname::text, k.ord::int8 \
  FROM pg_index ix JOIN pg_class t ON t.oid = ix.indrelid JOIN pg_class i ON i.oid = ix.indexrelid \
  JOIN pg_namespace ns ON ns.oid = t.relnamespace \
 CROSS JOIN LATERAL unnest(ix.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) \
  LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum AND k.attnum > 0 \
 WHERE ns.nspname = $1 AND ($2::text IS NULL OR t.relname = $2) AND k.ord <= ix.indnatts \
UNION ALL \
SELECT 'f', cl.relname::text, con.conname::text, a.attname::text, rcl.relname::text, ra.attname::text, \
       NULL, NULL, NULL, con.conname::text, k.ord::int8 \
  FROM pg_constraint con JOIN pg_class cl ON cl.oid = con.conrelid \
  JOIN pg_namespace ns ON ns.oid = cl.relnamespace JOIN pg_class rcl ON rcl.oid = con.confrelid \
 CROSS JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS k(attnum, refnum, ord) \
  JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum \
  JOIN pg_attribute ra ON ra.attrelid = con.confrelid AND ra.attnum = k.refnum \
 WHERE con.contype = 'f' AND ns.nspname = $1 AND ($2::text IS NULL OR cl.relname = $2) \
ORDER BY tbl, kind, s, n";

struct CatalogRow {
    kind: String,
    table: String,
    a: Option<String>,
    b: Option<String>,
    c: Option<String>,
    d: Option<String>,
    e: Option<String>,
    f: Option<String>,
    g: Option<String>,
    n: i64,
}

fn snapshot_from_catalog(schema: &str, rows: Vec<CatalogRow>) -> MetadataSnapshot {
    let mut tables: BTreeMap<String, TableMetadata> = BTreeMap::new();
    let mut indexes: BTreeMap<(String, String), IndexMetadata> = BTreeMap::new();
    let mut foreign_keys: BTreeMap<String, Vec<(String, String, String, String)>> = BTreeMap::new();
    for row in rows {
        let entry = tables.entry(row.table.clone()).or_insert_with(|| TableMetadata {
            schema: schema.to_string(),
            name: row.table.clone(),
            table_type: "BASE TABLE".into(),
            columns: Vec::new(),
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
        });
        match row.kind.as_str() {
            "t" => entry.table_type = row.a.unwrap_or_else(|| "BASE TABLE".into()),
            "c" => entry.columns.push(ColumnMetadata {
                name: row.a.unwrap_or_default(),
                data_type: row.b.unwrap_or_default(),
                nullable: row.c.as_deref() == Some("YES"),
                default_value: row.d,
                is_primary_key: row.e.as_deref() == Some("PRI"),
                ordinal: row.n,
                comment: row.f.filter(|value| !value.is_empty()),
                extra: row.g.filter(|value| !value.is_empty()),
            }),
            "i" => {
                let name = row.a.unwrap_or_default();
                let index = indexes
                    .entry((row.table.clone(), name.clone()))
                    .or_insert_with(|| IndexMetadata {
                        name,
                        columns: Vec::new(),
                        unique: row.c.as_deref() == Some("0"),
                    });
                // Functional index parts have no column name.
                index.columns.push(row.b.unwrap_or_else(|| "(expression)".into()));
            }
            "f" => foreign_keys.entry(row.table.clone()).or_default().push((
                row.a.unwrap_or_default(),
                row.b.unwrap_or_default(),
                row.c.unwrap_or_default(),
                row.d.unwrap_or_default(),
            )),
            _ => {}
        }
    }
    for ((table, _), index) in indexes {
        if let Some(entry) = tables.get_mut(&table) {
            entry.indexes.push(index);
        }
    }
    for (table, rows) in foreign_keys {
        if let Some(entry) = tables.get_mut(&table) {
            entry.foreign_keys = group_foreign_keys(rows);
        }
    }
    MetadataSnapshot {
        databases: vec![DatabaseMetadata {
            name: schema.to_string(),
            tables: tables.into_values().collect(),
        }],
    }
}

fn group_table_list(schemas: Vec<String>, tables: Vec<(String, String, String)>) -> MetadataSnapshot {
    let mut grouped: BTreeMap<String, Vec<TableMetadata>> =
        schemas.into_iter().map(|name| (name, Vec::new())).collect();
    for (schema, name, table_type) in tables {
        if let Some(list) = grouped.get_mut(&schema) {
            list.push(TableMetadata {
                schema: schema.clone(),
                name,
                table_type,
                columns: Vec::new(),
                indexes: Vec::new(),
                foreign_keys: Vec::new(),
            });
        }
    }
    MetadataSnapshot {
        databases: grouped
            .into_iter()
            .map(|(name, tables)| DatabaseMetadata { name, tables })
            .collect(),
    }
}

/// Composite foreign keys arrive as one row per column; fold them into one entry per constraint.
fn group_foreign_keys(rows: Vec<(String, String, String, String)>) -> Vec<ForeignKeyMetadata> {
    let mut grouped: Vec<ForeignKeyMetadata> = Vec::new();
    for (name, column, referenced_table, referenced_column) in rows {
        match grouped.iter_mut().find(|key| key.name == name) {
            Some(key) => {
                if !key.columns.contains(&column) {
                    key.columns.push(column);
                }
                if !key.referenced_columns.contains(&referenced_column) {
                    key.referenced_columns.push(referenced_column);
                }
            }
            None => grouped.push(ForeignKeyMetadata {
                name,
                columns: vec![column],
                referenced_table,
                referenced_columns: vec![referenced_column],
            }),
        }
    }
    grouped
}

pub async fn mysql_object_stats(pool: &MySqlPool, schema: &str) -> AppResult<Vec<ObjectStat>> {
    let rows = sqlx::query(
        "SELECT CAST(table_name AS CHAR) AS name, CAST(table_type AS CHAR) AS object_type, \
         CAST(table_rows AS SIGNED) AS rows_estimate, CAST(data_length AS SIGNED) AS data_bytes, \
         CAST(index_length AS SIGNED) AS index_bytes, CAST(engine AS CHAR) AS engine, \
         CAST(auto_increment AS SIGNED) AS auto_increment, CAST(table_collation AS CHAR) AS collation, \
         CAST(create_time AS CHAR) AS created, CAST(update_time AS CHAR) AS updated, \
         CAST(table_comment AS CHAR) AS comment \
         FROM information_schema.tables WHERE table_schema = ? ORDER BY table_name",
    )
    .bind(schema)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            let object_type: String = row.try_get("object_type")?;
            let is_view = object_type.contains("VIEW");
            Ok(ObjectStat {
                name: row.try_get("name")?,
                rows: if is_view { None } else { row.try_get("rows_estimate")? },
                data_bytes: if is_view { None } else { row.try_get("data_bytes")? },
                index_bytes: if is_view { None } else { row.try_get("index_bytes")? },
                engine: row.try_get("engine")?,
                auto_increment: row.try_get("auto_increment")?,
                collation: row.try_get("collation")?,
                created: row.try_get("created")?,
                updated: row.try_get("updated")?,
                // MySQL reports "VIEW" as the comment of every view.
                comment: row
                    .try_get::<Option<String>, _>("comment")?
                    .filter(|value| !value.is_empty() && !(is_view && value == "VIEW")),
                owner: None,
                object_type,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(AppError::from)
}

pub async fn postgres_object_stats(pool: &PgPool, schema: &str) -> AppResult<Vec<ObjectStat>> {
    let rows = sqlx::query(
        "SELECT c.relname::text AS name, \
         CASE c.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' WHEN 'f' THEN 'FOREIGN TABLE' ELSE 'BASE TABLE' END AS object_type, \
         CASE WHEN c.relkind IN ('r','p','m') AND c.reltuples >= 0 THEN c.reltuples::int8 END AS rows_estimate, \
         CASE WHEN c.relkind IN ('r','p','m') THEN pg_table_size(c.oid)::int8 END AS data_bytes, \
         CASE WHEN c.relkind IN ('r','p','m') THEN pg_indexes_size(c.oid)::int8 END AS index_bytes, \
         pg_get_userbyid(c.relowner)::text AS owner, obj_description(c.oid, 'pg_class') AS comment \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relkind IN ('r','p','v','m','f') ORDER BY c.relname",
    )
    .bind(schema)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(ObjectStat {
                name: row.try_get("name")?,
                object_type: row.try_get("object_type")?,
                rows: row.try_get("rows_estimate")?,
                data_bytes: row.try_get("data_bytes")?,
                index_bytes: row.try_get("index_bytes")?,
                owner: row.try_get("owner")?,
                comment: row.try_get("comment")?,
                ..Default::default()
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(AppError::from)
}

/// PostgreSQL has no SHOW CREATE TABLE; rebuild an equivalent script from the catalog the same
/// way pg_dump does for a single table (columns, constraints, extra indexes and comments).
pub async fn postgres_ddl(pool: &PgPool, schema: &str, table: &str) -> AppResult<String> {
    let relation: Option<(Oid, String, String)> = sqlx::query_as(
        "SELECT c.oid, c.relkind::text, quote_ident(n.nspname) || '.' || quote_ident(c.relname) \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2",
    )
    .bind(schema)
    .bind(table)
    .fetch_optional(pool)
    .await?;
    let (oid, kind, qualified) = relation
        .ok_or_else(|| AppError::validation(format!("Table '{schema}.{table}' was not found.")))?;

    let mut script = String::new();
    if kind == "v" || kind == "m" {
        let definition: String = sqlx::query_scalar("SELECT pg_get_viewdef($1, true)")
            .bind(oid)
            .fetch_one(pool)
            .await?;
        let keyword = if kind == "m" { "MATERIALIZED VIEW" } else { "VIEW" };
        script.push_str(&format!(
            "CREATE {keyword} {qualified} AS\n{};",
            definition.trim_end().trim_end_matches(';')
        ));
    } else {
        let columns: Vec<(String, String, bool, Option<String>, String, String)> = sqlx::query_as(
            "SELECT quote_ident(a.attname), format_type(a.atttypid, a.atttypmod), a.attnotnull, \
             pg_get_expr(d.adbin, d.adrelid), a.attidentity::text, a.attgenerated::text \
             FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
             WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
        )
        .bind(oid)
        .fetch_all(pool)
        .await?;
        let constraints: Vec<(String, String)> = sqlx::query_as(
            "SELECT quote_ident(conname), pg_get_constraintdef(oid, true) FROM pg_constraint \
             WHERE conrelid = $1 AND contype IN ('p', 'u', 'f', 'c', 'x') \
             ORDER BY CASE contype WHEN 'p' THEN 0 WHEN 'u' THEN 1 WHEN 'f' THEN 2 ELSE 3 END, conname",
        )
        .bind(oid)
        .fetch_all(pool)
        .await?;
        let mut lines = columns
            .into_iter()
            .map(|(name, data_type, not_null, default, identity, generated)| {
                let mut line = format!("    {name} {data_type}");
                match (identity.as_str(), generated.as_str()) {
                    ("a", _) => line.push_str(" GENERATED ALWAYS AS IDENTITY"),
                    ("d", _) => line.push_str(" GENERATED BY DEFAULT AS IDENTITY"),
                    (_, "s") => {
                        if let Some(expression) = &default {
                            line.push_str(&format!(" GENERATED ALWAYS AS ({expression}) STORED"));
                        }
                    }
                    _ => {
                        if let Some(expression) = &default {
                            line.push_str(&format!(" DEFAULT {expression}"));
                        }
                    }
                }
                if not_null {
                    line.push_str(" NOT NULL");
                }
                line
            })
            .collect::<Vec<_>>();
        lines.extend(
            constraints
                .into_iter()
                .map(|(name, definition)| format!("    CONSTRAINT {name} {definition}")),
        );
        script.push_str(&format!("CREATE TABLE {qualified} (\n{}\n);", lines.join(",\n")));

        let indexes: Vec<(String,)> = sqlx::query_as(
            "SELECT pg_get_indexdef(i.indexrelid) FROM pg_index i \
             WHERE i.indrelid = $1 AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid) \
             ORDER BY i.indexrelid",
        )
        .bind(oid)
        .fetch_all(pool)
        .await?;
        for (definition,) in indexes {
            script.push_str(&format!("\n\n{definition};"));
        }
        let column_comments: Vec<(String, String)> = sqlx::query_as(
            "SELECT quote_ident(a.attname), col_description(a.attrelid, a.attnum) FROM pg_attribute a \
             WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped \
             AND col_description(a.attrelid, a.attnum) IS NOT NULL ORDER BY a.attnum",
        )
        .bind(oid)
        .fetch_all(pool)
        .await?;
        for (column, comment) in column_comments {
            script.push_str(&format!(
                "\n\nCOMMENT ON COLUMN {qualified}.{column} IS '{}';",
                comment.replace('\'', "''")
            ));
        }
    }
    let table_comment: Option<String> = sqlx::query_scalar("SELECT obj_description($1, 'pg_class')")
        .bind(oid)
        .fetch_one(pool)
        .await?;
    if let Some(comment) = table_comment {
        let keyword = if kind == "v" { "VIEW" } else if kind == "m" { "MATERIALIZED VIEW" } else { "TABLE" };
        script.push_str(&format!(
            "\n\nCOMMENT ON {keyword} {qualified} IS '{}';",
            comment.replace('\'', "''")
        ));
    }
    Ok(script)
}
