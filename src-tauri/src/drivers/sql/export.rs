use crate::error::{AppError, AppResult};
use crate::models::Protocol;
use serde_json::Value;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

pub struct SqlSnapshotWriter {
    protocol: Protocol,
    format: String,
    path: PathBuf,
    final_path: PathBuf,
    staging_path: Option<PathBuf>,
    schema: String,
    database_scope: bool,
    shared: Option<BufWriter<File>>,
    table_file: Option<BufWriter<File>>,
    columns: Vec<String>,
    current_table: Option<String>,
    table_index: usize,
    tables: u64,
    rows: u64,
}

impl SqlSnapshotWriter {
    pub fn new(
        protocol: Protocol,
        format: &str,
        path: &str,
        schema: &str,
        database_scope: bool,
    ) -> AppResult<Self> {
        if !matches!(format, "csv" | "jsonl" | "sql") {
            return Err(AppError::validation(format!(
                "Unsupported export format '{format}'. Use csv, jsonl or sql."
            )));
        }
        let output = PathBuf::from(path);
        if database_scope && format == "csv" && output.is_file() {
            return Err(AppError::validation(
                "Database-level CSV export needs a folder path, but the path points to a file.",
            ));
        }
        let parent = if database_scope && format == "csv" {
            output.parent()
        } else {
            output.parent()
        };
        if let Some(parent) = parent.filter(|parent| !parent.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        let staging = temporary_output_path(&output, database_scope && format == "csv");
        if database_scope && format == "csv" {
            fs::create_dir(&staging)?;
        }
        let shared = if database_scope && format == "csv" {
            None
        } else {
            Some(BufWriter::new(File::create(&staging)?))
        };
        Ok(Self {
            protocol,
            format: format.to_string(),
            path: staging.clone(),
            final_path: output,
            staging_path: Some(staging),
            schema: schema.to_string(),
            database_scope,
            shared,
            table_file: None,
            columns: Vec::new(),
            current_table: None,
            table_index: 0,
            tables: 0,
            rows: 0,
        })
    }

    pub fn begin_table(&mut self, table: &str, columns: Vec<String>) -> AppResult<()> {
        self.finish_table()?;
        if columns.is_empty() {
            return Err(AppError::validation(format!(
                "Table '{table}' did not return any columns for export."
            )));
        }
        self.columns = columns;
        self.current_table = Some(table.to_string());
        self.table_index = self.table_index.saturating_add(1);
        self.tables = self.tables.saturating_add(1);
        match self.format.as_str() {
            "csv" if self.database_scope => {
                let file_name = format!(
                    "{:03}-{}.{}.csv",
                    self.table_index,
                    safe_file_part(&self.schema),
                    safe_file_part(table)
                );
                let file_path = self.path.join(file_name);
                let mut writer = BufWriter::new(File::create(file_path)?);
                writeln!(
                    writer,
                    "{}",
                    self.columns
                        .iter()
                        .map(|column| csv_escape(column))
                        .collect::<Vec<_>>()
                        .join(",")
                )?;
                self.table_file = Some(writer);
            }
            "csv" => {
                let writer = self.shared.as_mut().ok_or_else(|| {
                    AppError::new(
                        crate::models::ErrorClass::Internal,
                        "CSV export writer was not initialized.",
                    )
                })?;
                writeln!(
                    writer,
                    "{}",
                    self.columns
                        .iter()
                        .map(|column| csv_escape(column))
                        .collect::<Vec<_>>()
                        .join(",")
                )?;
            }
            "jsonl" => {}
            "sql" => {
                let writer = self.shared.as_mut().ok_or_else(|| {
                    AppError::new(
                        crate::models::ErrorClass::Internal,
                        "SQL export writer was not initialized.",
                    )
                })?;
                writeln!(writer, "\n-- {} table", qualified_table(&self.protocol, &self.schema, table))?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    pub fn write_row(&mut self, row: &[Value]) -> AppResult<()> {
        let table = self.current_table.clone().ok_or_else(|| {
            AppError::new(
                crate::models::ErrorClass::Internal,
                "Export row was written before a table was started.",
            )
        })?;
        if row.len() != self.columns.len() {
            return Err(AppError::new(
                crate::models::ErrorClass::Internal,
                format!(
                    "Export row for '{table}' has {} cells but {} columns.",
                    row.len(),
                    self.columns.len()
                ),
            ));
        }
        match self.format.as_str() {
            "csv" => {
                let line = format!(
                    "{}\n",
                    row.iter().map(csv_cell).collect::<Vec<_>>().join(",")
                );
                if let Some(writer) = self.table_file.as_mut() {
                    writer.write_all(line.as_bytes())?;
                } else {
                    self.shared
                        .as_mut()
                        .ok_or_else(|| {
                            AppError::new(
                                crate::models::ErrorClass::Internal,
                                "CSV export writer was not initialized.",
                            )
                        })?
                        .write_all(line.as_bytes())?;
                }
            }
            "jsonl" => {
                let writer = self.shared.as_mut().ok_or_else(|| {
                    AppError::new(
                        crate::models::ErrorClass::Internal,
                        "JSON Lines export writer was not initialized.",
                    )
                })?;
                let mut object = serde_json::Map::with_capacity(self.columns.len() + 2);
                for (column, value) in self.columns.iter().cloned().zip(row.iter().cloned()) {
                    object.insert(column, value);
                }
                if self.database_scope {
                    object.insert("__luxbase_schema".into(), Value::String(self.schema.clone()));
                    object.insert("__luxbase_table".into(), Value::String(table.to_string()));
                }
                serde_json::to_writer(&mut *writer, &object)?;
                writer.write_all(b"\n")?;
            }
            "sql" => {
                let writer = self.shared.as_mut().ok_or_else(|| {
                    AppError::new(
                        crate::models::ErrorClass::Internal,
                        "SQL export writer was not initialized.",
                    )
                })?;
                let values = row
                    .iter()
                    .map(|value| sql_literal(&self.protocol, value))
                    .collect::<Vec<_>>()
                    .join(", ");
                writeln!(
                    writer,
                    "INSERT INTO {} ({}) VALUES ({values});",
                    qualified_table(&self.protocol, &self.schema, &table),
                    self.columns
                        .iter()
                        .map(|column| quote_identifier(&self.protocol, column))
                        .collect::<Vec<_>>()
                        .join(", ")
                )?;
            }
            _ => unreachable!(),
        }
        self.rows = self.rows.saturating_add(1);
        Ok(())
    }

    pub fn finish_table(&mut self) -> AppResult<()> {
        if let Some(writer) = self.table_file.as_mut() {
            writer.flush()?;
        }
        self.table_file = None;
        self.columns.clear();
        self.current_table = None;
        Ok(())
    }

    pub fn finish(mut self) -> AppResult<(u64, u64)> {
        self.finish_table()?;
        if let Some(writer) = self.shared.as_mut() {
            writer.flush()?;
        }
        self.shared = None;
        let staging = self.staging_path.take().ok_or_else(|| {
            AppError::new(
                crate::models::ErrorClass::Internal,
                "Export staging path was not initialized.",
            )
        })?;
        if self.database_scope && self.format == "csv" {
            fs::create_dir_all(&self.final_path)?;
            for entry in fs::read_dir(&staging)? {
                let entry = entry?;
                replace_completed_output(&entry.path(), &self.final_path.join(entry.file_name()))?;
            }
            fs::remove_dir(&staging)?;
        } else {
            replace_completed_output(&staging, &self.final_path)?;
        }
        Ok((self.rows, self.tables))
    }
}

impl Drop for SqlSnapshotWriter {
    fn drop(&mut self) {
        let Some(staging) = self.staging_path.as_ref() else {
            return;
        };
        if staging.is_dir() {
            let _ = fs::remove_dir_all(staging);
        } else {
            let _ = fs::remove_file(staging);
        }
    }
}

fn temporary_output_path(output: &std::path::Path, directory: bool) -> PathBuf {
    let parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let name = output
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("export");
    let kind = if directory { "dir" } else { "file" };
    parent.join(format!(
        ".{name}.luxbase-{kind}-{}.tmp",
        uuid::Uuid::new_v4()
    ))
}

#[cfg(not(target_os = "windows"))]
fn replace_completed_output(source: &std::path::Path, destination: &std::path::Path) -> AppResult<()> {
    fs::rename(source, destination)?;
    Ok(())
}

#[cfg(target_os = "windows")]
fn replace_completed_output(source: &std::path::Path, destination: &std::path::Path) -> AppResult<()> {
    if !destination.exists() {
        fs::rename(source, destination)?;
        return Ok(());
    }
    let backup = temporary_output_path(destination, false);
    fs::rename(destination, &backup)?;
    match fs::rename(source, destination) {
        Ok(()) => {
            let _ = fs::remove_file(backup);
            Ok(())
        }
        Err(error) => {
            let _ = fs::rename(&backup, destination);
            Err(error.into())
        }
    }
}

fn safe_file_part(value: &str) -> String {
    let cleaned = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    let cleaned = cleaned.trim_matches('.').trim_matches('_');
    if cleaned.is_empty() {
        "unnamed".into()
    } else {
        cleaned.to_string()
    }
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn csv_cell(value: &Value) -> String {
    if value.is_null() {
        return String::new();
    }
    let text = match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    };
    csv_escape(&text)
}

fn sql_literal(protocol: &Protocol, value: &Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => format!("'{}'", value.replace('\'', "''")),
        Value::Object(object) if object.len() == 1 => {
            let Some(Value::String(encoded)) = object.get("bytesBase64") else {
                return format!("'{}'", value.to_string().replace('\'', "''"));
            };
            use base64::Engine;
            let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
                return format!("'{}'", value.to_string().replace('\'', "''"));
            };
            let hex = bytes.iter().map(|byte| format!("{byte:02X}")).collect::<String>();
            match protocol {
                Protocol::Mysql | Protocol::Sqlite => format!("X'{hex}'"),
                Protocol::Postgres => format!("decode('{hex}', 'hex')"),
                Protocol::Redis | Protocol::Elasticsearch | Protocol::Milvus | Protocol::Qdrant => format!("'{}'", value.to_string().replace('\'', "''")),
            }
        }
        value => format!("'{}'", value.to_string().replace('\'', "''")),
    }
}

fn quote_identifier(protocol: &Protocol, identifier: &str) -> String {
    match protocol {
        Protocol::Mysql => format!("`{}`", identifier.replace('`', "``")),
        Protocol::Postgres | Protocol::Sqlite => {
            format!("\"{}\"", identifier.replace('"', "\"\""))
        }
        Protocol::Redis | Protocol::Elasticsearch | Protocol::Milvus | Protocol::Qdrant => identifier.to_string(),
    }
}

pub(super) fn qualified_table(protocol: &Protocol, schema: &str, table: &str) -> String {
    match protocol {
        Protocol::Mysql => format!(
            "`{}`.`{}`",
            schema.replace('`', "``"),
            table.replace('`', "``")
        ),
        Protocol::Postgres => format!(
            "\"{}\".\"{}\"",
            schema.replace('"', "\"\""),
            table.replace('"', "\"\"")
        ),
        Protocol::Sqlite => format!("\"{}\"", table.replace('"', "\"\"")),
        Protocol::Redis | Protocol::Elasticsearch | Protocol::Milvus | Protocol::Qdrant => format!("\"{}\"", table.replace('"', "\"\"")),
    }
}