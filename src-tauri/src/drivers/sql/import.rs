use crate::error::{AppError, AppResult};
use crate::models::{ParamValue, Protocol};
use serde_json::{Map, Value};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use super::{value_placeholders, MAX_BIND_PARAMETERS};

pub struct SqlImportSource {
    reader: BufReader<File>,
    format: String,
    protocol: Protocol,
    columns: Vec<String>,
    insert_prefix: String,
    queued_json: Option<Map<String, Value>>,
    data_row: u64,
}

impl SqlImportSource {
    pub fn open(
        protocol: Protocol,
        path: &str,
        format: &str,
        schema: Option<&str>,
        table: &str,
    ) -> AppResult<Self> {
        let format = format.trim().to_ascii_lowercase();
        if !matches!(format.as_str(), "csv" | "jsonl") {
            return Err(AppError::validation(
                "SQL import supports CSV and JSON Lines.",
            ));
        }
        let qualified = qualified_table(protocol, schema, table)?;
        let mut reader = BufReader::new(File::open(Path::new(path))?);
        let mut header = String::new();
        loop {
            let read = reader.read_line(&mut header)?;
            if read == 0 {
                return Err(AppError::validation("Import file is empty."));
            }
            trim_line_ending(&mut header);
            if !header.trim().is_empty() {
                break;
            }
            header.clear();
        }

        let (columns, queued_json) = if format == "csv" {
            (parse_csv_line(&header)?, None)
        } else {
            let first: Map<String, Value> = serde_json::from_str(&header)
                .map_err(|error| AppError::validation(format!("JSON row 1 is invalid: {error}")))?;
            let columns = first.keys().cloned().collect::<Vec<_>>();
            (columns, Some(first))
        };
        if columns.is_empty() {
            return Err(AppError::validation("Import header contains no columns."));
        }
        if columns.len() > MAX_BIND_PARAMETERS {
            return Err(AppError::validation(format!(
                "Import has {} columns; at most {MAX_BIND_PARAMETERS} bound columns are supported per row.",
                columns.len()
            )));
        }
        let insert_prefix = format!(
            "INSERT INTO {qualified} ({}) VALUES ",
            columns
                .iter()
                .map(|column| quote_identifier(protocol, column))
                .collect::<Vec<_>>()
                .join(", ")
        );
        Ok(Self {
            reader,
            format,
            protocol,
            columns,
            insert_prefix,
            queued_json,
            data_row: 0,
        })
    }

    pub fn safe_batch_size(&self, requested: u32) -> usize {
        let parameter_limit = (MAX_BIND_PARAMETERS / self.columns.len().max(1)).max(1);
        (requested.clamp(1, 10_000) as usize).min(parameter_limit)
    }

    pub fn insert_sql(&self, row_count: usize) -> AppResult<String> {
        Ok(format!(
            "{}{}",
            self.insert_prefix,
            value_placeholders(self.protocol, self.columns.len(), row_count)?
        ))
    }

    pub fn next_batch(&mut self, max_rows: usize) -> AppResult<Vec<Vec<ParamValue>>> {
        let mut rows = Vec::with_capacity(max_rows);
        while rows.len() < max_rows {
            match self.next_values()? {
                Some(values) => rows.push(values),
                None => break,
            }
        }
        Ok(rows)
    }

    fn next_values(&mut self) -> AppResult<Option<Vec<ParamValue>>> {
        if let Some(first) = self.queued_json.take() {
            self.data_row = 1;
            return self.json_values(first).map(Some);
        }
        let mut line = String::new();
        loop {
            let read = self.reader.read_line(&mut line)?;
            if read == 0 {
                return Ok(None);
            }
            trim_line_ending(&mut line);
            if !line.trim().is_empty() {
                break;
            }
            line.clear();
        }
        self.data_row = self.data_row.saturating_add(1);
        if self.format == "csv" {
            let values = parse_csv_line(&line)?;
            if values.len() != self.columns.len() {
                return Err(AppError::validation(format!(
                    "CSV row {} has {} values but {} columns are expected.",
                    self.data_row + 1,
                    values.len(),
                    self.columns.len()
                )));
            }
            Ok(Some(
                values.into_iter().map(ParamValue::Text).collect::<Vec<_>>(),
            ))
        } else {
            let object = serde_json::from_str::<Map<String, Value>>(&line)
                .map_err(|error| {
                    AppError::validation(format!(
                        "JSON row {} is invalid: {error}",
                        self.data_row + 1
                    ))
                })?;
            self.json_values(object).map(Some)
        }
    }

    fn json_values(&self, object: Map<String, Value>) -> AppResult<Vec<ParamValue>> {
        self.columns
            .iter()
            .map(|column| object.get(column).cloned().unwrap_or(Value::Null))
            .map(json_to_param)
            .collect()
    }
}

fn trim_line_ending(line: &mut String) {
    if line.ends_with('\n') {
        line.pop();
        if line.ends_with('\r') {
            line.pop();
        }
    }
}

fn parse_csv_line(line: &str) -> AppResult<Vec<String>> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut chars = line.chars().peekable();
    let mut quoted = false;
    while let Some(character) = chars.next() {
        if quoted {
            if character == '"' {
                if chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    quoted = false;
                }
            } else {
                current.push(character);
            }
        } else {
            match character {
                '"' => quoted = true,
                ',' => values.push(std::mem::take(&mut current)),
                _ => current.push(character),
            }
        }
    }
    if quoted {
        return Err(AppError::validation(
            "CSV contains an unterminated quoted field.",
        ));
    }
    values.push(current);
    Ok(values)
}

fn quote_identifier(protocol: Protocol, identifier: &str) -> String {
    match protocol {
        Protocol::Mysql => format!("`{}`", identifier.replace('`', "``")),
        Protocol::Postgres | Protocol::Sqlite => {
            format!("\"{}\"", identifier.replace('"', "\"\""))
        }
        Protocol::Redis | Protocol::Elasticsearch => identifier.to_string(),
    }
}

fn qualified_table(protocol: Protocol, schema: Option<&str>, table: &str) -> AppResult<String> {
    if table.trim().is_empty() || table.contains('\0') {
        return Err(AppError::validation("Import target table is invalid."));
    }
    match protocol {
        Protocol::Mysql | Protocol::Postgres => {
            let schema = schema
                .filter(|value| !value.trim().is_empty() && !value.contains('\0'))
                .ok_or_else(|| AppError::validation("SQL import requires a valid schema."))?;
            Ok(format!(
                "{}.{}",
                quote_identifier(protocol, schema),
                quote_identifier(protocol, table)
            ))
        }
        Protocol::Sqlite => Ok(quote_identifier(protocol, table)),
        Protocol::Redis | Protocol::Elasticsearch => Err(AppError::unsupported(
            "SQL import is not available for Redis or Elasticsearch.",
        )),
    }
}

fn json_to_param(value: Value) -> AppResult<ParamValue> {
    Ok(match value {
        Value::Null => ParamValue::Null,
        Value::Bool(value) => ParamValue::Bool(value),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                ParamValue::I64(value)
            } else if let Some(value) = value.as_u64() {
                ParamValue::U64(value)
            } else if let Some(value) = value.as_f64() {
                ParamValue::F64(value)
            } else {
                return Err(AppError::validation("Numeric cell value is out of range."));
            }
        }
        Value::String(value) => ParamValue::Text(value),
        value => ParamValue::Json(value),
    })
}
