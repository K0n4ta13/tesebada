use super::{Config, Query, parser::Where};
use std::collections::HashSet;

use anyhow::anyhow;

pub fn validate_config(config: &Config) -> bool {
    let fields_per_table: Vec<HashSet<&str>> = config
        .tables
        .iter()
        .map(|table| table.fields.iter().map(|field| &*field.name).collect())
        .collect();
    let fragments_fields_per_table: Vec<Vec<HashSet<&str>>> = config
        .tables
        .iter()
        .map(|table| {
            table
                .fragments
                .iter()
                .map(|fragment| {
                    fragment
                        .fields
                        .iter()
                        .map(|field| &*field.reference)
                        .collect()
                })
                .collect()
        })
        .collect();

    fields_per_table
        .iter()
        .zip(&fragments_fields_per_table)
        .any(|(fields, fragments)| {
            fragments
                .iter()
                .any(|fragment_fields| fragment_fields != fields)
        })
}

pub fn check_query(query: &Query, config: &Config) -> anyhow::Result<()> {
    match query {
        Query::Select {
            table,
            fields,
            filter,
            ..
        } => {
            check_table(table, config)?;
            check_fields(table, fields, config)?;
            check_filter(table, filter, config)?;
        }
        Query::Insert {
            table,
            columns,
            values,
        } => {
            check_table(table, config)?;
            check_fields(table, columns, config)?;
            if values.iter().any(|v| v.len() != columns.len()) {
                return Err(anyhow!(
                    "expected {} values found {}",
                    columns.len(),
                    values.len()
                ));
            }
        }
        Query::Update {
            table,
            assignments,
            filter,
            ..
        } => {
            check_table(table, config)?;
            check_assignments(table, assignments, config)?;
            check_filter(table, filter, config)?;
        }
        Query::Delete { table, filter, .. } => {
            check_table(table, config)?;
            check_filter(table, filter, config)?;
        }
    };

    Ok(())
}

fn check_assignments(
    table_name: &str,
    assignments: &[(String, String)],
    config: &Config,
) -> anyhow::Result<()> {
    let table = config
        .tables
        .iter()
        .find(|table| table.name == table_name)
        .ok_or_else(|| anyhow!("table '{table_name}' not found"))?;

    let fields: Vec<_> = assignments.iter().map(|assignment| &assignment.0).collect();
    let table_fields: Vec<_> = table.fields.iter().map(|field| &field.name).collect();

    let missing: Vec<_> = fields
        .iter()
        .filter(|field| !table_fields.contains(field))
        .collect();

    if missing.is_empty() {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "missing fields {:?} in table '{table_name}'",
            missing
        ))
    }
}

fn check_filter(table_name: &str, filter: &Option<Where>, config: &Config) -> anyhow::Result<()> {
    if let Some(filter) = filter {
        let table = config
            .tables
            .iter()
            .find(|table| table.name == table_name)
            .ok_or_else(|| anyhow!("table '{table_name}' not found"))?;

        if !table.fields.iter().any(|field| field.name == filter.column) {
            return Err(anyhow!(
                "field '{}' not found in where clause",
                filter.column
            ));
        }
    }

    Ok(())
}

fn check_fields(table_name: &str, fields: &[String], config: &Config) -> anyhow::Result<()> {
    let table = config
        .tables
        .iter()
        .find(|table| table.name == table_name)
        .ok_or_else(|| anyhow!("table '{table_name}' not found"))?;

    let table_fields: Vec<_> = table.fields.iter().map(|field| &field.name).collect();
    let missing: Vec<_> = fields
        .iter()
        .filter(|field| !table_fields.contains(field))
        .collect();

    if missing.is_empty() || missing.len() == 1 && missing[0] == "*" {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "missing fields {:?} in table '{table_name}'",
            missing
        ))
    }
}

fn check_table(table_name: &str, config: &Config) -> anyhow::Result<()> {
    if config.tables.iter().any(|table| table.name == table_name) {
        Ok(())
    } else {
        Err(anyhow!("table '{table_name}' not found"))
    }
}
