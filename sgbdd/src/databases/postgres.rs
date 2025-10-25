use anyhow::anyhow;
use sqlx::{Acquire, Column, PgConnection, Row};
use tokio::runtime::Runtime;

use super::{Database, DatabaseInfo, Query, generate_id};
use crate::{
    QueryMessage,
    connections::{QueryResult, Value},
};

pub(crate) struct Postgres {
    conn: PgConnection,
    info: DatabaseInfo,
    rt: Runtime,
}

impl Postgres {
    pub(crate) fn new(conn: PgConnection, info: DatabaseInfo, rt: Runtime) -> Postgres {
        Postgres { conn, info, rt }
    }

    fn execute_write<F>(
        &mut self,
        query: &str,
        query_message: QueryMessage,
        map_result: F,
    ) -> anyhow::Result<()>
    where
        F: FnOnce(u64) -> QueryResult,
    {
        let mut tran = self
            .rt
            .block_on(self.conn.begin())
            .map_err(|_| anyhow!("failed to begin transaction"))?;

        let res = self
            .rt
            .block_on(sqlx::query(query).execute(&mut *tran))
            .map_err(|_| anyhow!("failed to execute query"))?;

        query_message
            .tx_result
            .send(map_result(res.rows_affected()))
            .map_err(|_| anyhow!("failed to send result"))?;

        // std::thread::sleep(std::time::Duration::from_secs(10));

        match query_message.rx_commit.recv() {
            Ok(_) => self
                .rt
                .block_on(tran.commit())
                .map_err(|_| anyhow!("failed to commit transaction"))?,
            Err(_) => self
                .rt
                .block_on(tran.rollback())
                .map_err(|_| anyhow!("transaction aborted"))?,
        }

        Ok(())
    }
}

impl Database for Postgres {
    type QueryType = String;

    fn query(&self, sql: &Query) -> Self::QueryType {
        match sql {
            Query::Select {
                table,
                fields,
                filter,
                ..
            } => {
                let (real_table, field_map) = &self.info[table];

                let real_fields = if fields[0] == "*" {
                    "*".to_string()
                } else {
                    fields
                        .iter()
                        .map(|field| field_map[field].name.as_str())
                        .collect::<Vec<_>>()
                        .join(",")
                };

                let mut query = format!("SELECT {} FROM {}", real_fields, real_table);

                if let Some(filter) = filter {
                    let col = &field_map[&filter.column];
                    query.push_str(&format!(
                        " WHERE {} {} '{}'",
                        col.name, filter.op, filter.value
                    ));
                }

                query
            }
            Query::Insert {
                table,
                columns,
                values,
            } => {
                let (real_table, field_map) = &self.info[table];

                let mut real_columns: Vec<&str> = vec![&field_map["IdCliente"].name];
                real_columns.extend(
                    columns
                        .iter()
                        .filter(|c| *c != "IdCliente")
                        .map(|c| field_map[c].name.as_str()),
                );

                let values: Vec<String> = values
                    .iter()
                    .map(|r| {
                        let mut row_values = Vec::new();

                        let id_value = generate_id();
                        row_values.push(id_value.to_string());

                        for (col, val) in columns.iter().zip(r.iter()) {
                            if col == "IdCliente" {
                                continue;
                            }
                            let f = &field_map[col];
                            let v = match f.r#type.as_str() {
                                "string" => format!("'{}'", val),
                                _ => val.to_string(),
                            };
                            row_values.push(v);
                        }

                        format!("({})", row_values.join(","))
                    })
                    .collect();

                let query = format!(
                    "INSERT INTO {} ({}) VALUES {}",
                    real_table,
                    real_columns.join(","),
                    values.join(",")
                );

                query
            }
            Query::Update {
                table,
                assignments,
                filter,
                ..
            } => {
                let (real_table, field_map) = &self.info[table];

                let assignments_str = assignments
                    .iter()
                    .map(|(col, val)| {
                        let real_col = &field_map[col];
                        format!("{} = {}", real_col.name, val)
                    })
                    .collect::<Vec<_>>()
                    .join(",");

                let mut query = format!("UPDATE {} SET {}", real_table, assignments_str);

                if let Some(filter) = filter {
                    let col = &field_map[&filter.column];
                    query.push_str(&format!(
                        " WHERE {} {} {}",
                        col.name, filter.op, filter.value
                    ));
                }

                query
            }
            Query::Delete { table, filter, .. } => {
                let (real_table, field_map) = &self.info[table];

                let mut query = format!("DELETE FROM {}", real_table);

                if let Some(filter) = filter {
                    let col = &field_map[&filter.column];
                    query.push_str(&format!(
                        " WHERE {} {} {}",
                        col.name, filter.op, filter.value
                    ));
                }

                query
            }
        }
    }

    fn execute(&mut self, query_message: QueryMessage) -> anyhow::Result<()> {
        let query = self.query(&query_message.query);

        match &*query_message.query {
            Query::Select { table, .. } => {
                let real_fields = &self.info[table].1;

                let rows = self
                    .rt
                    .block_on(sqlx::query(&query).fetch_all(&mut self.conn))?;

                let mut results = Vec::new();
                for row in rows {
                    let mut row_values = Vec::new();

                    for col in row.columns() {
                        let col_name = col.name();
                        let field = real_fields
                            .values()
                            .find(|v| v.name.to_lowercase() == col_name)
                            .unwrap();

                        let value = match field.r#type.as_str() {
                            "int" => match row.try_get::<i32, _>(col_name) {
                                Ok(v) => Value::Int(v as i64),
                                Err(_) => Value::Null,
                            },
                            "float" => match row.try_get::<f64, _>(col_name) {
                                Ok(v) => Value::Float(v),
                                Err(_) => Value::Null,
                            },
                            "bool" => match row.try_get::<bool, _>(col_name) {
                                Ok(v) => Value::Bool(v),
                                Err(_) => Value::Null,
                            },
                            "string" => match row.try_get::<String, _>(col_name) {
                                Ok(v) => Value::Str(v),
                                Err(_) => Value::Null,
                            },
                            _ => Value::Null,
                        };

                        row_values.push(value);
                    }

                    results.push(row_values);
                }

                query_message
                    .tx_result
                    .send(QueryResult::Select(results))
                    .unwrap();

                Ok(())
            }
            Query::Insert { .. } => self.execute_write(&query, query_message, QueryResult::Insert),
            Query::Update { .. } => self.execute_write(&query, query_message, QueryResult::Update),
            Query::Delete { .. } => self.execute_write(&query, query_message, QueryResult::Delete),
        }
    }
}
