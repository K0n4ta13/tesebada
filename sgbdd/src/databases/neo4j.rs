use std::collections::HashMap;

use anyhow::anyhow;
use neo4rs::{Graph, Query as QueryNeo4j};
use tokio::runtime::Runtime;

use crate::{
    QueryMessage, QueryResult, Value,
    databases::{DatabaseInfo, generate_id},
};

use super::{Database, Query};

pub(crate) struct Neo4J {
    conn: Graph,
    info: DatabaseInfo,
    rt: Runtime,
    wildcard: HashMap<String, Vec<String>>,
}

impl Neo4J {
    pub(crate) fn new(
        conn: Graph,
        info: DatabaseInfo,
        rt: Runtime,
        wildcard: HashMap<String, Vec<String>>,
    ) -> Neo4J {
        Neo4J {
            conn,
            info,
            rt,
            wildcard,
        }
    }

    fn execute_write<F>(
        &mut self,
        query: QueryNeo4j,
        query_message: QueryMessage,
        map_result: F,
    ) -> anyhow::Result<()>
    where
        F: FnOnce(u64) -> QueryResult,
    {
        let mut tran = self.rt.block_on(self.conn.start_txn())?;

        let mut result = self.rt.block_on(tran.execute(query))?;

        let mut affected_rows = 0;
        while let Ok(Some(row)) = self.rt.block_on(result.next(&mut tran)) {
            affected_rows += row.get::<u64>("affected_rows").unwrap_or(0);
        }

        query_message
            .tx_result
            .send(map_result(affected_rows))
            .map_err(|_| anyhow!("failed to send result"))?;

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

impl Database for Neo4J {
    type QueryType = QueryNeo4j;
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
                    self.wildcard[table]
                        .iter()
                        .map(|field| format!("n.{}", field_map[field].name.as_str()))
                        .collect::<Vec<_>>()
                        .join(", ")
                } else {
                    fields
                        .iter()
                        .map(|field| format!("n.{}", field_map[field].name.as_str()))
                        .collect::<Vec<_>>()
                        .join(", ")
                };

                let query = match filter {
                    Some(f) => {
                        let field = &field_map[&f.column];
                        let value = if field.r#type == "string" {
                            format!("\"{}\"", f.value)
                        } else {
                            f.value.clone()
                        };
                        format!(
                            "MATCH (n:{}) WHERE n.{} = {} RETURN {}",
                            real_table, field.name, value, real_fields
                        )
                    }
                    None => format!("MATCH (n:{}) RETURN {}", real_table, real_fields),
                };

                QueryNeo4j::new(query)
            }
            Query::Insert {
                table,
                columns,
                values,
            } => {
                let (real_table, field_map) = &self.info[table];

                let nodes: Vec<String> = values
                    .iter()
                    .map(|r| {
                        let mut node = Vec::new();

                        let id_field = &field_map["IdCliente"].name;
                        let id_value = generate_id();
                        node.push(format!("{}: {}", id_field, id_value));

                        for (col, val) in columns.iter().zip(r.iter()) {
                            if col == "IdCliente" {
                                continue;
                            }
                            let field = &field_map[col];
                            let part = if field.r#type == "string" {
                                format!("{}: '{}'", field.name, val)
                            } else {
                                format!("{}: {}", field.name, val)
                            };
                            node.push(part);
                        }

                        format!("{{{}}}", node.join(", "))
                    })
                    .collect();

                let query = format!(
                    "UNWIND [{}] AS row \
                     CREATE (n:{}) \
                     SET n = row \
                     RETURN count(n) as affected_rows",
                    nodes.join(", "),
                    real_table,
                );

                QueryNeo4j::new(query)
            }
            Query::Update {
                table,
                assignments,
                filter,
                ..
            } => {
                let (real_table, field_map) = &self.info[table];

                let assigns: Vec<String> = assignments
                    .iter()
                    .map(|(c, v)| {
                        let field = &field_map[c];
                        if field.r#type == "string" {
                            format!("n.{} = '{}'", field.name, v)
                        } else {
                            format!("n.{} = {}", field.name, v)
                        }
                    })
                    .collect();

                let mut query = match filter {
                    Some(f) => {
                        let field = &field_map[&f.column];
                        let value = if field.r#type == "string" {
                            format!("\"{}\"", f.value)
                        } else {
                            f.value.clone()
                        };
                        format!(
                            "MATCH (n:{}) WHERE n.{} = {} SET {}",
                            real_table,
                            field.name,
                            value,
                            assigns.join(", ")
                        )
                    }
                    None => format!("MATCH (n:{}) SET {}", real_table, assigns.join(", ")),
                };
                query.push_str(" RETURN count(n) as affected_rows");

                QueryNeo4j::new(query)
            }
            Query::Delete { table, filter, .. } => {
                let (real_table, field_map) = &self.info[table];

                let mut query = format!("MATCH (n:{})", real_table);

                if let Some(filter) = filter {
                    let col = &field_map[&filter.column];
                    let value = if col.r#type == "string" {
                        format!("\"{}\"", filter.value)
                    } else {
                        filter.value.clone()
                    };
                    query.push_str(&format!(" WHERE n.{} {} {}", col.name, filter.op, value));
                }
                query.push_str(" DELETE n RETURN count(n) as affected_rows");

                QueryNeo4j::new(query)
            }
        }
    }

    fn execute(&mut self, query_message: QueryMessage) -> anyhow::Result<()> {
        let query = self.query(&query_message.query);

        match &*query_message.query {
            Query::Select { table, fields, .. } => {
                let real_fields = &self.info[table].1;

                let (real_name_fields, fields): (Vec<_>, &Vec<_>) = if fields[0] == "*" {
                    (
                        self.wildcard[table]
                            .iter()
                            .map(|field| real_fields[field].name.as_str())
                            .collect(),
                        &self.wildcard[table],
                    )
                } else {
                    (
                        fields
                            .iter()
                            .map(|field| real_fields[field].name.as_str())
                            .collect(),
                        fields,
                    )
                };

                let mut res = self.rt.block_on(self.conn.execute(query))?;

                let mut results = Vec::new();
                while let Some(row) = self.rt.block_on(res.next())? {
                    let mut record = Vec::with_capacity(real_name_fields.len());

                    for (real_field, query_field) in real_name_fields.iter().zip(fields) {
                        let field = format!("n.{real_field}");
                        let value = match real_fields[query_field].r#type.as_str() {
                            "int" => match row.get::<i32>(&field) {
                                Ok(v) => Value::Int(v as i64),
                                Err(_) => Value::Null,
                            },
                            "float" => {
                                row.get::<f64>(&field)
                                    .map(Value::Float)
                                    .unwrap_or_else(|_| {
                                        row.get::<u32>(&field)
                                            .map(|v| Value::Float(v as f64))
                                            .unwrap_or(Value::Null)
                                    })
                            }
                            "bool" => match row.get::<bool>(&field) {
                                Ok(v) => Value::Bool(v),
                                Err(_) => Value::Null,
                            },
                            "string" => match row.get::<String>(&field) {
                                Ok(v) => Value::Str(v),
                                Err(_) => Value::Null,
                            },
                            _ => Value::Null,
                        };
                        record.push(value);
                    }
                    results.push(record);
                }

                query_message
                    .tx_result
                    .send(QueryResult::Select(results))
                    .map_err(|_| anyhow!("failed to send result"))?;

                Ok(())
            }
            Query::Insert { .. } => self.execute_write(query, query_message, QueryResult::Insert),
            Query::Update { .. } => self.execute_write(query, query_message, QueryResult::Update),
            Query::Delete { .. } => self.execute_write(query, query_message, QueryResult::Delete),
        }
    }
}
