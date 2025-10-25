use anyhow::anyhow;
use mongodb::{
    bson::{Bson, Document, doc},
    sync::{Client, Database as MongoDatabase},
};

use crate::{
    QueryResult, Value,
    databases::{DatabaseInfo, generate_id},
};

use super::{Database, Query};

pub(crate) struct Mongo {
    client: Client,
    db: MongoDatabase,
    info: DatabaseInfo,
}

impl Mongo {
    pub(crate) fn new(client: Client, db: MongoDatabase, info: DatabaseInfo) -> Mongo {
        Mongo { client, db, info }
    }
}

pub(crate) enum DocumentType {
    Select((Document, Document)),
    Insert(Vec<Document>),
    Update((Document, Document)),
    Delete(Document),
}

impl Database for Mongo {
    type QueryType = DocumentType;

    fn query(&self, sql: &Query) -> Self::QueryType {
        match sql {
            Query::Select {
                table,
                fields,
                filter,
                ..
            } => {
                let real_fields = &self.info[table].1;

                let project_doc = if fields[0] == "*" {
                    Document::new()
                } else {
                    fields
                        .iter()
                        .map(|field| (real_fields[field].name.to_string(), Bson::Int64(1)))
                        .collect()
                };

                let filter_doc = filter
                    .as_ref()
                    .map(|f| doc! { &real_fields[&f.column].name: &f.value })
                    .unwrap_or(Document::new());

                DocumentType::Select((filter_doc, project_doc))
            }
            Query::Insert {
                table,
                columns,
                values,
            } => {
                let real_fields = &self.info[table].1;

                let docs: Vec<Document> = values
                    .iter()
                    .map(|row| {
                        let mut doc = Document::new();

                        let id_field = &real_fields["IdCliente"].name;
                        let id_value = generate_id();
                        doc.insert(id_field, id_value.to_string());

                        for (col, val) in columns.iter().zip(row) {
                            if col == "IdCliente" {
                                continue; // saltar si ya existe
                            }
                            doc.insert(&real_fields[col].name, val);
                        }

                        doc
                    })
                    .collect();

                DocumentType::Insert(docs)
            }
            Query::Update {
                table,
                assignments,
                filter,
                ..
            } => {
                let real_fields = &self.info[table].1;
                let mut update_doc = Document::new();
                for (col, val) in assignments {
                    update_doc.insert(&real_fields[col].name, val);
                }

                let filter_doc = filter
                    .as_ref()
                    .map(|f| doc! { &real_fields[&f.column].name: &f.value })
                    .unwrap_or(Document::new());

                DocumentType::Update((filter_doc, doc! { "$set": update_doc }))
            }
            Query::Delete { table, filter, .. } => {
                let real_fields = &self.info[table].1;
                let doc = filter
                    .as_ref()
                    .map(|f| doc! { &real_fields[&f.column].name: &f.value })
                    .unwrap_or(Document::new());

                DocumentType::Delete(doc)
            }
        }
    }

    fn execute(&mut self, query_message: crate::QueryMessage) -> anyhow::Result<()> {
        let query = self.query(&query_message.query);

        match (&*query_message.query, query) {
            (Query::Select { table, .. }, DocumentType::Select((filter_doc, project_doc))) => {
                let (table_real_name, real_fields) = &self.info[table];

                let collection = self.db.collection::<Document>(table_real_name.as_str());
                let res = collection.find(filter_doc).projection(project_doc).run()?;

                let mut results = Vec::new();
                for doc in res {
                    let doc = doc?;
                    let mut row_values = Vec::new();

                    for (name, value) in doc {
                        if name == "_id" {
                            continue;
                        }
                        let field = real_fields.values().find(|f| f.name == *name).unwrap();

                        let value = match field.r#type.as_str() {
                            "int" => match value.as_str().unwrap().parse() {
                                Ok(v) => Value::Int(v),
                                Err(_) => Value::Null,
                            },
                            "float" => match value.as_str().unwrap().parse() {
                                Ok(v) => Value::Float(v),
                                Err(_) => Value::Null,
                            },
                            "bool" => match value.as_bool() {
                                Some(v) => Value::Bool(v),
                                None => Value::Null,
                            },
                            "string" => match value.as_str() {
                                Some(v) => Value::Str(v.to_string()),
                                None => Value::Null,
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
                    .map_err(|_| anyhow!("failed to send result"))?;

                Ok(())
            }
            (Query::Insert { table, .. }, DocumentType::Insert(docs)) => {
                let table_real_name = &self.info[table].0;
                let mut session = self
                    .client
                    .start_session()
                    .run()
                    .map_err(|_| anyhow!("failed to start session"))?;
                session.start_transaction().run()?;

                let collection = self.db.collection::<Document>(table_real_name.as_str());

                let res = collection.insert_many(docs).session(&mut session).run()?;

                query_message
                    .tx_result
                    .send(QueryResult::Insert(res.inserted_ids.len() as u64))
                    .map_err(|_| anyhow!("failed to send result"))?;

                match query_message.rx_commit.recv() {
                    Ok(_) => session
                        .commit_transaction()
                        .run()
                        .map_err(|_| anyhow!("failed to commit transaction"))?,
                    Err(_) => session
                        .abort_transaction()
                        .run()
                        .map_err(|_| anyhow!("transaction aborted"))?,
                }

                Ok(())
            }
            (Query::Update { table, .. }, DocumentType::Update((filter_doc, update_doc))) => {
                let table_real_name = &self.info[table].0;

                let mut session = self
                    .client
                    .start_session()
                    .run()
                    .map_err(|_| anyhow!("failed to start session"))?;
                session
                    .start_transaction()
                    .run()
                    .map_err(|_| anyhow!("failed to start transaccion"))?;

                let collection = self.db.collection::<Document>(table_real_name.as_str());

                let res = collection
                    .update_many(filter_doc, update_doc)
                    .session(&mut session)
                    .run()?;

                query_message
                    .tx_result
                    .send(QueryResult::Update(res.modified_count))
                    .map_err(|_| anyhow!("failed to send result"))?;

                match query_message.rx_commit.recv() {
                    Ok(_) => session
                        .commit_transaction()
                        .run()
                        .map_err(|_| anyhow!("failed to commit transaction"))?,
                    Err(_) => session
                        .abort_transaction()
                        .run()
                        .map_err(|_| anyhow!("transaction aborted"))?,
                }

                Ok(())
            }
            (Query::Delete { table, .. }, DocumentType::Delete(filter_doc)) => {
                let table_real_name = &self.info[table].0;

                let mut session = self
                    .client
                    .start_session()
                    .run()
                    .map_err(|_| anyhow!("failed to start session"))?;
                session
                    .start_transaction()
                    .run()
                    .map_err(|_| anyhow!("failed to start transaccion"))?;

                let collection = self.db.collection::<Document>(table_real_name.as_str());

                let res = collection
                    .delete_many(filter_doc)
                    .session(&mut session)
                    .run()?;

                query_message
                    .tx_result
                    .send(QueryResult::Delete(res.deleted_count))
                    .map_err(|_| anyhow!("failed to send result"))?;

                match query_message.rx_commit.recv() {
                    Ok(_) => session
                        .commit_transaction()
                        .run()
                        .map_err(|_| anyhow!("failed to commit transaction"))?,
                    Err(_) => session
                        .abort_transaction()
                        .run()
                        .map_err(|_| anyhow!("transaction aborted"))?,
                }

                Ok(())
            }
            (_, _) => unreachable!(),
        }
    }
}
