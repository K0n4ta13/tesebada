mod mongo;
mod neo4j;
mod postgres;

use crate::{QueryMessage, config::FragmentField};

use super::{Config, Query};
use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{self, Write},
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::Receiver,
    },
};

pub(crate) use mongo::Mongo;
pub(crate) use neo4j::Neo4J;
pub(crate) use postgres::Postgres;

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

pub fn load_next_id<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let value = match fs::read_to_string(&path) {
        Ok(contents) => contents.trim().parse::<u64>().unwrap_or(1),
        Err(e) if e.kind() == io::ErrorKind::NotFound => 1,
        Err(e) => return Err(e),
    };
    NEXT_ID.store(value, Ordering::SeqCst);

    Ok(())
}

pub fn save_next_id<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let value = NEXT_ID.load(Ordering::SeqCst);
    let mut file = fs::File::create(path)?;
    writeln!(file, "{}", value)?;

    Ok(())
}

pub(crate) fn generate_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::SeqCst)
}

pub trait Database {
    type QueryType;

    fn query(&self, sql: &Query) -> Self::QueryType;
    fn execute(&mut self, query_message: QueryMessage) -> anyhow::Result<()>;
}

pub(crate) fn connections(config: &Config) -> HashSet<(&str, &str, &str)> {
    config
        .tables
        .iter()
        .flat_map(|table| {
            table
                .fragments
                .iter()
                .map(|fragment| (&*fragment.manager, &*fragment.connection, &*fragment.zone))
        })
        .collect()
}

type DatabaseInfo = HashMap<String, (String, HashMap<String, FragmentField>)>;

pub(crate) fn database_info(connection: &str, config: &Config) -> DatabaseInfo {
    config
        .tables
        .iter()
        .filter_map(|table| {
            table
                .fragments
                .iter()
                .find(|fragment| fragment.connection == connection)
                .map(|fragment| {
                    (
                        table.name.clone(),
                        (
                            fragment.name.clone(),
                            fragment
                                .fields
                                .iter()
                                .map(|field| {
                                    (
                                        field.reference.clone(),
                                        fragment
                                            .fields
                                            .iter()
                                            .find(|f| f.reference == field.reference)
                                            .unwrap()
                                            .clone(),
                                    )
                                })
                                .collect::<HashMap<String, FragmentField>>(),
                        ),
                    )
                })
        })
        .collect()
}

pub(crate) fn wildcard(connection: &str, config: &Config) -> HashMap<String, Vec<String>> {
    config
        .tables
        .iter()
        .filter(|table| {
            table
                .fragments
                .iter()
                .any(|fragment| fragment.connection == connection)
        })
        .map(|table| {
            (
                table.name.clone(),
                table
                    .fields
                    .iter()
                    .map(|field| field.name.clone())
                    .collect(),
            )
        })
        .collect()
}

pub(crate) fn run_database(mut db: impl Database, rx: Receiver<QueryMessage>) {
    while let Ok(query_message) = rx.recv() {
        // if let Err(message) = db.execute(query_message) {
        //     dbg!(message);
        // }
        let _ = db.execute(query_message);
    }
}
