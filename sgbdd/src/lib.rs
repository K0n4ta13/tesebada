#![allow(warnings)]

mod config;
mod connections;
mod cursor;
mod databases;
mod parser;
mod token;
mod validations;

use std::thread;

use connections::{
    DistributedMongoConnection, DistributedNeo4jConnection, DistributedPgConnection,
};
use cursor::Cursor;
use mongodb::sync::Client;
use neo4rs::Graph;
use parser::Parser;
use sqlx::{Connection, PgConnection};
use token::Token;

pub use config::Config;
pub use connections::{DistributedConnection, QueryMessage, QueryResult, Value};
pub use parser::{Query, Where};
pub use validations::{check_query, validate_config};

pub use databases::{load_next_id, save_next_id};

pub fn spawn_databases(config: &Config) -> Vec<Box<dyn DistributedConnection>> {
    let connections = databases::connections(config);

    let mut distributed_connections: Vec<Box<dyn DistributedConnection>> = Vec::new();
    for (manager, connection_url, zone) in connections {
        match manager {
            "postgres" => {
                let (tx, rx) = std::sync::mpsc::channel();

                let pg_connection =
                    DistributedPgConnection::new(tx, connection_url.to_string(), zone.to_string());
                distributed_connections.push(Box::new(pg_connection));

                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .build()
                    .unwrap();
                let conn = rt
                    .block_on(PgConnection::connect(connection_url))
                    .expect("unable to connect with the PostgreSQL databse");
                let info = databases::database_info(connection_url, config);

                let db = databases::Postgres::new(conn, info, rt);
                thread::spawn(move || databases::run_database(db, rx));
            }
            "mongo" => {
                let (tx, rx) = std::sync::mpsc::channel();

                let mongo_connection = DistributedMongoConnection::new(
                    tx,
                    connection_url.to_string(),
                    zone.to_string(),
                );
                distributed_connections.push(Box::new(mongo_connection));

                let client = Client::with_uri_str(connection_url)
                    .expect("unable to connect with the MongoDB database");
                let db_name = connection_url
                    .rsplit("/")
                    .next()
                    .unwrap()
                    .split("?")
                    .next()
                    .unwrap();

                let db = client.database(db_name);
                let info = databases::database_info(connection_url, config);

                let db = databases::Mongo::new(client, db, info);
                thread::spawn(move || databases::run_database(db, rx));
            }
            "neo4j" => {
                let (tx, rx) = std::sync::mpsc::channel();
                let neo4j_connection = DistributedNeo4jConnection::new(
                    tx,
                    connection_url.to_string(),
                    zone.to_string(),
                );
                distributed_connections.push(Box::new(neo4j_connection));

                let s = connection_url.strip_prefix("bolt://").unwrap();

                let parts: Vec<&str> = s.split('@').collect();
                let user_pass = parts[0];
                let uri = parts[1];

                let user_pass_parts: Vec<&str> = user_pass.split(':').collect();
                let user = user_pass_parts[0];
                let pass = user_pass_parts[1];

                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .build()
                    .unwrap();
                let conn = rt
                    .block_on(Graph::new(uri, user, pass))
                    .expect("unable to connect with the Neo4J databse");
                let info = databases::database_info(connection_url, config);
                let wildcard = databases::wildcard(connection_url, config);

                let db = databases::Neo4J::new(conn, info, rt, wildcard);
                thread::spawn(move || databases::run_database(db, rx));
            }
            manager => panic!("database manager '{manager} not supported'"),
        }
    }

    distributed_connections
}

fn tokenize(source: &str) -> impl Iterator<Item = anyhow::Result<Token>> {
    let mut cursor = Cursor::new(source);
    std::iter::from_fn(move || match cursor.advance_token() {
        Ok(Token::Eof) => None,
        Ok(token) => Some(Ok(token)),
        Err(e) => Some(Err(e)),
    })
}

pub fn parse_query(query: &str) -> anyhow::Result<Query> {
    let tokens: Vec<Token> = tokenize(query).collect::<anyhow::Result<_>>()?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}
