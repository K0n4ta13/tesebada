use std::sync::{
    Arc,
    mpsc::{Receiver, Sender},
};

use super::Query;

pub trait DistributedConnection {
    fn execute_query(&self, query: QueryMessage);
    fn zone(&self) -> &str;
}

#[derive(Debug)]
pub struct QueryMessage {
    pub(crate) query: Arc<Query>,
    pub(crate) tx_result: Sender<QueryResult>,
    pub(crate) rx_commit: Receiver<()>,
}

impl QueryMessage {
    pub fn new(
        query: Arc<Query>,
        tx_result: Sender<QueryResult>,
        rx_commit: Receiver<()>,
    ) -> QueryMessage {
        QueryMessage {
            query,
            tx_result,
            rx_commit,
        }
    }
}

#[derive(Debug)]
pub enum Value {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Null,
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::Str(v) => write!(f, "{}", v),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Null => write!(f, "null"),
        }
    }
}

#[derive(Debug)]
pub enum QueryResult {
    Select(Vec<Vec<Value>>),
    Insert(u64),
    Update(u64),
    Delete(u64),
}

macro_rules! distributed_connection {
    ($name:ident) => {
        pub(crate) struct $name {
            tx_query: Sender<QueryMessage>,
            conn: String,
            zone: String,
        }

        impl $name {
            pub(crate) fn new(tx_query: Sender<QueryMessage>, conn: String, zone: String) -> $name {
                $name {
                    tx_query,
                    conn,
                    zone,
                }
            }
        }

        impl DistributedConnection for $name {
            fn execute_query(&self, query: QueryMessage) {
                self.tx_query.send(query).unwrap();
            }

            fn zone(&self) -> &str {
                &self.zone
            }
        }
    };
}

distributed_connection!(DistributedPgConnection);
distributed_connection!(DistributedMongoConnection);
distributed_connection!(DistributedNeo4jConnection);
