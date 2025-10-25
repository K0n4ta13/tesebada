#![allow(warnings)]

use anyhow::anyhow;
use sgbdd::{
    Config, DistributedConnection, Query, QueryMessage, QueryResult, Where, parse_query,
    validate_config,
};
use std::{
    collections::HashSet,
    fmt::Display,
    io::Write,
    sync::{
        Arc, LazyLock,
        mpsc::{Receiver, RecvTimeoutError, Sender},
    },
};
pub static NORTH: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "Baja California",
        "Baja California Sur",
        "Sonora",
        "Chihuahua",
        "Coahuila de Zaragoza",
        "Nuevo León",
        "Tamaulipas",
        "Durango",
        "Sinaloa",
    ]
    .into_iter()
    .collect()
});

pub static CENTER: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "Aguascalientes",
        "Zacatecas",
        "San Luis Potosí",
        "Nayarit",
        "Jalisco",
        "Colima",
        "Michoacán de Ocampo",
        "Guanajuato",
        "Querétaro",
        "Hidalgo",
        "México",
        "Ciudad de México",
        "Tlaxcala",
        "Puebla",
        "Morelos",
    ]
    .into_iter()
    .collect()
});

pub static SOUTH: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "Guerrero",
        "Oaxaca",
        "Chiapas",
        "Veracruz de Ignacio de la Llave",
        "Tabasco",
        "Campeche",
        "Yucatán",
        "Quintana Roo",
    ]
    .into_iter()
    .collect()
});

fn main() -> anyhow::Result<()> {
    let config_file = std::fs::read_to_string("schema.toml")?;
    let config: Config = toml::from_str(&config_file)?;

    if validate_config(&config) {
        return Err(anyhow!("bad config"));
    }

    sgbdd::load_next_id("./id")?;
    run(config);
    sgbdd::save_next_id("./id")?;

    Ok(())
}

fn run(config: Config) {
    let databases = sgbdd::spawn_databases(&config);
    run_prompt(databases, config);
}

fn run_prompt(databases: Vec<Box<dyn DistributedConnection>>, config: Config) {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut sql = String::new();

    'main_loop: loop {
        write!(stdout, "> ").unwrap();
        stdout.flush().unwrap();
        sql.clear();
        let query = match stdin.read_line(&mut sql) {
            Ok(0) => break,
            Ok(_) => {
                let query = match parse_query(&sql) {
                    Ok(query) => Arc::new(query),
                    Err(message) => {
                        report(message);
                        continue;
                    }
                };
                if let Err(message) = sgbdd::check_query(&query, &config) {
                    report(message);
                    continue;
                }

                query
            }
            Err(_) => {
                report("error");
                break;
            }
        };

        let mut results = Vec::new();
        let mut rxs_result = Vec::new();
        let mut txs_commit = Vec::new();

        execute_query(&databases, query.clone(), &mut rxs_result, &mut txs_commit).unwrap();

        for rx_result in rxs_result {
            match rx_result.recv_timeout(std::time::Duration::from_secs(5)) {
                Ok(res) => results.push(res),
                Err(RecvTimeoutError::Timeout) => {
                    report("timeout, query canceled");
                    continue 'main_loop;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    report("failed to execute, query canceled");
                    continue 'main_loop;
                }
            }
        }

        match *query {
            Query::Select { .. } => (),
            _ => commit(txs_commit),
        }

        show_result(results);
    }
}

fn execute_query(
    databases: &[Box<dyn DistributedConnection>],
    query: Arc<Query>,
    rxs_result: &mut Vec<Receiver<QueryResult>>,
    txs_commit: &mut Vec<Sender<()>>,
) -> anyhow::Result<()> {
    match &*query {
        Query::Insert {
            table,
            columns,
            values,
        } => {
            let state_idx = columns.iter().position(|c| c == "Estado").unwrap();

            let mut north = Vec::new();
            let mut center = Vec::new();
            let mut south = Vec::new();

            for row in values {
                let state = &row[state_idx];
                if NORTH.contains(state.as_str()) {
                    north.push(row.clone());
                } else if CENTER.contains(&state.as_str()) {
                    center.push(row.clone());
                } else if SOUTH.contains(&state.as_str()) {
                    south.push(row.clone());
                } else {
                    return Err(anyhow!("unknown state '{state}'"));
                }
            }
            for db in databases {
                let zone = db.zone();
                let query = match zone {
                    "Norte" => Query::Insert {
                        table: table.clone(),
                        columns: columns.clone(),
                        values: north.clone(),
                    },
                    "Centro" => Query::Insert {
                        table: table.clone(),
                        columns: columns.clone(),
                        values: center.clone(),
                    },
                    "Sur" => Query::Insert {
                        table: table.clone(),
                        columns: columns.clone(),
                        values: south.clone(),
                    },
                    zone => return Err(anyhow!("unknown zone '{zone}'")),
                };
                let query = Arc::new(query);
                let (tx_result, rx_result) = std::sync::mpsc::channel();
                rxs_result.push(rx_result);
                let (tx_commit, rx_commit) = std::sync::mpsc::channel();
                txs_commit.push(tx_commit);

                let query_message = QueryMessage::new(query, tx_result, rx_commit);

                db.execute_query(query_message);
            }
        }
        Query::Select { zones, filter, .. }
        | Query::Update { zones, filter, .. }
        | Query::Delete { zones, filter, .. } => {
            let dbs = select_databases(databases, zones, filter)?;
            for db in dbs {
                let query = Arc::clone(&query);

                let (tx_result, rx_result) = std::sync::mpsc::channel();
                rxs_result.push(rx_result);
                let (tx_commit, rx_commit) = std::sync::mpsc::channel();
                txs_commit.push(tx_commit);

                let query_message = QueryMessage::new(query, tx_result, rx_commit);
                db.execute_query(query_message);
            }
        }
    }

    Ok(())
}

fn select_databases<'a>(
    databases: &'a [Box<dyn DistributedConnection>],
    zones: &Option<Vec<String>>,
    filter: &Option<Where>,
) -> anyhow::Result<Vec<&'a Box<dyn DistributedConnection>>> {
    if let Some(zones) = zones {
        Ok(databases
            .iter()
            .filter(|db| zones.contains(&db.zone().to_string()))
            .collect())
    } else if let Some(filter) = filter {
        if filter.column == "Estado" && filter.op == "=" {
            let state = &filter.value;
            let zone = if NORTH.contains(state.as_str()) {
                "Norte"
            } else if CENTER.contains(state.as_str()) {
                "Centro"
            } else if SOUTH.contains(state.as_str()) {
                "Sur"
            } else {
                return Err(anyhow::anyhow!("unknown state '{state}'"));
            };
            Ok(databases.iter().filter(|db| db.zone() == zone).collect())
        } else {
            Ok(databases.iter().collect())
        }
    } else {
        Ok(databases.iter().collect())
    }
}

fn commit(txs_commit: Vec<Sender<()>>) {
    for tx_commit in txs_commit {
        tx_commit.send(()).unwrap();
    }
}

fn report(message: impl Display) {
    println!("\n{message}\n")
}

fn show_result(result: Vec<QueryResult>) {
    use std::fmt::Write;

    if result.is_empty() {
        return;
    }

    match &result[0] {
        QueryResult::Select(_) => {
            for res in result {
                if let QueryResult::Select(rows) = res {
                    let mut printable = String::new();
                    for row in rows {
                        writeln!(
                            printable,
                            "{}",
                            row.iter()
                                .map(|v| v.to_string())
                                .collect::<Vec<_>>()
                                .join(",")
                        )
                        .unwrap();
                    }
                    print!("{printable}");
                }
            }
        }
        QueryResult::Insert(_) => {
            let total: u64 = result
                .into_iter()
                .map(|r| if let QueryResult::Insert(n) = r { n } else { 0 })
                .sum();
            println!("\nrows inserted: {total}\n");
        }
        QueryResult::Update(_) => {
            let total: u64 = result
                .into_iter()
                .map(|r| if let QueryResult::Update(n) = r { n } else { 0 })
                .sum();
            println!("\nrows updated: {total}\n");
        }
        QueryResult::Delete(_) => {
            let total: u64 = result
                .into_iter()
                .map(|r| if let QueryResult::Delete(n) = r { n } else { 0 })
                .sum();
            println!("\nrows deleted: {total}\n");
        }
    }
}
