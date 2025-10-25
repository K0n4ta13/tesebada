use std::{
    collections::{HashMap, LinkedList},
    fs::File,
    hash::{DefaultHasher, Hash, Hasher},
    io::{BufRead, Write},
};

use chrono::NaiveDate;

#[derive(Debug)]
struct Record {
    red: String,
    client_name: String,
    country: String,
    application_date: NaiveDate,
    employee_name: String,
    placed: String,
    card_id: u32,
    target: u32,
    gender: String,
}

impl Record {
    fn from_str(s: &str) -> Self {
        let parts = s.split(',').collect::<Vec<_>>();
        Self {
            red: parts[0].to_string(),
            client_name: parts[1].to_string(),
            country: parts[2].to_string(),
            application_date: normalize_date(parts[3]),
            employee_name: parts[4].to_string(),
            placed: parts[5].to_string(),
            card_id: parts[6].parse().unwrap(),
            target: parts[7].parse().unwrap(),
            gender: parts[8].to_string(),
        }
    }
}

impl std::fmt::Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{},{},{},{},{},{},{}",
            self.red,
            self.client_name,
            self.country,
            self.application_date.format("%-d/%-m/%-Y"),
            self.employee_name,
            self.placed,
            self.card_id,
            self.target,
            self.gender
        )
    }
}

fn main() {
    let file = std::fs::File::open("solicitudes.csv").unwrap();
    let file = std::io::BufReader::new(file);
    let mut file = file.lines();

    let mut seen: HashMap<u128, (String, HashMap<NaiveDate, u32>)> = HashMap::new();

    let headers = file.next().map(|r| r.unwrap()).unwrap();

    let data = file
        .map(|row| row.unwrap())
        .map(|row| Record::from_str(&row))
        .map(|row| {
            let hash: u128 = row
                .employee_name
                .split(' ')
                .map(|s| {
                    let mut hasher = DefaultHasher::new();
                    s.hash(&mut hasher);
                    hasher.finish() as u128
                })
                .sum();
            seen.entry(hash)
                .and_modify(|(name, targets)| {
                    targets
                        .entry(row.application_date)
                        .and_modify(|target| {
                            if *target < row.target {
                                *name = row.employee_name.clone();
                                *target = row.target
                            }
                        })
                        .or_insert(row.target);
                })
                .or_insert_with(|| {
                    (
                        row.employee_name.clone(),
                        HashMap::from([(row.application_date, row.target)]),
                    )
                });
            (hash, row)
        })
        .collect::<LinkedList<(_, _)>>();
    let data = data
        .into_iter()
        .map(|(hash, mut row)| {
            let (name, targets) = seen.get(&hash).unwrap();
            if row.employee_name != *name {
                row.employee_name = name.clone();
            }
            let target = targets.get(&row.application_date).unwrap();
            if row.target != *target {
                row.target = *target;
            }
            row
        })
        .collect::<LinkedList<_>>();

    let mut output_file = File::create("clean.csv").unwrap();
    writeln!(output_file, "{headers}").unwrap();
    data.iter()
        .for_each(|r| writeln!(output_file, "{r}").unwrap());
}

fn normalize_date(date: &str) -> NaiveDate {
    let parts = date.split('/').collect::<Vec<_>>();
    let mut day = parts[0].parse::<u32>().unwrap();
    let month = parts[1].parse::<u32>().unwrap();
    let year = parts[2].parse::<i32>().unwrap();

    loop {
        match NaiveDate::from_ymd_opt(year, month, day) {
            Some(date) => return date,
            None => day -= 1,
        }
    }
}
