use monkeydb::server::ConcObj;
use monkeydb::sql::SQLdb;
use std::collections::HashMap;
use std::net;

use std::fs::File;
use std::io::{prelude::*, BufReader};
use lazy_static::lazy_static;
use regex::Regex;

use std::sync::{Arc, Condvar, Mutex};

use msql_srv::MysqlIntermediary;

use std::thread;

use clap::Parser;

#[derive(Parser)]
struct Opts {
    #[clap(short, long, default_value = "3306")]
    anyport: u16,

    // ISOPredict
    #[clap(short, long, default_value = "")]
    trace: String,

    // ISOPredict
    #[clap(short, long, default_value = "3")]
    nodes: u64,
}

fn main() {
    let opts: Opts = Opts::parse();

    // listen to sql default port 3306
    let listener = net::TcpListener::bind(format!("127.0.0.1:{}", opts.anyport)).unwrap();

    let port = listener.local_addr().unwrap().port();

    // ISOPredict
    let mut traces: HashMap<u64, Vec<(u64, u64, u64)>> = HashMap::new();
    let mut tx_reads: HashMap<u64, Vec<usize>> = HashMap::new();
    let mut co: Vec<(u64, u64)> = Vec::new();

    println!("port:{}", port);

    // ISOPredict
    println!("trace: {}", opts.trace);
    println!("nodes: {}", opts.nodes);
    if !opts.trace.is_empty() {
        lazy_static! {
            static ref RE_READ: Regex = Regex::new(r"KEY\[(.+?)\].*Txn\((.+?)\) From\((.+?)\)").unwrap();
            static ref RE_WRITE: Regex = Regex::new(r"KEY\[(.+?)\] Txn\((.+?)\)").unwrap();
            static ref RE_INSERT: Regex = Regex::new(r"INSERT\[(.+?)\] to Set\[(.+?)\] Txn\((.+?)\)").unwrap();
            static ref RE_CONTAINS: Regex = Regex::new(r"CONTAINS\[(.+?)\] in Set\[(.+?)\] From\((.+?)\) Txn\((.+?)\)").unwrap();
            static ref RE_DELETE: Regex = Regex::new(r"DELETE\[(.+?)\] from Set\[(.+?)\] Txn\((.+?)\)").unwrap();
            static ref RE_TX: Regex = Regex::new(r"(.+?), (.+?)").unwrap();
            static ref RE_SET: Regex = Regex::new(r"Set\((.+?):(.+?)\)").unwrap();
        }

        let file = match File::open(opts.trace) {
            Ok(file) => file,
            Err(err) => {
                println!("Cannot open trace file: {}", err);
                std::process::exit(1);
            }
        };

        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line_str = line.unwrap().clone();
            if RE_READ.is_match(&line_str) {
                let cap = RE_READ.captures(&line_str).unwrap();
                let cap_tx = RE_TX.captures(&cap[2]).unwrap();
                let cap_from = RE_TX.captures(&cap[3]).unwrap();

                let mut key = cap[1].to_string();
                if RE_SET.is_match(&cap[1]) {
                    let cap_key = RE_SET.captures(&cap[1]).unwrap();
                    key = cap_key[2].to_string();
                }

                let session = cap_tx[1].parse::<u64>().unwrap();
                let tx = cap_tx[2].parse::<u64>().unwrap();
                let tx_size = cap_tx[2].parse::<usize>().unwrap();
                let write_session = cap_from[1].parse::<u64>().unwrap();
                let write_tx = cap_from[2].parse::<u64>().unwrap();
                let var = key.parse::<u64>().unwrap();

                let trace = traces.entry(session).or_default();
                let reads_vec = tx_reads.entry(session).or_default();
                
                // skip local reads in the trace
                if write_session != session || write_tx != tx {
                    trace.push((write_session, write_tx, var));

                    while reads_vec.len() <= tx_size {
                        reads_vec.push(0);
                    }

                    
                    reads_vec[tx_size] += 1;
                    
                }

                if session == 0 {
                    continue;
                }

                if let Some(prev) = co.last() {
                    if *prev != (session, tx) {
                        co.push((session, tx));
                    }
                } else {
                    co.push((session, tx));
                }

                // println!("READ KEY[{}] TX[Session{} T{}] FROM[Session{} T{}]", &cap[1], session, tx, write_session, write_tx);

                continue;
            }

            if RE_WRITE.is_match(&line_str) {
                let cap = RE_WRITE.captures(&line_str).unwrap();
                let cap_tx = RE_TX.captures(&cap[2]).unwrap();

                let session = cap_tx[1].parse::<u64>().unwrap();
                let tx = cap_tx[2].parse::<u64>().unwrap();

                if session == 0 {
                    continue;
                }

                if let Some(prev) = co.last() {
                    if *prev != (session, tx) {
                        co.push((session, tx));
                    }
                } else {
                    co.push((session, tx));
                }

                // println!("WRITE KEY[{}] TX[{}]", &cap[1], &cap[2]);
                continue;
            }

            if RE_INSERT.is_match(&line_str) {
                let cap = RE_INSERT.captures(&line_str).unwrap();
                let cap_tx = RE_TX.captures(&cap[3]).unwrap();

                let session = cap_tx[1].parse::<u64>().unwrap();
                let tx = cap_tx[2].parse::<u64>().unwrap();

                if session == 0 {
                    continue;
                }

                if let Some(prev) = co.last() {
                    if *prev != (session, tx) {
                        co.push((session, tx));
                    }
                } else {
                    co.push((session, tx));
                }
                
                // println!("INSERT SET[{}] KEY[{}] TX[{}]", &cap[2], &cap[1], &cap[3]);
                continue;
            }

            if RE_CONTAINS.is_match(&line_str) {
                let cap = RE_CONTAINS.captures(&line_str).unwrap();
                let cap_tx = RE_TX.captures(&cap[4]).unwrap();
                let cap_from = RE_TX.captures(&cap[3]).unwrap();

                let mut key = cap[1].to_string();
                if RE_SET.is_match(&cap[1]) {
                    let cap_key = RE_SET.captures(&cap[1]).unwrap();
                    key = cap_key[2].to_string();
                }

                let session = cap_tx[1].parse::<u64>().unwrap();
                let tx = cap_tx[2].parse::<u64>().unwrap();
                let tx_size = cap_tx[2].parse::<usize>().unwrap();
                let write_session = cap_from[1].parse::<u64>().unwrap();
                let write_tx = cap_from[2].parse::<u64>().unwrap();
                let var = key.parse::<u64>().unwrap();

                // skip local reads in the trace
                if write_session != session || write_tx != tx {
                    let trace = traces.entry(session).or_default();
                    let reads_vec = tx_reads.entry(session).or_default();
                    
                    trace.push((write_session, write_tx, var));
                    
                    while reads_vec.len() <= tx_size {
                        reads_vec.push(0);
                    }

                    
                    reads_vec[tx_size] += 1;
                }

                if session == 0 {
                    continue;
                }

                if let Some(prev) = co.last() {
                    if *prev != (session, tx) {
                        co.push((session, tx));
                    }
                } else {
                    co.push((session, tx));
                }

                // println!("CONTAINS SET[{}] KEY[{}] TX[{}]", &cap[2], &cap[1], &cap[3]);

                continue;
            }

            if RE_DELETE.is_match(&line_str) {
                let cap = RE_DELETE.captures(&line_str).unwrap();
                let cap_tx = RE_TX.captures(&cap[3]).unwrap();

                let session = cap_tx[1].parse::<u64>().unwrap();
                let tx = cap_tx[2].parse::<u64>().unwrap();

                if session == 0 {
                    continue;
                }

                if let Some(prev) = co.last() {
                    if *prev != (session, tx) {
                        co.push((session, tx));
                    }
                } else {
                    co.push((session, tx));
                }
                
                // println!("DELETE SET[{}] KEY[{}] TX[{}]", &cap[2], &cap[1], &cap[3]);
                continue;
            }
        }
    }

    let shared_obj = Arc::new((Mutex::new(SQLdb::default()), Condvar::new()));

    let mut sessions = Vec::new();

    let mut curr_session_id = 0;

    // ISOPredict
    {
        let (sqldb_mutex, _cvar) = &*shared_obj;
        let mut sqldb = sqldb_mutex.lock().unwrap();

        sqldb.nodes = opts.nodes;

        // load traces
        for (s_id, trace) in &traces {
            sqldb.traces.insert(*s_id, trace.to_vec());
        }

        // load tx_reads
        for (s_id, reads) in &tx_reads {
            sqldb.tx_reads.insert(*s_id, reads.to_vec());
        }
        
        // load commit order vector
        if co.len() > 0 {
            sqldb.co = co.to_vec();
        }
    }

    while let Ok((stream, _)) = listener.accept() {
        println!("new connection");
        curr_session_id += 1;
        let obj = shared_obj.clone();
        let t = thread::spawn(move || {
            let obj = ConcObj::from_sqldb(curr_session_id, obj);
            MysqlIntermediary::run_on_tcp(obj, stream).unwrap();
            
            println!("serving done");
        });
        sessions.push(t);
    }

    sessions.drain(..).for_each(|jh| jh.join().unwrap());
}
