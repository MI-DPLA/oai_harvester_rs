use std::time::Instant;
use std::fs;
use reqwest::blocking::Client;
use xee_xpath::{Documents, Queries, Query};
use clap::error::ErrorKind;
use clap::{Parser, Subcommand};
use iri_string::types::{IriString};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "OAI Harvester")]
#[command(version, about, long_about = None)]
struct Args {
    /// OAI repository endpoint to harvest from
    repository: IriString,

    /// Verb to use
    #[command(subcommand)]
    verb: Verb,

    #[arg(short, long)]
    write: bool,
}

#[derive(Subcommand, Debug)]
enum Verb {
    /// List metadata formats available from this repository
    ListMetadataFormats,
    /// Get all records specified by metadataPrefix and, optionally, set
    ListRecords {
        metadata_prefix: String,
        #[arg(short, long)]
        set: Option<String>,
        #[arg(short, long)]
        from: Option<String>
    },
    /// Get the set structure of this repository
    ListSets,
}

fn main() {
    let args = Args::parse();
    // println!("args: {:?}", args);
    // https://www.sifet.org/bollettino/index.php/bollettinosifet/oai
    let repository = args.repository;
    let write = args.write;
    let client = Client::builder()
        .timeout(None)
        .build()
        .unwrap();

    match &args.verb {
        Verb::ListMetadataFormats => {
            println!("Listing metadata formats available from {}", repository);
            get_metadata_formats(client, repository, write);
        },
        Verb::ListRecords { metadata_prefix, set, from } => {
            println!("Harvesting records from {} using prefix {} from {}{}", repository, metadata_prefix, match set {
                Some(s) => format!("set {}", s),
                None => "all sets".to_string(),
            }, match from {
                Some(s) => format!(" starting from date {}", s),
                None => "".to_string(),
            });
            get_records(client, repository, metadata_prefix, set, from, write);
        },
        Verb::ListSets => {
            println!("Listing sets available from {}", repository);
            get_sets(client, repository, write);
        },
    };
}

fn write_result(filepath: &str, result: &str) {
    let path: PathBuf = PathBuf::from(filepath);
    fs::write(path.as_path(), result);
}

fn get_metadata_formats(client: Client, repository: IriString, write: bool) {
    let request_target = format!("{}?verb=ListMetadataFormats", repository.to_string());
    let request = client.get(request_target);
    let result = client.execute(request.build().unwrap()).unwrap().text().unwrap();
    if write {
        write_result("formats.xml", &result);
    }
}

fn get_sets(client: Client, repository: IriString, write: bool) {
    let request_target = format!("{}?verb=ListSets", repository.to_string());
    let request = client.get(request_target);
    let result = client.execute(request.build().unwrap()).unwrap().text().unwrap();
    if write {
        write_result("sets.xml", &result);
    }
}
fn get_records(client: Client, repository: IriString, prefix: &String, set: &Option<String>, from: &Option<String>, write: bool) {
    let now = Instant::now();
    let request_base = format!("{}?verb=ListRecords", repository.to_string());
    let request_target = format!("{}&metadataPrefix={}{}{}", request_base, prefix, match set {
        Some(s) => format!("&set={}", s),
        None => "".to_string(),
    }, match from {
        Some(s) => format!("&from={}", s),
        None => "".to_string(),
    });
    let request = client.get(request_target);
    let result = client.execute(request.build().unwrap()).unwrap().text().unwrap();
    let base_filename = format!("{}-{}", prefix, match set {
        Some(s) => s,
        None => "all",
    });
    // println!("{:?}", result);
    if write {
        let filename = format!("{}-0.xml", base_filename);
        write_result(&filename, &result);
    }
    let re = regex::Regex::new(r"resumptionToken.*>(.*)</resumptionToken").unwrap();
    let captures_opt = re.captures(&result);
    match captures_opt {
        Some(captures) => {
            let resumption_token = captures.get(1);
            match resumption_token {
                Some(token) => {
                    fetch_results(client, request_base, token.as_str().to_owned(), now, write, &base_filename)
                },
                None => {
                    println!("done1!")
                }
            };
        },
        None => {
            println!("done!")
        }
    };
}

fn fetch_results(client: Client, request_base: String, resumption_token: String, now: Instant, write: bool, base_filename: &str) {
    let elapsed = now.elapsed().as_secs();
    println!("{} fetching for token: {}", elapsed, resumption_token);
    let request = client.get(&format!("{}&resumptionToken={}", request_base, urlencoding::encode(&resumption_token)));
    let result = client.execute(request.build().unwrap()).unwrap().text().unwrap();
    //println!("{}", result);
    if write {
        let filename = format!("{}-{}.xml", base_filename, elapsed.to_string());
        write_result(&filename, &result);
    }
    let re = regex::Regex::new(r"resumptionToken.*>(.*)</resumptionToken").unwrap();
    let captures = re.captures(&result);
    //println!("{:?}", captures);
    match captures {
        Some(c) => {
            let token = c.get(1);
            match token {
                Some(t) => {
                    fetch_results(client, request_base, t.as_str().to_owned(), now, write, base_filename)
                },
                None => {
                    println!("done2!")
                }
            }
        },
        None => {
            println!("done3!")
        }
    }
}
