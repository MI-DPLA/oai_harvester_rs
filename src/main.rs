use std::time::Instant;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::PathBuf;
use reqwest::blocking::Client;
use xee_xpath::{Documents, Itemable, Queries, Query, Sequence};
use xee_xpath::context::{StaticContextBuilder};
use clap::{Parser, Subcommand};
use iri_string::types::{IriString};

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
        from: Option<String>,
        #[arg(short, long)]
        until: Option<String>,
    },
    /// Get the set structure of this repository
    ListSets,
    TestXpath {
        infile: PathBuf,
    },
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
        Verb::ListRecords { metadata_prefix, set, from, until } => {
            println!("Harvesting records from {} using prefix {} from {}{}{}", repository, metadata_prefix, match set {
                Some(s) => format!("set {}", s),
                None => "all sets".to_string(),
            }, match from {
                Some(s) => format!(" starting from date {}", s),
                None => "".to_string(),
            }, match until {
                Some(s) => format!(" until date {}", s),
                None => "".to_string(),
            });
            get_records(client, repository, metadata_prefix, set, from, until, write);
        },
        Verb::ListSets => {
            println!("Listing sets available from {}", repository);
            get_sets(client, repository, write);
        },
        Verb::TestXpath { infile } => {
            test_xpath(infile);
        },
    };
}

fn test_xpath(infile: &PathBuf) -> Result<(), anyhow::Error> {
    let mut reader: Box<dyn BufRead> = Box::new(BufReader::new(File::open(infile)?));

    let mut input_xml = String::new();
    reader.read_to_string(&mut input_xml)?;

    let resumption_token = get_xpath(&input_xml, "//resumptionToken")?;
    println!("{:?}", resumption_token);
    Ok(())
}

fn get_xpath(input_xml: &str, xpath: &str) -> Result<Option<String>, anyhow::Error> {
    let mut documents = Documents::new();
    let doc = documents.add_string_without_uri(&input_xml)?;

    let mut static_context_builder = StaticContextBuilder::default();
    static_context_builder.default_element_namespace("http://www.openarchives.org/OAI/2.0/");

    let queries = Queries::new(static_context_builder);

    let sequence_query = queries.sequence(xpath)?;
    let mut context_builder = sequence_query.dynamic_context_builder(&documents);
    context_builder.context_item(doc.to_item(&documents)?);
    let context = context_builder.build();

    let sequence = sequence_query.execute_with_context(&mut documents, &context)?;
    let result = match sequence {
        Sequence::Empty(_) => None,
        something => {
            let s = something.string_value(documents.xot())?;
            if s.is_empty() {
                None
            } else {
                Some(s)
            }
        },
    };
    Ok(result)
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
fn get_records(client: Client, repository: IriString, prefix: &String, set: &Option<String>, from: &Option<String>, until: &Option<String>, write: bool) -> anyhow::Result<()> {
    let now = Instant::now();
    let request_base = format!("{}?verb=ListRecords", repository.to_string());
    let request_target = format!("{}&metadataPrefix={}{}{}{}", request_base, prefix, match set {
        Some(s) => format!("&set={}", s),
        None => "".to_string(),
    }, match from {
        Some(s) => format!("&from={}", s),
        None => "".to_string(),
    }, match until {
        Some(s) => format!("&until={}", s),
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

    let resumption_token = get_xpath(&result, "//resumptionToken");
    match resumption_token {
        Ok(token_opt) => {
            match token_opt {
                Some(token) => fetch_results(&client, &request_base, &token, now, write, &base_filename),
                None => {
                    println!("done! (no resumption token)");
                    return Ok(());
                }
            }
        },
        Err(e) => {
            println!("{:?}", e);
            return Ok(());
        },
    };
    Ok(())
}

fn fetch_results(client: &Client, request_base: &str, resumption_token: &str, now: Instant, write: bool, base_filename: &str) -> anyhow::Result<()> {
    let elapsed = now.elapsed().as_secs();
    println!("{} fetching for token: {}", elapsed, resumption_token);
    let request = client.get(&format!("{}&resumptionToken={}", request_base, urlencoding::encode(&resumption_token)));
    let result = client.execute(request.build().unwrap()).unwrap().text().unwrap();
    //println!("{}", result);
    if write {
        let filename = format!("{}-{}.xml", base_filename, elapsed.to_string());
        write_result(&filename, &result);
    }
    let resumption_token = get_xpath(&result, "//resumptionToken");
    match resumption_token {
        Ok(token_opt) => {
            match token_opt {
                Some(token) => fetch_results(&client, &request_base, &token, now, write, &base_filename),
                None => {
                    println!("done! (no resumption token)");
                    return Ok(());
                }
            }
        },
        Err(e) => {
            println!("{:?}", e);
            return Ok(());
        },
    };
    Ok(())
}
