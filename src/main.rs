use std::time::Instant;
use std::fmt;
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
        xpath: String,
    },
    TestResponse,
}

struct Harvest {
    repository: IriString,
    metadata_prefix: String,
    set: Option<String>,
    from: Option<String>,
    until: Option<String>,
    last_record_date: Option<String>,
}

impl Harvest {
    fn request_base(&self) -> String {
        format!("{}?verb=ListRecords", self.repository.to_string())
    }

    fn request_url(&self) -> String {
        format!("{}&metadataPrefix={}{}{}{}", self.request_base(), &self.metadata_prefix, match &self.set {
            Some(s) => format!("&set={}", s),
            None => "".to_string(),
        }, match &self.from {
            Some(s) => format!("&from={}", s),
            None => "".to_string(),
        }, match &self.until {
            Some(s) => format!("&until={}", s),
            None => "".to_string(),
        })
    }

    fn resumption_url(&self, resumption_token: &str) -> String {
        format!("{}&resumptionToken={}", &self.request_base(), urlencoding::encode(&resumption_token))
    }

    fn filename(&self, file_id: String) -> String {
        let base_filename = format!("{}-{}",
                                    &self.metadata_prefix,
                                    match &self.set {
                                        Some(s) => s,
                                        None => "all",
                                    });
        format!("{}-{}.xml", base_filename, file_id)
    }
}

impl fmt::Display for Harvest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Harvesting records from {} using prefix {} from {}{}{}{}",
                 &self.repository,
                 &self.metadata_prefix,
                 match &self.set {
                    Some(s) => format!("set {}", s),
                    None => "all sets".to_string(),
                }, match &self.from {
                    Some(s) => format!(" starting from date {}", s),
                    None => "".to_string(),
                }, match &self.until {
                    Some(s) => format!(" until date {}", s),
                    None => "".to_string(),
                }, match &self.last_record_date {
                    Some(s) => format!(" (last record harvested was from date {})", s),
                    None => "".to_string(),
                })
    }
}

impl fmt::Debug for Harvest {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Harvest")
            .field("repository", &self.repository)
            .field("metadata_prefix", &self.metadata_prefix)
            .field("set", &self.set)
            .field("from", &self.from)
            .field("until", &self.until)
            .field("last_record_date", &self.last_record_date)
            .finish()
    }
}

fn main() -> anyhow::Result<()> {
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
            get_metadata_formats(client, repository, write)?;
        },
        Verb::ListRecords { metadata_prefix, set, from, until } => {
            let harvest = Harvest {
                repository: repository.clone(),
                metadata_prefix: metadata_prefix.clone(),
                set: set.clone(),
                from: from.clone(),
                until: until.clone(),
                last_record_date: None,
            };
            println!("{}", harvest);
            get_records(client, harvest, write)?;
        },
        Verb::ListSets => {
            println!("Listing sets available from {}", repository);
            get_sets(client, repository, write)?;
        },
        Verb::TestXpath { infile, xpath } => {
            test_xpath(infile, xpath)?;
        },
        Verb::TestResponse => {
            test_response(client, repository)?;
        },
    };
    Ok(())
}

fn test_xpath(infile: &PathBuf, xpath: &str) -> Result<(), anyhow::Error> {
    let mut reader: Box<dyn BufRead> = Box::new(BufReader::new(File::open(infile)?));

    let mut input_xml = String::new();
    reader.read_to_string(&mut input_xml)?;

    let result = get_xpath(&input_xml, xpath)?;
    println!("{:?}", result);
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

fn write_result(filepath: &str, result: &str) -> anyhow::Result<()> {
    let path: PathBuf = PathBuf::from(filepath);
    Ok(fs::write(path.as_path(), result)?)
}

fn get_metadata_formats(client: Client, repository: IriString, write: bool) -> anyhow::Result<()> {
    let request_target = format!("{}?verb=ListMetadataFormats", repository.to_string());
    let request = client.get(request_target);
    let result = client.execute(request.build().unwrap()).unwrap().text().unwrap();
    if write {
        write_result("formats.xml", &result)?;
    };
    Ok(())
}

fn get_sets(client: Client, repository: IriString, write: bool) -> anyhow::Result<()> {
    let request_target = format!("{}?verb=ListSets", repository.to_string());
    let request = client.get(request_target);
    let result = client.execute(request.build().unwrap()).unwrap().text().unwrap();
    if write {
        write_result("sets.xml", &result)?;
    };
    Ok(())
}

fn test_response(client: Client, repository: IriString) -> anyhow::Result<()> {
    let request = client.get(format!("{}", repository.to_string()));
    let response = client.execute(request.build()?)?;
    if response.status().is_success() {
        return Ok(())
    } else {
        println!("error: {:?}", response.status());
    }
    Ok(())
}

fn handle_resumption(client: &Client, result_text: &str, now: Instant, write: bool, harvest: Harvest) -> anyhow::Result<()> {
    let resumption_token = get_xpath(&result_text, "//resumptionToken");
    match resumption_token {
        Ok(token_opt) => {
            match token_opt {
                Some(token) => {
                    fetch_results(&client, &token, now, write, harvest)?;
                },
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

fn get_records(client: Client, harvest: Harvest, write: bool) -> anyhow::Result<()> {
    let now = Instant::now();
    let request = client.get(harvest.request_url());
    let result = client.execute(request.build().unwrap()).unwrap().text().unwrap();
    // println!("{:?}", result);
    if write {
        let filename = harvest.filename("0".to_string());
        write_result(&filename, &result)?;
    }
    let last_record_date = get_xpath(&result, "//record[last()]//datestamp")?;
    match last_record_date {
        Some(from) => {
            let new_harvest = Harvest {
                from: Some(from.clone()),
                ..harvest
            };
            return handle_resumption(&client, &result, now, write, new_harvest);
        },
        None => println!("no last record date found; not continuing"),
    };
    Ok(())
}

fn fetch_results(client: &Client, resumption_token: &str, now: Instant, write: bool, harvest: Harvest) -> anyhow::Result<()> {
    let elapsed = now.elapsed().as_secs();
    println!("{} fetching for token: {}", elapsed, resumption_token);

    let request = client.get(&harvest.resumption_url(resumption_token)).build()?;
    let response = client.execute(request)?;
    if response.status().is_success() {
        let result = response.text()?;
        //println!("{}", result);
        if write {
            write_result(&harvest.filename(elapsed.to_string()), &result)?;
        }
        let last_record_date = get_xpath(&result, "//record[last()]//datestamp")?;
        match last_record_date {
            Some(from) => {
                let new_harvest = Harvest {
                    from: Some(from.clone()),
                    ..harvest
                };
                return handle_resumption(&client, &result, now, write, new_harvest);
            },
            None => println!("no last record date found; not continuing"),
        };
    } else {
        match harvest.last_record_date {
            Some(ref _from) => {
                return get_records(client.clone(), harvest, write);
            },
            None => {
                println!("Got an error! Need to retry.");
                println!("{:?}", response.text()?);
            },
        }
    }
    Ok(())
}
