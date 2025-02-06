use std::time::Instant;
use reqwest::blocking::Client;

fn main() {
    let now = Instant::now();
    let client = Client::new();
    let request = client.get("https://<SERVER>/oai?verb=ListRecords&metadataPrefix=<PREFIX>&set=<SET>");
    let result = client.execute(request.build().unwrap()).unwrap().text().unwrap();
    let re = regex::Regex::new(r"resumptionToken.*>(.*)</resumptionToken").unwrap();
    let captures_opt = re.captures(&result);
    match captures_opt {
        Some(captures) => {
            let resumption_token = captures.get(1);
            match resumption_token {
                Some(token) => {
                    fetch_results(client, token.as_str().to_owned(), now)
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

fn fetch_results(client: Client, resumption_token: String, now: Instant) {
    println!("{} fetching for token: {}", now.elapsed().as_secs(), resumption_token);
    let request = client.get(&format!("https://<SERVER>/oai?verb=ListRecords&resumptionToken={}", urlencoding::encode(&resumption_token)));
    let result = client.execute(request.build().unwrap()).unwrap().text().unwrap();
    //println!("{}", result);
    let re = regex::Regex::new(r"resumptionToken.*>(.*)</resumptionToken").unwrap();
    let captures = re.captures(&result);
    //println!("{:?}", captures);
    match captures {
        Some(c) => {
            let token = c.get(1);
            match token {
                Some(t) => {
                    fetch_results(client, t.as_str().to_owned(), now)
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
