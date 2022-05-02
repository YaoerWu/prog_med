use anyhow::Result;
use hyper::{body::HttpBody as _, Client};
use hyper_tls::HttpsConnector;
use serde_derive::Deserialize;
use std::env;
use std::fs::create_dir;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::task;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;

#[derive(Deserialize, Debug)]
struct Config {
    save_path: String,
    read_path: String,
    download_url: Vec<String>,
}

lazy_static! {
    static ref CONFIG: Config = {
        use std::fs;
        //Enter your config file path here.
        let config_path: &Path = Path::new("./config.toml");
        let contents = fs::read_to_string(config_path).unwrap();
        toml::from_str(&contents).unwrap()
    };
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init_timed();
    debug!("Config : {:?}", *CONFIG);

    let (target_name, uniprot_accession) =
        (env::args().nth(1).unwrap(), env::args().nth(2).unwrap());
    if let Err(e) = task::spawn(process_data(target_name, uniprot_accession)).await? {
        error!("Failed to process data due to \"{}\"", e);
    }

    Ok(())
}

async fn format(url: &str, formatter: &str) -> Result<String, std::fmt::Error> {
    if let Some(url) = url.split_once('%') {
        Ok(format!("{}{}{}", url.0, formatter, url.1))
    } else {
        Err(std::fmt::Error)
    }
}

async fn fetch_data(url: hyper::Uri) -> Result<Vec<u8>> {
    let https = HttpsConnector::new();
    let client = Client::builder().build::<_, hyper::Body>(https);

    let mut page = Vec::new();

    let mut res = client.get(url).await?;
    while let Some(next) = res.data().await {
        page.append(&mut next?.to_vec());
    }

    Ok(page)
}

async fn process_data(target_name: String, uniprot_accession: String) -> Result<()> {
    info!("Processing data for {}", target_name);
    let url = format!("https://www.uniprot.org/uniprot/{}.txt", uniprot_accession).parse()?;
    let page = fetch_data(url).await?;

    let lines = page
        //split into line
        .split(|ch| *ch == b'\n')
        //find PDB ID
        .filter(|slice| slice.starts_with(b"DR   PDB;"))
        //extract PDB ID
        .map(|slice| String::from_utf8_lossy(&slice[10..14]).to_lowercase())
        //collect PDB IDs
        .collect::<Vec<_>>();

    //Check if there is no PDB data
    if lines.is_empty() {
        warn!("No PDB data found");
        return Ok(());
    }

    //Crating folder for target
    let path = Path::new(&CONFIG.save_path).join(&target_name);
    info!("Crating folder: {}", path.display());
    if !path.exists() {
        create_dir(&path)?;
    }

    //Spawn download tasks
    let mut tasks: Vec<task::JoinHandle<Result<(), anyhow::Error>>> = Vec::new();
    for i in lines {
        debug!("PDB ID : {}", i);
        tasks.push(task::spawn(download_pdb(i.to_string(), path.clone())));
    }

    //Wait until download done
    for i in tasks {
        if let Err(e) = i.await? {
            error!("Failed to download due to \"{}\"", e);
        }
    }
    info!("Target {} downloaded", target_name);
    Ok(())
}

async fn download_pdb(pdb_id: String, save_path: PathBuf) -> Result<()> {
    let https = HttpsConnector::new();
    let client = Client::builder().build::<_, hyper::Body>(https);

    for url in &CONFIG.download_url {
        let url: hyper::Uri = format(url, &pdb_id).await?.parse()?;
        debug!("Formatted url : {}", url.to_string());
        let save_filepath = save_path.join({
            if let Some(file_name) = Path::new(url.path()).file_name() {
                file_name
            } else {
                return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Check your config urls").into());
            }
        });
        debug!("Save path : {}", save_filepath.display());
        if save_filepath.exists() {
            info!("already download {}", pdb_id);
            return Ok(());
        }

        let mut res = client.get(url).await?;
        if res.status() != hyper::StatusCode::OK {
            continue;
        }

        info!("start download {}", pdb_id);
        let mut file = File::create(&save_filepath).await?;
        while let Some(next) = res.data().await {
            file.write(&next?).await?;
        }
        info!("{} downloaded", save_filepath.to_string_lossy());
        break;
    }

    Ok(())
}
