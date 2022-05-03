use anyhow::Result;
use csv::ReaderBuilder;
use hyper::{body::HttpBody as _, Client};
use hyper_tls::HttpsConnector;
use log::LevelFilter;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Root};
use log4rs::encode::pattern::PatternEncoder;
use serde_derive::Deserialize;
use std::fs::{create_dir, create_dir_all};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;
use tokio::task;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;

#[derive(Deserialize, Debug)]
struct UserConfig {
    save_path: String,
    read_path: String,
    log_path: String,
    processor_limit: i64,
    downloader_limit: i64,
    download_url: Vec<String>,
}

lazy_static! {
    static ref CONFIG: UserConfig = {
        use std::fs;
        //Enter your config file path here.
        let config_path: &Path = Path::new("./config.toml");
        let contents = fs::read_to_string(config_path).unwrap();
        toml::from_str(&contents).unwrap()
    };
}

#[derive(Deserialize, Debug)]
struct Target {
    chembl_id: String,
    target_name: String,
    uniprot_accession: String,
}

//Using CONFIG.read_path
#[tokio::main]
async fn main() -> Result<()> {
    let logfile = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{l} - {m}\n")))
        .build(&CONFIG.log_path)?;
    let config = Config::builder()
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .build(Root::builder().appender("logfile").build(LevelFilter::Info))?;

    log4rs::init_config(config)?;
    debug!("Config : {:?}", *CONFIG);
    debug!("{:?}", Path::new(&CONFIG.read_path));
    let mut data_bank = File::open(&CONFIG.read_path).await?;
    let mut data = Vec::new();
    data_bank.read_to_end(&mut data).await?;
    let mut rdr = ReaderBuilder::new().delimiter(b';').from_reader(&*data);

    let mut tasks = Vec::new();
    let processor_limit = Arc::new(Semaphore::new(CONFIG.processor_limit as usize));

    for (i, result) in rdr.records().enumerate() {
        let record = result?;
        let target: Target = record.deserialize(None)?;
        let semaphore = processor_limit.clone();
        let path_grouped = Path::new(&CONFIG.save_path).join(format!("{}00", i / 100));
        if !path_grouped.exists() {
            create_dir_all(&path_grouped)?;
        }
        tasks.push(task::spawn(async move {
            let permit = semaphore.acquire_owned().await.unwrap();
            process_data(target, path_grouped).await?;
            drop(permit);
            Result::<()>::Ok(())
        }));
    }

    for task in tasks {
        if let Err(e) = task.await? {
            error!("Failed to process data due to \"{}\"", e);
        }
    }
    info!("Procedure completed successfully. Exiting...");
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

//Using CONFIG.save_path
async fn process_data(target: Target, save_path: PathBuf) -> Result<()> {
    info!("Processing data for {}", target.target_name);
    let path_target = save_path.join(&target.target_name.replace('/', "|"));
    info!("Crating folder: {}", path_target.display());
    if !path_target.exists() {
        if let Err(e) = create_dir(&path_target) {
            error!("Failed to create directory: {}", &path_target.display());
            return Err(e.into());
        }
    }

    let id_file = path_target.join(&target.chembl_id);
    if !id_file.exists() {
        if let Err(e) = File::create(&id_file).await {
            error!("Failed to create file: {}", &id_file.display());
            return Err(e.into());
        }
    }

    if target.uniprot_accession.is_empty() {
        info!("No Uniprot data for {}", target.target_name);
        return Ok(());
    }

    let uniprot_accessions = target.uniprot_accession.split('|').collect::<Vec<_>>();
    for uniprot_accession in uniprot_accessions {
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
            info!(
                "No PDB data found for {}:{}",
                &target.target_name, uniprot_accession
            );
            continue;
        }

        //Crating folder for target
        let path_uniprot = path_target.join(&uniprot_accession);
        info!("Crating folder: {}", path_uniprot.display());
        if !path_uniprot.exists() {
            create_dir(&path_uniprot)?;
        }

        //Spawn download tasks
        let downloader_limit = Arc::new(Semaphore::new(CONFIG.downloader_limit as usize));
        let mut tasks: Vec<task::JoinHandle<Result<(), anyhow::Error>>> = Vec::new();
        for pdb_id in lines {
            debug!("PDB ID : {}", pdb_id);
            let semaphore = downloader_limit.clone();
            let path_uniprot = path_uniprot.clone();
            tasks.push(task::spawn(async move {
                let permit = semaphore.acquire_owned().await.unwrap();
                download_pdb(pdb_id, path_uniprot).await?;
                drop(permit);
                Result::<()>::Ok(())
            }));
        }

        //Wait until download done
        for task in tasks {
            if let Err(e) = task.await? {
                error!("Failed to download due to \"{}\"", e);
            }
        }
    }
    info!("Target {} processed", target.target_name);
    Ok(())
}

//Using CONFIG.download_url
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
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Check your config urls",
                )
                .into());
            }
        });
        debug!("Save path : {}", save_filepath.display());
        if save_filepath.exists() {
            return Ok(());
        }

        let mut res = client.get(url).await?;
        if res.status() != hyper::StatusCode::OK {
            continue;
        }

        let mut file = File::create(&save_filepath).await?;
        while let Some(next) = res.data().await {
            file.write(&next?).await?;
        }

        break;
    }

    Ok(())
}
