use anyhow::Result;
use csv::ReaderBuilder;
use reqwest::{Client, Url};
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
    log_config: String,
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
static ref CLIENT:Client= Client::new();}

#[derive(Deserialize, Debug)]
struct Target {
    chembl_id: String,
    target_name: String,
    uniprot_accession: String,
}

//Using CONFIG.read_path
#[tokio::main]
async fn main() -> Result<()> {
    log4rs::init_file(&CONFIG.log_config, Default::default()).unwrap();
    debug!(target:"debug","Config : {:?}", *CONFIG);

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
        let path_grouped = Path::new(&CONFIG.save_path).join(format!("{}", i));
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

//Using CONFIG.save_path
async fn process_data(target: Target, save_path: PathBuf) -> Result<()> {
    let path_target = save_path.join(&target.target_name.replace('/', "|"));
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
        let url: Url =
            format!("https://www.uniprot.org/uniprot/{}.txt", uniprot_accession).parse()?;
        let page = CLIENT.get(url).send().await?.text().await?;

        let lines = page
            //split into line
            .split('\n')
            //find PDB ID
            .filter(|slice| slice.starts_with("DR   PDB;"))
            //extract PDB ID
            .map(|slice| slice[10..14].to_lowercase())
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
        if !path_uniprot.exists() {
            create_dir(&path_uniprot)?;
        }

        //Spawn download tasks
        let downloader_limit = Arc::new(Semaphore::new(CONFIG.downloader_limit as usize));
        let mut tasks: Vec<task::JoinHandle<Result<(), anyhow::Error>>> = Vec::new();
        for pdb_id in lines {
            debug!(target:"debug","PDB ID : {}", pdb_id);
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
    Ok(())
}

//Using CONFIG.download_url
async fn download_pdb(pdb_id: String, save_path: PathBuf) -> Result<()> {
    for url in &CONFIG.download_url {
        let url: Url = format(url, &pdb_id).await?.parse()?;
        debug!(target:"debug","Formatted url : {}", url.to_string());
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
        if save_filepath.exists() {
            return Ok(());
        }

        let data = match CLIENT.get(url).send().await {
            Ok(data) => data.text().await?,
            Err(_) => continue,
        };
        let mut file = File::create(&save_filepath).await?;
        file.write_all(data.as_bytes()).await?;
        break;
    }

    Ok(())
}
