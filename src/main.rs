use anyhow::Result;
use hyper::{body::HttpBody as _, Client};
use hyper_tls::HttpsConnector;
use std::env;
use std::fs::create_dir;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::task;

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let (target_name, uniprot_accession) =
        (env::args().nth(1).unwrap(), env::args().nth(2).unwrap());
    drop(task::spawn(process_data(target_name, uniprot_accession)).await?);

    Ok(())
}

async fn process_data(target_name: String, uniprot_accession: String) -> Result<()> {
    let url = format!("https://www.uniprot.org/uniprot/{}.txt", uniprot_accession).parse()?;
    let page = fetch_data(url).await?;

    let lines = page
        //split into line
        .split(|ch| *ch == b'\n')
        //find PDB ID
        .filter(|slice| slice.starts_with(b"DR   PDB;"))
        //extract PDB ID
        .map(|slice| String::from_utf8_lossy(&slice[10..14]))
        //collect PDB IDs
        .collect::<Vec<_>>();

    let path = Path::new("..\\Human").join(target_name);
    if !path.exists() {
        create_dir(&path)?;
    }

    let mut tasks = Vec::new();
    for i in lines {
        let file_name = path.join(&*i).with_extension("ent");
        if file_name.exists() {
            println!("already download {}", i);
            continue;
        }
        let url: hyper::Uri = format!("https://files.rcsb.org/download/{}.pdb", i).parse()?;
        println!("start download {}", i);
        tasks.push(task::spawn(download_file(url, file_name)));
    }
    for i in tasks {
        drop(i.await?);
    }
    Ok(())
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

async fn download_file(url: hyper::Uri, save_filepath: PathBuf) -> Result<()> {
    let mut file = File::create(&save_filepath).await?;
    let https = HttpsConnector::new();
    let client = Client::builder().build::<_, hyper::Body>(https);
    let mut res = client.get(url).await?;
    while let Some(next) = res.data().await {
        file.write(&next?).await?;
    }
    println!("saved at {}", save_filepath.to_string_lossy());
    Ok(())
}
