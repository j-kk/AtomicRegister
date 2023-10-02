use std::path::PathBuf;

use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use crate::StableStorage;

// You can add here other imports from std or crates listed in Cargo.toml.

// You can add any private types, structs, consts, functions, methods, etc., you need.

struct Storage {
    root_storage_dir: PathBuf,
}

fn calc_name(key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key);
    let raw = hasher.finalize();
    let s = base64::encode(raw);
    s.replace("/", "_")
}

#[async_trait::async_trait]
impl StableStorage for Storage {
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String> {
        if key.len() > 255 {
            return Err(String::from("Key is too long"));
        }
        if value.len() > 65535 {
            return Err(String::from("Value is too long"))
        }
        let mut tmp_path = self.root_storage_dir.clone();
        let file_name = calc_name(key);
        tmp_path.push(format!("tmpfile_{}", &file_name));

        let mut file = tokio::fs::File::create(&tmp_path).await.map_err(|e| e.to_string())?;
        file.write_all(value).await.map_err(|e| e.to_string())?;
        file.sync_data().await.map_err(|e| e.to_string())?;

        let mut dst_path = self.root_storage_dir.clone();
        dst_path.push(&file_name);

        tokio::fs::rename(&tmp_path, &dst_path).await.map_err(|e| e.to_string())?;
        let dir = tokio::fs::File::create(&self.root_storage_dir).await.map_err(|e| e.to_string())?;
        dir.sync_data().await.map_err(|e| e.to_string())
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut dst_path = self.root_storage_dir.clone();
        dst_path.push(calc_name(key));
        let content = tokio::fs::read(&dst_path).await;
        match content {
            Ok(value) => Some(value),
            Err(_) => None,
        }
    }
}

/// Creates a new instance of stable storage.
pub async fn build_stable_storage(root_storage_dir: PathBuf) -> Box<dyn StableStorage> {
    return Box::new(Storage {
        root_storage_dir
    });
}
