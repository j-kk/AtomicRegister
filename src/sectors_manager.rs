use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::{SectorIdx, SectorsManager, SectorVec};
use crate::network::netdata::SECTOR_SIZE;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
struct MetaInfoUnit {
    idx: SectorIdx,
    ts: u64,
    wr: u8,
}

impl MetaInfoUnit {
    fn new(idx: SectorIdx, ts: u64, wr: u8) -> MetaInfoUnit {
        MetaInfoUnit {
            idx,
            ts,
            wr,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct MetaInfo {
    units: Vec<MetaInfoUnit>,
}

struct SManager {
    root_storage_dir: std::path::PathBuf,
}

pub(crate) struct SManagerWrapper {
    mutex: tokio::sync::RwLock<SManager>,
}

impl MetaInfo {
    fn update(&mut self, update: MetaInfoUnit) {
        for meta_info_unit in self.units.iter_mut() {
            if meta_info_unit.idx == update.idx {
                *meta_info_unit = update;
                return;
            }
        }
        self.units.push(update);
    }
}

impl SManager {
    fn new(storage_dir: PathBuf) -> SManager {
        let sm = SManager { root_storage_dir: storage_dir };
        let mut transaction_path = sm.root_storage_dir.clone();
        transaction_path.push("temp_transaction");
        if transaction_path.exists() {
            let maybe_idx = fs::read(&transaction_path);
            if let Err(error) = maybe_idx {
                log::error!("An error occurred during init (transaction file) using SectorsManager {}", error.to_string());
                panic!("An error occurred during init (transaction file) using SectorsManager {}", error.to_string());
            }
            let transaction_idx_deserialized = bincode::deserialize::<SectorIdx>(&*maybe_idx.unwrap());
            if let Err(error) = transaction_idx_deserialized {
                log::error!("An error occurred during init (transaction file deserialize) using SectorsManager {}", error.to_string());
                panic!("An error occurred during init (transaction file deserialize) using SectorsManager {}", error.to_string());
            }
            let transaction_idx = transaction_idx_deserialized.unwrap();


            let mut temp_data_path = sm.root_storage_dir.clone();
            temp_data_path.push(format!("temp_sector_data_{}", transaction_idx));

            let mut temp_meta_path = sm.root_storage_dir.clone();
            temp_meta_path.push("temp_meta");
            match (temp_data_path.exists(), temp_meta_path.exists()) {
                (false, true) => {
                    let mut meta_path = sm.root_storage_dir.clone();
                    meta_path.push("meta");

                    if let Err(error) = fs::rename(&temp_meta_path, &meta_path) {
                        log::error!("An error occurred during repair (sector meta rename) using SectorsManager {}", error.to_string());
                        panic!("An error occurred during repair (sector meta rename) using SectorsManager {}", error.to_string());
                    }
                    fs::remove_file(&transaction_path).unwrap();
                }
                (_, _) => {
                    fs::remove_file(&temp_data_path).unwrap();
                    fs::remove_file(&temp_meta_path).unwrap();
                    fs::remove_file(&transaction_path).unwrap();
                }
            }
        }
        return sm;
    }

    async fn write_temp_meta_file(&self, meta_info: &MetaInfo) -> std::io::Result<()> {
        let mut temp_meta_path = self.root_storage_dir.clone();
        temp_meta_path.push("temp_meta");
        let meta_content = bincode::serialize(meta_info).unwrap();

        let mut file = tokio::fs::File::create(&temp_meta_path).await?;
        file.write_all(&meta_content).await?;
        file.sync_data().await
    }

    async fn read_meta_file(&self) -> MetaInfo {
        let mut meta_path = self.root_storage_dir.clone();
        meta_path.push("meta");
        return match tokio::fs::read(meta_path).await {
            Ok(meta_content) => {
                    match bincode::deserialize::<MetaInfo>(&*meta_content) {
                        Ok(x) => { x }
                        Err(error) => {
                            log::error!("An error occurred during deserializing meta file using SectorsManager {} ", error.to_string());
                            panic!("An error occurred during deserializing meta file using SectorsManager {}", error.to_string())
                        }
                    }
                }
            Err(ref error) if error.kind() == std::io::ErrorKind::NotFound => {
                MetaInfo {
                    units: vec![]
                }
            }
            Err(error) => {
                log::error!("An error occurred during reading meta file using SectorsManager {}", error.to_string());
                panic!("An error occurred during reading meta file using SectorsManager {}", error.to_string())
            }
        };
    }

    async fn query_meta_file(&self, idx: SectorIdx) -> Result<MetaInfoUnit, String> {
        let meta_info: MetaInfo = self.read_meta_file().await;
        for meta_info_unit in meta_info.units.into_iter() {
            if meta_info_unit.idx == idx {
                return Ok(meta_info_unit);
            }
        }
        return Ok(MetaInfoUnit::new(idx, 0, 0));
    }

    async fn read_data(&self, idx: SectorIdx) -> SectorVec {
        let mut data_path = self.root_storage_dir.clone();
        data_path.push(format!("sector_data_{}", idx));
        return match tokio::fs::read(&data_path).await {
            Ok(content) => { SectorVec(content) }
            Err(ref error) if error.kind() == std::io::ErrorKind::NotFound => {
                SectorVec(vec![0; SECTOR_SIZE])
            }
            Err(error) => {
                log::error!("Could not read sector: {} error: {}", idx, error.to_string());
                panic!("Could not read sector: {} error: {}", idx, error.to_string());
            }
        };
    }

    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        let meta_result = self.query_meta_file(idx).await;
        match meta_result {
            Ok(meta_info) => return (meta_info.ts, meta_info.wr),
            Err(error) => {
                log::error!("An error occurred during read_metadata (transaction file) using SectorsManager {}", error.to_string());
                panic!("An error occurred during read_metadata (transaction file) using SectorsManager {}", error.to_string())
            }
        };
    }

    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) -> std::io::Result<()> {
        let (content, ts, wr) = sector;
        let dir = tokio::fs::File::open(&self.root_storage_dir).await?;
        // Create transaction file
        let mut transaction_path = self.root_storage_dir.clone();
        transaction_path.push("temp_transaction");
        let mut transaction_file = tokio::fs::File::create(&transaction_path).await?;

        match bincode::serialize(&idx) {
            Ok(idx_serialized) => {
                transaction_file.write_all(&idx_serialized).await?;
                transaction_file.sync_data().await?;
            }
            Err(error) => {
                log::error!("An error occurred during write (serialization of idx) using SectorsManager {}", error.to_string());
                panic!("An error occurred during write (serialization of idx) using SectorsManager {}", error.to_string());
            }
        }
        // Write temp sector
        let mut temp_data_path = self.root_storage_dir.clone();
        temp_data_path.push(format!("temp_sector_data_{}", idx));
        let mut temp_data_file = tokio::fs::File::create(&temp_data_path).await?;
        temp_data_file.write_all(&content.0).await?;

        // Read temp meta
        let mut meta_info = self.read_meta_file().await;

        // Update meta
        let update = MetaInfoUnit {
            idx,
            ts: *ts,
            wr: *wr,
        };
        meta_info.update(update);
        // Write temp meta
        self.write_temp_meta_file(&meta_info).await?;

        // Rename data
        let mut data_path = self.root_storage_dir.clone();
        data_path.push(format!("sector_data_{}", idx));
        tokio::fs::rename(&temp_data_path, &data_path).await?;

        // Rename meta
        let mut temp_meta_path = self.root_storage_dir.clone();
        temp_meta_path.push("temp_meta");
        let mut meta_path = self.root_storage_dir.clone();
        meta_path.push("meta");

        tokio::fs::rename(&temp_meta_path, &meta_path).await?;
        // Delete transaction file

        tokio::fs::remove_file(&transaction_path).await?;
        log::debug!("Successful write to to sector {}", idx);
        dir.sync_data().await
    }
}


impl SManagerWrapper {
    pub(crate) fn new(storage_dir: PathBuf) -> SManagerWrapper {
        return SManagerWrapper {
            mutex: tokio::sync::RwLock::new(SManager::new(storage_dir))
        };
    }
}

#[async_trait::async_trait]
impl SectorsManager for SManagerWrapper {
    async fn read_data(&self, idx: SectorIdx) -> SectorVec {
        let guard = self.mutex.read().await;
        return guard.read_data(idx).await;
    }

    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        let guard = self.mutex.read().await;
        return guard.read_metadata(idx).await;
    }

    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        let guard = self.mutex.write().await;
        if let Err(error) = guard.write(idx, sector).await {
            log::error!("Unexpected error occurred during write to sector manager on id {}, details: {}", sector.1, error.to_string());
            panic!("Unexpected error occurred during write to sector manager on id {}, details: {}", sector.1, error.to_string());
        }
    }
}
