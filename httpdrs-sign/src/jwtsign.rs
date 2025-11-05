use std::fmt::Display;
use std::path::{Path, PathBuf};

use jwt::{Token, Header, Claims};
use base64::prelude::*;
use rmp_serde::from_slice;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct  HttpdMetaReader{
    pub proto: String,
    pub path: String,
    pub prefix: String,
}

impl HttpdMetaReader {
    pub fn local_relative_path(&self) -> PathBuf {
        PathBuf::from(&self.prefix).join(&self.path)
    }

    pub fn local_absolute_path(&self, base_dir: impl AsRef<Path>) -> PathBuf {
        base_dir.as_ref().join(self.local_relative_path())
    }
    pub fn local_absolute_path_str(&self, base_dir: &str) -> PathBuf {
        PathBuf::from(base_dir).join(self.local_relative_path())
    }

    pub fn check_local_file(&self, base_dir: &str) -> Option<u64> {
        let path = self.local_absolute_path_str(base_dir);
        check_file_meta(path)
    }

    pub fn local_part_path(&self, base_dir: &str, part_index: u64, temp_dir: &str) -> PathBuf {
        // 获取本地绝对路径
        let local_path = self.local_absolute_path_str(base_dir);

        // 计算文件路径的MD5哈希
        let absolute_path_str = local_path.canonicalize()
            .unwrap_or(local_path.clone())
            .to_string_lossy()
            .to_string();
        let file_hash = format!("{:x}", md5::compute(absolute_path_str.as_bytes()));

        // 获取文件名
        let file_name = local_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("unknown");

        // 生成分片文件名: {part_index}__{file_hash}__{file_name}.bin
        let part_filename = format!("{}__{}__{}.bin", part_index, file_hash, file_name);
        PathBuf::from(temp_dir).join(part_filename)
    }

}

impl Display for HttpdMetaReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.local_relative_path())
    }
}

pub fn reader_parse(token: String) -> Result<HttpdMetaReader, Box<dyn std::error::Error>> {
    let t = Token::<Header, Claims, _>::parse_unverified(&*token).unwrap();
    let claims =  t.claims().clone();

    let download_path = claims.private.get("download_path").unwrap().to_string().trim_matches('"').to_string();
    let base64_decoded = BASE64_STANDARD.decode(download_path)?;
    let meta_reader: HttpdMetaReader = from_slice(&base64_decoded)?;
    Ok(meta_reader)
}

pub fn check_file_meta(file_path: PathBuf) -> Option<u64> {
    if file_path.is_file() {
        std::fs::metadata(&file_path)
            .ok()
            .map(|meta| meta.len())
    } else {
        tracing::debug!("{} is not a file", file_path.display());
        Some(0)
    }
}

