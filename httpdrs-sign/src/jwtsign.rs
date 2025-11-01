use std::fmt::Display;
use std::path::{Path, PathBuf};

use jwt::{Token, Header, Claims};
use base64::prelude::*;
use rmp_serde::from_slice;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct  MetaData{
    pub proto: String,
    pub path: String,
    pub prefix: String,
}

impl MetaData {
    pub fn local_relative_path(&self) -> PathBuf {
        PathBuf::from(&self.prefix).join(&self.path)
    }

    pub fn local_absolute_path(&self, base_dir: impl AsRef<Path>) -> PathBuf {
        base_dir.as_ref().join(self.local_relative_path())
    }
    pub fn local_absolute_path_str(&self, base_dir: &str) -> PathBuf {
        PathBuf::from(base_dir).join(self.local_relative_path())
    }

}

impl Display for MetaData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.local_relative_path())
    }
}

pub fn jwtsign(token: String) -> Result<MetaData, Box<dyn std::error::Error>> {
    let t = Token::<Header, Claims, _>::parse_unverified(&*token).unwrap();
    let claims =  t.claims().clone();

    let download_path = claims.private.get("download_path").unwrap().to_string().trim_matches('"').to_string();
    let base64_decoded = BASE64_STANDARD.decode(download_path)?;
    let meta_data: MetaData = from_slice(&base64_decoded)?;
    Ok(meta_data)
}
