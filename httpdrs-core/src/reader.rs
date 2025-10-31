use std::fmt::Display;
use csv::Reader;

pub struct CSVMetaReader {
    meta_path: String,
    file_lines : i64,
    file_bytes : i64,

}

impl CSVMetaReader {
    pub fn new(file_path: String) -> CSVMetaReader {
        CSVMetaReader {
            meta_path: file_path,
            file_lines: 0,
            file_bytes: 0,
        }
    }

    pub async fn init(&mut self) -> Result<(), Box<dyn std::error::Error>>{
        let paths = std::fs::read_dir(self.meta_path.as_str()).unwrap();
        for path in paths {
            let meta_path =  path.unwrap().path().to_string_lossy().to_string();
            let (lines, bytes) = read_meta_bin(meta_path.as_str(),  |_, _, _| {}).await? ;
            self.file_lines += lines;
            self.file_bytes += bytes;
        }
        Ok(())
    }

}

impl Display for CSVMetaReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "file_lines: {}, file_bytes: {}", self.file_lines, self.file_bytes)
    }
}


pub async fn read_meta_bin<F>(
    file_path: &str,
    mut processor: F
) -> Result<(i64, i64), Box<dyn std::error::Error>>
where
    F: FnMut(String, i64, String)
{
    let mut csv_reader = Reader::from_path(file_path)?;

    let mut lines: i64 = 0;
    let mut bytes: i64 = 0;
    for raw_result in csv_reader.records(){
        let raw_line = raw_result?;
        let sign = raw_line.get(0).unwrap();
        let size = raw_line.get(1).unwrap().parse::<i64>().unwrap();
        let extn = raw_line.get(2).unwrap();
        processor(sign.to_string(), size, extn.to_string());
        lines += 1;
        bytes += size
    }

    Ok((lines, bytes))
}




#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_read_rows() {

        let mut reader = CSVMetaReader::new("/Users/hgshicc/test/flagdataset/AIM-500/meta".to_string());
        reader.init().await.unwrap();
    }

}