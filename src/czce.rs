use crate::Result;
use bytesize::ByteSize;
use calamine::{Reader, Xls, XlsOptions};
use std::{
    fs::File,
    io::{self, Cursor},
};

const XLS: &str =
    "http://www.czce.com.cn/cn/DFSStaticFiles/Future/2023/FutureDataAllHistory/ALLFUTURES2023.xls";

const URL: &str = "http://www.czce.com.cn/cn/DFSStaticFiles/Future/2023/ALLFUTURES2023.zip";

pub fn fetch_xls(year: u16) -> Result<Cursor<Vec<u8>>> {
    let url = format!(
        "http://www.czce.com.cn/cn/DFSStaticFiles/\
         Future/{year}/FutureDataAllHistory/ALLFUTURES{year}.xls",
    );
    let bytes = minreq::get(url).send()?.into_bytes();
    info!("{URL} 获取的字节数：{}", ByteSize(bytes.len() as u64));
    Ok(Cursor::new(bytes))
}

fn fetch_parse_xls() -> Result<()> {
    // let init = crate::util::init_log();
    let reader = fetch_xls(2023)?;
    let workbook = Xls::new_with_options(reader, XlsOptions::default())?;
    Ok(())
}

#[test]
fn parse_xls() -> Result<()> {
    let mut xls = calamine::open_workbook_auto("./cache/c.xlsx")?;
    // let mut opts = XlsOptions::default();
    // opts.force_codepage = Some(1201);
    // let mut xls = Xls::new_with_options(File::open("./cache/ALLFUTURES2023.xls")?, opts)?;
    info!("Reading {:?} in c.xlsx", xls.sheet_names());
    let sheet = xls
        .worksheet_range_at(0)
        .ok_or_else(|| format!("无法获取到第 0 个表，所有表为：{:?}", xls.sheet_names()))??;
    for row in sheet.rows().take(3) {
        println!("{row:#?}");
    }
    Ok(())
}

#[test]
fn fetch_parse() -> Result<()> {
    let init = crate::util::init_log();
    let resp = minreq::get(URL).send()?;
    let bytes = resp.as_bytes();
    info!("{URL} 获取的字节数：{}", ByteSize(bytes.len() as u64));
    let mut zipped = zip::ZipArchive::new(Cursor::new(bytes))?;
    for i in 0..zipped.len() {
        let mut unzipped = zipped.by_index(i)?;
        if unzipped.is_file() {
            let file_name = unzipped
                .enclosed_name()
                .ok_or_else(|| format!("`{}` 无法转成 &Path", unzipped.name()))?;
            info!(
                "{URL} 获取的第 {i} 个文件：{} ({} => {})",
                file_name.display(),
                ByteSize(unzipped.compressed_size()),
                ByteSize(unzipped.size()),
            );
            let cached_file = init.cache_dir.join(file_name);
            let mut file = File::create(&cached_file)?;
            io::copy(&mut unzipped, &mut file)?;
            info!("已解压至 {}", cached_file.display());
        } else {
            return Err(format!("{} 还未实现解压成文件夹", unzipped.name()).into());
        }
    }
    Ok(())
}
