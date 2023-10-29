use super::{io, ByteSize, Result};
use std::process::{Command, Output, Stdio};

fn output(output: Output, cmd: String) -> Result<String> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stdout = stdout.trim();
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = stderr.trim();
    if output.status.success() {
        struct StdOutErr<'s> {
            stdout: &'s str,
            stderr: &'s str,
        }
        impl std::fmt::Display for StdOutErr<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let StdOutErr { stdout, stderr } = self;
                if !stdout.is_empty() {
                    writeln!(f, "stdout:\n{stdout}")?;
                }
                if !stderr.is_empty() {
                    write!(f, "stderr:\n{stderr}")?;
                }
                Ok(())
            }
        }
        info!(
            "成功运行命令：{}\n{}",
            regex::Regex::new("\n")
                .unwrap()
                .find_iter(&cmd)
                .nth(3)
                .map(|cap| format!("{} ...\"", &cmd[..cap.start()]))
                .unwrap_or(cmd),
            StdOutErr { stdout, stderr }
        );
        Ok(stdout.to_owned())
    } else {
        bail!("{cmd} 运行失败\nstdout:\n{stdout}\nstderr:\n{stderr}")
    }
}

pub fn execute(sql: &str) -> Result<String> {
    const MULTI: &str = "--multiquery";
    let mut cmd = Command::new("clickhouse-client");
    cmd.args([MULTI, sql]);
    let cmd_string = format!(r#"clickhouse-client "{MULTI}" "{sql}""#);
    output(cmd.output()?, cmd_string)
}

pub fn insert(sql: &str, reader: impl io::Read + io::Seek) -> Result<()> {
    use io::Seek;
    const MULTI: &str = "--multiquery";
    let mut cmd = Command::new("clickhouse-client");
    cmd.stdin(Stdio::piped());
    cmd.args([MULTI, sql]);
    let cmd_string = format!(r#"clickhouse-client "{MULTI}" "{sql}""#);
    let mut child = cmd.spawn()?;
    if let Some(stdin) = child.stdin.as_mut() {
        let mut buf = io::BufReader::new(reader);
        let start = buf.stream_position().unwrap_or(0);
        io::copy(&mut buf, stdin)?;
        let end = buf.stream_position().unwrap_or(start);
        info!("成功向 clickhouse 插入了 {} 数据", ByteSize(end - start));
    } else {
        bail!("无法打开 stdin 来传输 clickhouse 所需的数据");
    }
    output(child.wait_with_output()?, cmd_string)?;
    Ok(())
}

pub fn insert_with_count_reported(table: &str, bytes: &[u8]) -> Result<()> {
    let sql_count = format!("SELECT count(*) FROM {table}");
    let count_old = execute(&sql_count)?;
    info!("{table} 现有数据 {count_old} 条");
    let sql_insert_csv = format!("INSERT INTO {table} FORMAT CSV");
    insert(&sql_insert_csv, io::Cursor::new(bytes))?;
    execute(&format!("OPTIMIZE TABLE {table} DEDUPLICATE BY date, code"))?;
    info!("{table} 已去重");
    let count_new = execute(&sql_count)?;
    let added = count_new
        .parse::<u64>()
        .ok()
        .zip(count_old.parse::<u64>().ok())
        .and_then(|(new, old)| new.checked_sub(old).map(|r| r.to_string()))
        .unwrap_or_default();
    info!("{table} 现有数据 {count_new} 条（增加了 {added} 条）");
    Ok(())
}
