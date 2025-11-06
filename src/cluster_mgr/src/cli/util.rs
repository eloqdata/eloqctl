use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use std::io::{self, Write};

/// Prompt user for confirmation to proceed with an action
/// Uses blocking I/O since this is a user interaction
///
/// # Arguments
/// * `prompt` - The confirmation prompt message (e.g., "Do you want to proceed with deletion?")
///
/// # Returns
/// * `Ok(true)` if user confirms (enters "yes" or "y")
/// * `Ok(false)` if user declines (enters anything else)
/// * `Err` if there's an I/O error
pub fn confirm_action(prompt: &str) -> Result<bool> {
    print!("{} (yes/no): ", prompt);
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    let input = input.trim().to_lowercase();
    Ok(input == "yes" || input == "y")
}

pub fn os_id() -> String {
    let os_id = sysinfo::System::distribution_id();
    match os_id.as_str() {
        "centos" | "rocky" => "rhel".to_owned(),
        _ => os_id,
    }
}

pub fn os_major_version() -> String {
    let os_version = sysinfo::System::os_version().expect("version id not found");
    match os_version.find('.') {
        Some(i) => os_version[..i].to_owned(),
        None => os_version,
    }
}

pub fn cpu_arch() -> String {
    let cpu_arch = sysinfo::System::cpu_arch().expect("can't know cpu arch");
    match cpu_arch.as_str() {
        "aarch64" | "arm64" => "arm64",
        "x86" | "x86_64" | "amd64" => "amd64",
        _ => return cpu_arch,
    }
    .to_owned()
}

pub fn file_pg_bar() -> ProgressBar {
    let temp =
        "{spinner:.green} {bar:40.cyan/grey} {msg} [{bytes}/{total_bytes}] {elapsed}(ETA {eta})";
    let cmd_pb = ProgressBar::new(0);
    cmd_pb.set_style(ProgressStyle::default_spinner().template(temp).unwrap());
    cmd_pb
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread::sleep, time::Duration};

    #[test]
    fn test_progress_bar() {
        println!("start");
        let bar = file_pg_bar();
        bar.set_length(30 * 1024);
        for _ in 0..30 {
            sleep(Duration::from_secs(1));
            bar.set_message("downloading");
            bar.inc(1024);
        }
        bar.finish_with_message("done");
    }
}
