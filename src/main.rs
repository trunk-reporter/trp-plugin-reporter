//! The `reporter` executable: picks a mode from the command line and runs it.

use std::path::PathBuf;

use reporter::plugin;

/// What the command line asks for.
#[derive(Debug)]
pub enum Mode {
    /// Print the manifest (`--describe`).
    Describe,
    /// Print the version (`--version`).
    Version,
    /// Serve a data folder's history without the recorder (`serve <data dir>`).
    Serve { data_dir: PathBuf },
    /// Talk to the recorder over stdin/stdout (no arguments).
    Plugin,
}

pub fn mode(args: &[String]) -> Result<Mode, String> {
    match args.first().map(String::as_str) {
        None => Ok(Mode::Plugin),
        Some("--describe") => Ok(Mode::Describe),
        Some("--version" | "-V") => Ok(Mode::Version),
        Some("serve") => match args.get(1) {
            Some(dir) => Ok(Mode::Serve { data_dir: dir.into() }),
            None => Err("serve needs a data folder: reporter serve <data dir>".into()),
        },
        Some(a) => Err(format!("unknown argument {a} (use --describe, --version or serve <data dir>)")),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match mode(&args) {
        Ok(Mode::Plugin) => trunk_recorder_plugin::run::<plugin::Reporter>(),
        Ok(Mode::Describe) => println!("{}", serde_json::to_string_pretty(&trunk_recorder_plugin::describe::<plugin::Reporter>()).unwrap_or_default()),
        Ok(Mode::Version) => println!("{}", <plugin::Reporter as trunk_recorder_plugin::Plugin>::manifest().version),
        Ok(Mode::Serve { .. }) => {
            eprintln!("not built yet");
            std::process::exit(2);
        }
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arguments_pick_a_mode() {
        assert!(matches!(mode(&[]).unwrap(), Mode::Plugin));
        assert!(matches!(mode(&["--describe".into()]).unwrap(), Mode::Describe));
        assert!(matches!(mode(&["serve".into(), "d".into()]).unwrap(), Mode::Serve { .. }));
        assert!(mode(&["serve".into()]).is_err());
        assert!(mode(&["bogus".into()]).is_err());
    }
}
