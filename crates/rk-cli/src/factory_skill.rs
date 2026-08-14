use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const SKILL_FILES: &[(&str, &str)] = &[
    (
        "SKILL.md",
        include_str!("../../../.jcode/skills/factory-foreman/SKILL.md"),
    ),
    (
        "REFERENCE.md",
        include_str!("../../../.jcode/skills/factory-foreman/REFERENCE.md"),
    ),
    (
        "scripts/factory_foreman.py",
        include_str!("../../../.jcode/skills/factory-foreman/scripts/factory_foreman.py"),
    ),
    (
        "dashboard/render_factory_dashboard.py",
        include_str!(
            "../../../.jcode/skills/factory-foreman/dashboard/render_factory_dashboard.py"
        ),
    ),
    (
        "dashboard/templates/factory-dashboard.md",
        include_str!(
            "../../../.jcode/skills/factory-foreman/dashboard/templates/factory-dashboard.md"
        ),
    ),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InstallDisposition {
    Installed,
    AlreadyInstalled,
    Updated,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct InstallResult {
    pub disposition: InstallDisposition,
    pub destination: PathBuf,
}

pub fn global_factory_foreman_destination() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| {
        anyhow!("cannot locate Jcode's global skill directory: no home directory")
    })?;
    Ok(home.join(".jcode").join("skills").join("factory-foreman"))
}

pub fn install_factory_foreman_skill(destination: &Path, force: bool) -> Result<InstallResult> {
    if destination.exists() {
        if installed_files_match(destination)? {
            return Ok(InstallResult {
                disposition: InstallDisposition::AlreadyInstalled,
                destination: destination.to_path_buf(),
            });
        }
        if !force {
            return Err(anyhow!(
                "{} already exists and differs from this RK release; rerun with --force to replace it",
                destination.display()
            ));
        }
    }

    let parent = destination
        .parent()
        .ok_or_else(|| anyhow!("skill destination has no parent: {}", destination.display()))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create Jcode skill directory {}", parent.display()))?;

    let staging = sibling_path(destination, "install");
    if staging.exists() {
        remove_path(&staging)
            .with_context(|| format!("remove stale installer path {}", staging.display()))?;
    }
    write_skill_files(&staging)?;

    let disposition = if destination.exists() {
        let backup = sibling_path(destination, "backup");
        if backup.exists() {
            remove_path(&backup)
                .with_context(|| format!("remove stale skill backup {}", backup.display()))?;
        }
        fs::rename(destination, &backup).with_context(|| {
            format!(
                "move existing skill {} to {}",
                destination.display(),
                backup.display()
            )
        })?;
        if let Err(error) = fs::rename(&staging, destination) {
            return match fs::rename(&backup, destination) {
                Ok(()) => Err(error).with_context(|| {
                    format!(
                        "install staged skill {} to {}; restored the previous skill",
                        staging.display(),
                        destination.display()
                    )
                }),
                Err(restore_error) => Err(anyhow!(
                    "install staged skill {} to {} failed: {error}; restoring the previous skill also failed: {restore_error}; the previous skill remains recoverable at {}",
                    staging.display(),
                    destination.display(),
                    backup.display()
                )),
            };
        }
        remove_path(&backup)
            .with_context(|| format!("remove replaced skill backup {}", backup.display()))?;
        InstallDisposition::Updated
    } else {
        fs::rename(&staging, destination).with_context(|| {
            format!(
                "install staged skill {} to {}",
                staging.display(),
                destination.display()
            )
        })?;
        InstallDisposition::Installed
    };

    Ok(InstallResult {
        disposition,
        destination: destination.to_path_buf(),
    })
}

fn installed_files_match(destination: &Path) -> Result<bool> {
    if !fs::symlink_metadata(destination)?.is_dir() {
        return Ok(false);
    }
    let expected_files = SKILL_FILES
        .iter()
        .map(|(relative, _)| PathBuf::from(relative))
        .collect::<BTreeSet<_>>();
    if installed_tree_files(destination)? != expected_files {
        return Ok(false);
    }
    for (relative, expected) in SKILL_FILES {
        let path = destination.join(relative);
        match fs::read_to_string(&path) {
            Ok(actual) if actual == *expected => {}
            Ok(_) => return Ok(false),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("read installed skill {}", path.display()));
            }
        }
    }
    Ok(true)
}

fn remove_path(path: &Path) -> std::io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
}

fn installed_tree_files(destination: &Path) -> Result<BTreeSet<PathBuf>> {
    let mut files = BTreeSet::new();
    let mut directories = vec![destination.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(&directory)
            .with_context(|| format!("read installed skill directory {}", directory.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.is_dir() {
                directories.push(path);
            } else {
                files.insert(
                    path.strip_prefix(destination)
                        .expect("walked path stays under skill destination")
                        .to_path_buf(),
                );
            }
        }
    }
    Ok(files)
}

fn write_skill_files(destination: &Path) -> Result<()> {
    for (relative, contents) in SKILL_FILES {
        let path = destination.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create skill directory {}", parent.display()))?;
        }
        fs::write(&path, contents)
            .with_context(|| format!("write embedded skill file {}", path.display()))?;
        set_script_permissions(&path, relative)?;
    }
    Ok(())
}

#[cfg(unix)]
fn set_script_permissions(path: &Path, relative: &str) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    if relative.ends_with(".py") {
        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions)
            .with_context(|| format!("mark skill script executable {}", path.display()))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn set_script_permissions(_path: &Path, _relative: &str) -> Result<()> {
    Ok(())
}

fn sibling_path(destination: &Path, label: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("factory-foreman");
    destination.with_file_name(format!(".{name}.{label}-{}-{stamp}", std::process::id()))
}

#[cfg(test)]
mod tests {
    use super::{InstallDisposition, install_factory_foreman_skill};
    use std::fs;

    #[test]
    fn installs_a_global_native_factory_foreman_skill() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("factory-foreman");

        let result = install_factory_foreman_skill(&destination, false).unwrap();

        assert_eq!(result.disposition, InstallDisposition::Installed);
        assert_eq!(result.destination, destination);
        let skill = fs::read_to_string(destination.join("SKILL.md")).unwrap();
        assert!(skill.contains("rk --json factory snapshot"));
        assert!(skill.contains("rk --json factory propose-workflow"));
        assert!(!skill.contains("Use this repository-local skill"));
        assert!(destination.join("REFERENCE.md").is_file());
        assert!(destination.join("scripts/factory_foreman.py").is_file());
    }

    #[test]
    fn identical_install_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("factory-foreman");
        install_factory_foreman_skill(&destination, false).unwrap();

        let result = install_factory_foreman_skill(&destination, false).unwrap();

        assert_eq!(result.disposition, InstallDisposition::AlreadyInstalled);
    }

    #[test]
    fn refuses_to_replace_a_modified_global_skill_without_force() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("factory-foreman");
        install_factory_foreman_skill(&destination, false).unwrap();
        fs::write(destination.join("SKILL.md"), "customized\n").unwrap();

        let error = install_factory_foreman_skill(&destination, false).unwrap_err();

        assert!(error.to_string().contains("--force"));
        assert_eq!(
            fs::read_to_string(destination.join("SKILL.md")).unwrap(),
            "customized\n"
        );
    }

    #[test]
    fn refuses_an_installed_skill_with_stale_extra_files_without_force() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("factory-foreman");
        install_factory_foreman_skill(&destination, false).unwrap();
        fs::write(destination.join("LEGACY.md"), "stale\n").unwrap();

        let error = install_factory_foreman_skill(&destination, false).unwrap_err();

        assert!(error.to_string().contains("--force"));
        assert!(destination.join("LEGACY.md").is_file());
    }

    #[test]
    fn force_replaces_a_modified_global_skill() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("factory-foreman");
        install_factory_foreman_skill(&destination, false).unwrap();
        fs::write(destination.join("SKILL.md"), "customized\n").unwrap();

        let result = install_factory_foreman_skill(&destination, true).unwrap();

        assert_eq!(result.disposition, InstallDisposition::Updated);
        let skill = fs::read_to_string(destination.join("SKILL.md")).unwrap();
        assert!(skill.contains("rk --json factory snapshot"));
    }

    #[test]
    fn force_replaces_a_conflicting_file_at_the_skill_destination() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("factory-foreman");
        fs::write(&destination, "not a skill directory\n").unwrap();

        let result = install_factory_foreman_skill(&destination, true).unwrap();

        assert_eq!(result.disposition, InstallDisposition::Updated);
        assert!(destination.join("SKILL.md").is_file());
    }
}
