//! Path layout. Everything lives under one home directory (default
//! `~/.rat-kingdom`, override with `RK_HOME`) so state is predictable and
//! socket paths stay short enough for `sockaddr_un`.

use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Layout {
    home: PathBuf,
}

impl Layout {
    /// Resolve the layout from `RK_HOME` or the default home directory.
    pub fn discover() -> crate::Result<Self> {
        if let Ok(home) = std::env::var("RK_HOME") {
            return Ok(Self { home: home.into() });
        }
        let base = dirs::home_dir()
            .ok_or_else(|| crate::Error::Config("cannot determine home directory".into()))?;
        Ok(Self {
            home: base.join(".rat-kingdom"),
        })
    }

    pub fn at(home: impl Into<PathBuf>) -> Self {
        Self { home: home.into() }
    }

    pub fn home(&self) -> &Path {
        &self.home
    }

    pub fn config_file(&self) -> PathBuf {
        self.home.join("config.toml")
    }

    pub fn socket_path(&self) -> PathBuf {
        self.home.join("rk.sock")
    }

    pub fn pid_file(&self) -> PathBuf {
        self.home.join("rk.pid")
    }

    pub fn db_path(&self) -> PathBuf {
        self.home.join("space.db")
    }

    pub fn log_dir(&self) -> PathBuf {
        self.home.join("logs")
    }

    pub fn worktrees_dir(&self) -> PathBuf {
        self.home.join("worktrees")
    }

    pub fn workflows_dir(&self) -> PathBuf {
        self.home.join("workflows")
    }

    /// Global `#Trigger` definitions dir (`<home>/triggers/*.cue`). Repo-local
    /// triggers live at `<repo>/.rk/triggers.cue` instead.
    pub fn triggers_dir(&self) -> PathBuf {
        self.home.join("triggers")
    }

    /// Global `#Schedule` definitions dir (`<home>/schedules/*.cue`). Repo-local
    /// schedules live at `<repo>/.rk/schedules.cue` instead.
    pub fn schedules_dir(&self) -> PathBuf {
        self.home.join("schedules")
    }

    /// Create the directories the daemon needs at startup.
    pub fn ensure(&self) -> crate::Result<()> {
        for dir in [self.home.clone(), self.log_dir(), self.worktrees_dir()] {
            std::fs::create_dir_all(&dir)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_paths_are_under_home() {
        let l = Layout::at("/tmp/rk-test");
        assert_eq!(l.socket_path(), PathBuf::from("/tmp/rk-test/rk.sock"));
        assert_eq!(l.db_path(), PathBuf::from("/tmp/rk-test/space.db"));
        assert!(l.config_file().starts_with(l.home()));
    }
}
