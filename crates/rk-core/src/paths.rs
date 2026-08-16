//! Path layout. Everything lives under one home directory (default
//! `~/.rat-kingdom`, override with `RK_HOME`) so state is predictable and
//! socket paths stay short enough for `sockaddr_un`.

use std::path::{Path, PathBuf};

const AUTH_TOKEN_BYTES: usize = 32;

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

    /// The `flock`-held singleton lock: exactly one daemon may hold this file
    /// exclusively at a time, so a second daemon started against the same
    /// `RK_HOME` (stray build, leaked test daemon, imprecise kill) refuses to
    /// bind instead of contending with the first over the tuplespace WAL.
    pub fn lockfile_path(&self) -> PathBuf {
        self.home.join("rk.lock")
    }

    /// The bearer token used to authenticate local RPC clients. It is kept
    /// separate from the castle signing key: the token authorizes this local
    /// daemon connection, while the castle key authenticates replicated notes.
    pub fn auth_token_path(&self) -> PathBuf {
        self.home.join("auth.token")
    }

    /// Create or read the per-layout RPC bearer token. The file is deliberately
    /// owner-readable only; callers should not put it in logs or tuple payloads.
    pub fn auth_token(&self) -> crate::Result<String> {
        self.ensure()?;
        let path = self.auth_token_path();
        if path.exists() {
            let token = std::fs::read_to_string(&path)?.trim().to_string();
            if token.len() < AUTH_TOKEN_BYTES * 2 {
                return Err(crate::Error::Config(format!(
                    "{} is too short; remove it and restart rk",
                    path.display()
                )));
            }
            set_private_permissions(&path)?;
            return Ok(token);
        }

        let mut bytes = [0u8; AUTH_TOKEN_BYTES];
        rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut bytes);
        let token = hex::encode(bytes);
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => {
                set_private_permissions(&path)?;
                use std::io::Write;
                let mut file = file;
                writeln!(file, "{token}")?;
                Ok(token)
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let token = std::fs::read_to_string(&path)?.trim().to_string();
                if token.len() < AUTH_TOKEN_BYTES * 2 {
                    return Err(crate::Error::Config(format!(
                        "{} is incomplete; remove it and restart rk",
                        path.display()
                    )));
                }
                set_private_permissions(&path)?;
                Ok(token)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Derive a scoped bearer token for a supervised agent without exposing
    /// the operator token in the agent environment. The server recomputes the
    /// same value from its owner-readable root token.
    pub fn agent_auth_token(&self, agent: &str) -> crate::Result<String> {
        Ok(derive_agent_token(&self.auth_token()?, agent))
    }

    pub fn db_path(&self) -> PathBuf {
        self.home.join("space.db")
    }

    /// The persisted per-castle Ed25519 keypair (32-byte seed, hex, 0600). Its
    /// public key is this castle's stable, authenticated replication actor id.
    pub fn castle_key_path(&self) -> PathBuf {
        self.home.join("castle.key")
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
        set_private_directory_permissions(&self.home)?;
        Ok(())
    }
}

pub fn derive_agent_token(root: &str, agent: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"rat-kingdom-agent-token\0");
    digest.update(root.as_bytes());
    digest.update(b"\0");
    digest.update(agent.as_bytes());
    hex::encode(digest.finalize())
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> crate::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &Path) -> crate::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> crate::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> crate::Result<()> {
    Ok(())
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

    #[test]
    fn auth_token_is_stable_private_and_agent_scoped() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        let root = layout.auth_token().unwrap();
        assert_eq!(root, layout.auth_token().unwrap());
        assert_ne!(
            layout.agent_auth_token("A").unwrap(),
            layout.agent_auth_token("B").unwrap()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(layout.auth_token_path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
    }
}
