//! `xmtp-bridge.toml`. Relative paths are resolved against the config file's
//! directory. Secrets are referenced by file path only.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::Context;
use bridge_core::bucket::Rate;
use bridge_core::limiter::{InboundLimits, XmtpOutboundLimits};
use bridge_core::pacer::OutboundLimits;
use bridge_core::router::{Notices, RouterConfig};
use bridge_core::secrets::{read_hex_secret, read_secret_file};
use bridge_core::worker::WorkerSpec;
use frontend_shoutbox::ShoutboxSettings;
use frontend_signal::SignalSettings;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default = "default_state_path")]
    state_path: PathBuf,
    xmtp: XmtpConfig,
    signal: Option<SignalConfig>,
    shoutbox: Option<ShoutboxConfig>,
    #[serde(default)]
    limits: Limits,
    #[serde(default)]
    notices: NoticeOverrides,
}

fn default_state_path() -> PathBuf {
    PathBuf::from("data/bridge-state.json")
}

/// The XMTP side.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct XmtpConfig {
    /// `production`, `dev` or `local`; must match the agent's network.
    pub env: String,
    /// The agent's Ethereum address or XMTP inbox id.
    pub agent: String,
    /// Local XMTP database.
    #[serde(default = "default_xmtp_db")]
    pub db_path: PathBuf,
    /// The bridge's Ethereum private key (hex).
    pub key_file: PathBuf,
    /// 32-byte XMTP database key (hex).
    pub db_key_file: PathBuf,
    /// Worker binary; defaults to `xmtp-bridge-agent` next to this binary.
    pub worker: Option<PathBuf>,
}

fn default_xmtp_db() -> PathBuf {
    PathBuf::from("data/xmtp.db3")
}

/// The Signal front end.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalConfig {
    /// presage store (encrypted).
    #[serde(default = "default_signal_store")]
    pub store_path: PathBuf,
    /// Store passphrase.
    pub passphrase_file: PathBuf,
    /// Name in the phone's linked-devices list.
    #[serde(default = "default_device_name")]
    pub device_name: String,
}

fn default_signal_store() -> PathBuf {
    PathBuf::from("data/signal.db3")
}

fn default_device_name() -> String {
    "xmtp-bridge".into()
}

/// The web3 shoutbox front end: a second XMTP identity that room leaders add
/// to their groups.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShoutboxConfig {
    /// Local XMTP database of the room identity.
    #[serde(default = "default_shoutbox_db")]
    pub db_path: PathBuf,
    /// The room identity's Ethereum private key (hex). Must differ from
    /// `xmtp.key_file`: this is the address shoutbox leaders add to rooms.
    pub key_file: PathBuf,
    /// 32-byte database key (hex).
    pub db_key_file: PathBuf,
    /// Worker binary; defaults to `xmtp-bridge-room` next to this binary.
    pub worker: Option<PathBuf>,
    /// Lines one person may get through to the agent.
    #[serde(default = "default_shoutbox_per_user")]
    pub per_user: Rate,
    /// Most people tracked by the per-person buckets.
    #[serde(default = "default_shoutbox_tracked_users")]
    pub max_tracked_users: usize,
    /// If non-empty, only lines containing one of these words are forwarded.
    #[serde(default)]
    pub triggers: Vec<String>,
    /// Post the bridge's own notices into rooms (default: no).
    #[serde(default)]
    pub post_notices: bool,
}

fn default_shoutbox_db() -> PathBuf {
    PathBuf::from("data/shoutbox-xmtp.db3")
}

const fn default_shoutbox_per_user() -> Rate {
    Rate::new(2, 4, 60)
}

const fn default_shoutbox_tracked_users() -> usize {
    10_000
}

/// Every limit, with defaults.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    /// Messages one sender may send to the agent.
    pub per_sender: Rate,
    /// Messages all senders together may send to the agent.
    pub global: Rate,
    /// New conversations (first messages) the bridge accepts.
    pub new_senders: Rate,
    /// Messages the bridge sends to one user.
    pub outbound_per_recipient: Rate,
    /// Messages the bridge sends per network in total.
    pub outbound_global: Rate,
    /// Calls the bridge makes to XMTP (send or open-thread) per user.
    pub xmtp_per_user: Rate,
    /// Calls the bridge makes to XMTP (send or open-thread) in total.
    pub xmtp_global: Rate,
    /// Most users tracked by the XMTP outbound buckets.
    pub max_tracked_xmtp_users: usize,
    /// Most user sessions kept; least recently active are evicted beyond it.
    pub max_sessions: usize,
    /// Most senders tracked by the per-sender buckets.
    pub max_tracked_senders: usize,
    /// Longest accepted message, in characters.
    pub max_message_chars: usize,
    /// At most one bridge notice per user per this many seconds.
    pub notice_cooldown_secs: u64,
    /// Outbound queue length per user.
    pub outbound_queue_per_recipient: usize,
    /// Outbound queue length per network.
    pub outbound_queue_total: usize,
    /// Undelivered messages held while the agent link is down.
    pub max_inflight: usize,
    /// Hold admitted messages while the agent link is down (`true`) or drop
    /// them (`false`; what a live room wants).
    pub hold_while_offline: bool,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            per_sender: Rate::new(3, 5, 60),
            global: Rate::new(10, 30, 60),
            new_senders: Rate::new(5, 20, 3600),
            outbound_per_recipient: Rate::new(3, 1, 1),
            outbound_global: Rate::new(10, 2, 1),
            xmtp_per_user: Rate::new(3, 5, 60),
            xmtp_global: Rate::new(10, 30, 60),
            max_tracked_xmtp_users: 10_000,
            max_sessions: 500,
            max_tracked_senders: 10_000,
            max_message_chars: 4000,
            notice_cooldown_secs: 60,
            outbound_queue_per_recipient: 20,
            outbound_queue_total: 1000,
            max_inflight: 1000,
            hold_while_offline: true,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct NoticeOverrides {
    rate_limited: Option<String>,
    busy: Option<String>,
    new_sender_busy: Option<String>,
    too_long: Option<String>,
    agent_unavailable: Option<String>,
    still_connecting: Option<String>,
    agent_offline: Option<String>,
    xmtp_rate_limited: Option<String>,
}

/// Loaded configuration with absolute paths.
#[derive(Debug)]
pub struct Config {
    /// Session state file.
    pub state_path: PathBuf,
    /// XMTP side.
    pub xmtp: XmtpConfig,
    /// Signal front end, if configured.
    pub signal: Option<SignalConfig>,
    /// Shoutbox front end, if configured.
    pub shoutbox: Option<ShoutboxConfig>,
    /// Limits.
    pub limits: Limits,
    notices: Notices,
}

impl Config {
    /// Load and validate `path`.
    ///
    /// # Errors
    /// Unreadable or invalid configuration.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
        let raw: RawConfig = toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
        let base = path
            .canonicalize()
            .with_context(|| format!("resolving {}", path.display()))?
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default();
        Self::from_raw(raw, &base)
    }

    fn from_raw(raw: RawConfig, base: &Path) -> anyhow::Result<Self> {
        let abs = |p: &Path| if p.is_absolute() { p.to_path_buf() } else { base.join(p) };
        anyhow::ensure!(
            matches!(raw.xmtp.env.as_str(), "production" | "dev" | "local"),
            "xmtp.env must be production, dev or local (got {:?})",
            raw.xmtp.env
        );
        anyhow::ensure!(!raw.xmtp.agent.trim().is_empty(), "xmtp.agent is required");
        anyhow::ensure!(
            raw.signal.is_some() || raw.shoutbox.is_some(),
            "no front end configured (add a [signal] or [shoutbox] section)"
        );
        if let Some(shoutbox) = &raw.shoutbox {
            shoutbox.per_user.validate("shoutbox.per_user").map_err(anyhow::Error::msg)?;
            anyhow::ensure!(shoutbox.max_tracked_users > 0, "shoutbox.max_tracked_users must be positive");
        }
        let l = &raw.limits;
        for (name, rate) in [
            ("limits.per_sender", l.per_sender),
            ("limits.global", l.global),
            ("limits.new_senders", l.new_senders),
            ("limits.outbound_per_recipient", l.outbound_per_recipient),
            ("limits.outbound_global", l.outbound_global),
            ("limits.xmtp_per_user", l.xmtp_per_user),
            ("limits.xmtp_global", l.xmtp_global),
        ] {
            rate.validate(name).map_err(anyhow::Error::msg)?;
        }
        anyhow::ensure!(l.max_sessions > 0 && l.max_tracked_senders > 0, "session and sender limits must be positive");
        anyhow::ensure!(l.max_tracked_xmtp_users > 0, "limits.max_tracked_xmtp_users must be positive");
        anyhow::ensure!(l.max_message_chars > 0, "limits.max_message_chars must be positive");
        anyhow::ensure!(
            l.outbound_queue_per_recipient > 0 && l.outbound_queue_total >= l.outbound_queue_per_recipient,
            "outbound queue limits must be positive, total ≥ per recipient"
        );

        let defaults = Notices::default();
        let n = raw.notices;
        let notices = Notices {
            rate_limited: n.rate_limited.unwrap_or(defaults.rate_limited),
            busy: n.busy.unwrap_or(defaults.busy),
            new_sender_busy: n.new_sender_busy.unwrap_or(defaults.new_sender_busy),
            too_long: n.too_long.unwrap_or(defaults.too_long),
            agent_unavailable: n.agent_unavailable.unwrap_or(defaults.agent_unavailable),
            still_connecting: n.still_connecting.unwrap_or(defaults.still_connecting),
            agent_offline: n.agent_offline.unwrap_or(defaults.agent_offline),
            xmtp_rate_limited: n.xmtp_rate_limited.unwrap_or(defaults.xmtp_rate_limited),
        };

        let mut xmtp = raw.xmtp;
        xmtp.db_path = abs(&xmtp.db_path);
        xmtp.key_file = abs(&xmtp.key_file);
        xmtp.db_key_file = abs(&xmtp.db_key_file);
        xmtp.worker = xmtp.worker.as_deref().map(abs);
        let signal = raw.signal.map(|mut s| {
            s.store_path = abs(&s.store_path);
            s.passphrase_file = abs(&s.passphrase_file);
            s
        });
        let shoutbox = raw.shoutbox.map(|mut s| {
            s.db_path = abs(&s.db_path);
            s.key_file = abs(&s.key_file);
            s.db_key_file = abs(&s.db_key_file);
            s.worker = s.worker.as_deref().map(abs);
            s
        });
        if let Some(s) = &shoutbox {
            anyhow::ensure!(
                s.key_file != xmtp.key_file,
                "shoutbox.key_file must be a different key from xmtp.key_file (the room identity is separate)"
            );
            anyhow::ensure!(s.db_path != xmtp.db_path, "shoutbox.db_path must differ from xmtp.db_path");
        }
        Ok(Self { state_path: abs(&raw.state_path), xmtp, signal, shoutbox, limits: raw.limits, notices })
    }

    /// Router settings.
    #[must_use]
    pub fn router_config(&self) -> RouterConfig {
        let l = &self.limits;
        RouterConfig {
            limits: InboundLimits {
                per_sender: l.per_sender,
                global: l.global,
                new_senders: l.new_senders,
                max_tracked_senders: l.max_tracked_senders,
            },
            xmtp_limits: XmtpOutboundLimits {
                per_user: l.xmtp_per_user,
                global: l.xmtp_global,
                max_tracked_users: l.max_tracked_xmtp_users,
            },
            max_sessions: l.max_sessions,
            max_message_chars: l.max_message_chars,
            notice_cooldown_secs: l.notice_cooldown_secs,
            max_queued_while_opening: 5,
            max_send_attempts: 3,
            max_inflight: l.max_inflight,
            dedupe_capacity: 10_000,
            hold_while_offline: l.hold_while_offline,
            notices: self.notices.clone(),
        }
    }

    /// Outbound pacing for one network.
    #[must_use]
    pub const fn outbound_limits(&self) -> OutboundLimits {
        let l = &self.limits;
        OutboundLimits {
            per_recipient: l.outbound_per_recipient,
            global: l.outbound_global,
            max_queue_per_recipient: l.outbound_queue_per_recipient,
            max_queue_total: l.outbound_queue_total,
        }
    }

    /// Signal settings with the passphrase read from its file.
    ///
    /// # Errors
    /// No `[signal]` section, or an unreadable passphrase file.
    pub fn signal_settings(&self) -> anyhow::Result<SignalSettings> {
        let signal = self.signal.as_ref().context("no [signal] section in the config")?;
        Ok(SignalSettings {
            store_path: signal.store_path.clone(),
            passphrase: read_secret_file(&signal.passphrase_file)?,
            device_name: signal.device_name.clone(),
        })
    }

    /// Shoutbox settings, including how to start its worker.
    ///
    /// # Errors
    /// No `[shoutbox]` section, or the worker binary cannot be found.
    pub fn shoutbox_settings(&self) -> anyhow::Result<ShoutboxSettings> {
        let s = self.shoutbox.as_ref().context("no [shoutbox] section in the config")?;
        let program = resolve_worker(s.worker.as_deref(), "xmtp-bridge-room", "shoutbox.worker")?;
        let args = vec![
            "--env".into(),
            self.xmtp.env.clone().into(),
            "--db-path".into(),
            s.db_path.clone().into_os_string(),
            "--key-file".into(),
            s.key_file.clone().into_os_string(),
            "--db-key-file".into(),
            s.db_key_file.clone().into_os_string(),
        ];
        Ok(ShoutboxSettings {
            worker: WorkerSpec { program, args },
            per_user: s.per_user,
            max_tracked_users: s.max_tracked_users,
            triggers: s.triggers.clone(),
            post_notices: s.post_notices,
        })
    }

    /// The agent worker program and its arguments (paths only; it reads the
    /// secrets itself).
    ///
    /// # Errors
    /// When the worker binary cannot be found.
    pub fn worker_command(&self) -> anyhow::Result<(PathBuf, Vec<OsString>)> {
        let program = resolve_worker(self.xmtp.worker.as_deref(), "xmtp-bridge-agent", "xmtp.worker")?;
        let args = vec![
            "--env".into(),
            self.xmtp.env.clone().into(),
            "--agent".into(),
            self.xmtp.agent.clone().into(),
            "--db-path".into(),
            self.xmtp.db_path.clone().into_os_string(),
            "--key-file".into(),
            self.xmtp.key_file.clone().into_os_string(),
            "--db-key-file".into(),
            self.xmtp.db_key_file.clone().into_os_string(),
        ];
        Ok((program, args))
    }

    /// Create the directories holding state and databases, owner-only
    /// (mode 700) when this creates them. The databases inside are encrypted;
    /// the directory mode keeps their metadata private too.
    ///
    /// # Errors
    /// I/O failures.
    pub fn prepare_dirs(&self) -> anyhow::Result<()> {
        let mut paths = vec![&self.state_path, &self.xmtp.db_path];
        if let Some(signal) = &self.signal {
            paths.push(&signal.store_path);
        }
        if let Some(shoutbox) = &self.shoutbox {
            paths.push(&shoutbox.db_path);
        }
        for dir in paths.into_iter().filter_map(|p| p.parent()) {
            if dir.exists() {
                continue;
            }
            let mut builder = std::fs::DirBuilder::new();
            builder.recursive(true);
            #[cfg(unix)]
            std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
            builder.create(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
        Ok(())
    }

    /// Check every secret file and the worker binary without connecting.
    ///
    /// # Errors
    /// The first problem found.
    pub fn check(&self) -> anyhow::Result<()> {
        let key = check_private_key(&self.xmtp.key_file)?;
        read_hex_secret(&self.xmtp.db_key_file, 32)?;
        if self.signal.is_some() {
            self.signal_settings()?;
        }
        if let Some(shoutbox) = &self.shoutbox {
            let room_key = check_private_key(&shoutbox.key_file)?;
            anyhow::ensure!(
                room_key != key,
                "shoutbox.key_file holds the same key as xmtp.key_file; the room identity must be separate"
            );
            read_hex_secret(&shoutbox.db_key_file, 32)?;
            self.shoutbox_settings()?;
        }
        self.worker_command()?;
        Ok(())
    }
}

/// Read a private-key file and return the key, lower-case hex without prefix.
fn check_private_key(path: &Path) -> anyhow::Result<String> {
    let key = read_secret_file(path)?;
    let key = key.strip_prefix("0x").unwrap_or(&key);
    anyhow::ensure!(
        key.len() == 64 && key.bytes().all(|b| b.is_ascii_hexdigit()),
        "{} must hold a 32-byte hex private key",
        path.display()
    );
    Ok(key.to_lowercase())
}

/// A worker binary: the configured path, or `name` next to this executable.
fn resolve_worker(configured: Option<&Path>, name: &str, key: &str) -> anyhow::Result<PathBuf> {
    let program = match configured {
        Some(path) => path.to_path_buf(),
        None => std::env::current_exe()?.parent().context("locating this binary's directory")?.join(name),
    };
    anyhow::ensure!(program.is_file(), "worker not found at {} (set {key})", program.display());
    Ok(program)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> anyhow::Result<Config> {
        Config::from_raw(toml::from_str(text)?, Path::new("/srv/bridge"))
    }

    const MINIMAL: &str = r#"
        [xmtp]
        env = "production"
        agent = "0xa85dD3FbD8C2c831Ef156036F14638CcFf03b44e"
        key_file = "secrets/evm.key"
        db_key_file = "/abs/db.key"

        [signal]
        passphrase_file = "secrets/signal.pass"
    "#;

    #[test]
    fn minimal_config_gets_defaults_and_absolute_paths() {
        let c = parse(MINIMAL).unwrap();
        assert_eq!(c.state_path, PathBuf::from("/srv/bridge/data/bridge-state.json"));
        assert_eq!(c.xmtp.key_file, PathBuf::from("/srv/bridge/secrets/evm.key"));
        assert_eq!(c.xmtp.db_key_file, PathBuf::from("/abs/db.key"));
        assert_eq!(c.signal.as_ref().unwrap().store_path, PathBuf::from("/srv/bridge/data/signal.db3"));
        assert_eq!(c.router_config().limits.per_sender, Rate::new(3, 5, 60));
    }

    #[test]
    fn limits_and_notices_can_be_overridden() {
        let text = format!(
            "{MINIMAL}\n[limits]\nper_sender = {{ burst = 1, tokens = 1, per_secs = 10 }}\n[notices]\nbusy = \"later\"\n"
        );
        let c = parse(&text).unwrap();
        assert_eq!(c.limits.per_sender, Rate::new(1, 1, 10));
        assert_eq!(c.limits.global, Limits::default().global, "unset limits keep defaults");
        assert_eq!(c.router_config().notices.busy, "later");
    }

    #[test]
    fn shipped_example_config_is_valid() {
        let example = include_str!("../../../config.example.toml");
        let c = parse(example).unwrap();
        let defaults = Limits::default();
        assert_eq!(c.limits.per_sender, defaults.per_sender, "example documents the defaults");
        assert_eq!(c.limits.outbound_global, defaults.outbound_global);
        assert_eq!(c.limits.max_inflight, defaults.max_inflight);
        assert_eq!(c.limits.xmtp_per_user, defaults.xmtp_per_user);
        assert_eq!(c.limits.xmtp_global, defaults.xmtp_global);
        assert_eq!(c.limits.max_tracked_xmtp_users, defaults.max_tracked_xmtp_users);
    }

    #[test]
    fn shoutbox_section_is_parsed_and_checked() {
        let text = r#"
            [xmtp]
            env = "production"
            agent = "0xa85dD3FbD8C2c831Ef156036F14638CcFf03b44e"
            key_file = "secrets/evm.key"
            db_key_file = "secrets/db.key"

            [shoutbox]
            key_file = "secrets/room.key"
            db_key_file = "secrets/room-db.key"
            triggers = ["@agent"]

            [limits]
            hold_while_offline = false
        "#;
        let c = parse(text).unwrap();
        let s = c.shoutbox.as_ref().unwrap();
        assert_eq!(s.db_path, PathBuf::from("/srv/bridge/data/shoutbox-xmtp.db3"));
        assert_eq!(s.key_file, PathBuf::from("/srv/bridge/secrets/room.key"));
        assert_eq!(s.per_user, Rate::new(2, 4, 60), "default per-person rate");
        assert_eq!(s.triggers, vec!["@agent".to_owned()]);
        assert!(!s.post_notices, "notices stay out of rooms by default");
        assert!(c.signal.is_none());
        assert!(!c.router_config().hold_while_offline);
        // The room identity must be its own key.
        let same = text.replace("secrets/room.key", "secrets/evm.key");
        assert!(parse(&same).unwrap_err().to_string().contains("different key"));
        // Some front end is required.
        let none = text.split("[shoutbox]").next().unwrap();
        assert!(parse(none).unwrap_err().to_string().contains("no front end"));
    }

    #[test]
    fn rejects_bad_values() {
        assert!(parse(&MINIMAL.replace("production", "mainnet")).is_err());
        let zero = format!("{MINIMAL}\n[limits]\nglobal = {{ burst = 0, tokens = 1, per_secs = 1 }}\n");
        assert!(parse(&zero).is_err());
        let typo = format!("{MINIMAL}\n[limits]\nper_sendr = {{ burst = 1, tokens = 1, per_secs = 1 }}\n");
        assert!(parse(&typo).is_err(), "unknown keys are rejected");
    }
}
