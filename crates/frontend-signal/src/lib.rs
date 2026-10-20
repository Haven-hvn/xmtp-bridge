//! Signal front end for `xmtp-bridge`, built on [presage].
//!
//! The bridge runs as a **linked device** of a dedicated Signal account: the
//! account's phone (Signal for iOS/Android) stays the primary device, and
//! [`link_device`] prints a QR code to scan from it under
//! *Settings → Linked devices*. Anyone on any Signal app can then message that
//! number.
//!
//! Only 1:1 text messages are relayed. Group messages, stories, reactions,
//! edits, calls and attachments are ignored. Messages are sent and received by
//! Signal service id (ACI), never by phone number.
//!
//! presage's futures are not `Send`, so the front end runs on its own thread
//! with a single-threaded Tokio runtime and a `LocalSet`.
//!
//! [presage]: https://github.com/whisperfish/presage

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use bridge_core::message::{Frontend, FrontendEvent, FrontendFuture, FrontendIo, Inbound, Outbound, UserAddress};
use bridge_core::text::split_message;
use futures::channel::oneshot;
use futures::{StreamExt, future, pin_mut};
use presage::Manager;
use presage::libsignal_service::configuration::SignalServers;
use presage::libsignal_service::content::{Content, ContentBody};
use presage::libsignal_service::protocol::ServiceId;
use presage::manager::Registered;
use presage::model::identity::OnNewIdentity;
use presage::model::messages::Received;
use presage::proto::DataMessage;
use presage_store_sqlite::SqliteStore;
use tokio::sync::{mpsc, watch};

/// Network id used in [`UserAddress::network`].
pub const NETWORK: &str = "signal";

/// Signal sends long bodies as a "long text" attachment above roughly 2 KB,
/// so replies are split below that.
pub const MAX_BODY_BYTES: usize = 2000;

const SEND_ATTEMPTS: u32 = 3;
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// Signal front-end settings.
#[derive(Clone)]
pub struct SignalSettings {
    /// presage SQLite store (SQLCipher-encrypted with `passphrase`).
    pub store_path: PathBuf,
    /// Store encryption passphrase.
    pub passphrase: String,
    /// Name shown in the phone's linked-devices list.
    pub device_name: String,
}

impl std::fmt::Debug for SignalSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignalSettings")
            .field("store_path", &self.store_path)
            .field("device_name", &self.device_name)
            .finish_non_exhaustive()
    }
}

async fn open_store(settings: &SignalSettings) -> anyhow::Result<SqliteStore> {
    if let Some(dir) = settings.store_path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let url = format!("sqlite://{}", settings.store_path.display());
    SqliteStore::open_with_passphrase(&url, Some(&settings.passphrase), OnNewIdentity::Trust)
        .await
        .with_context(|| format!("opening Signal store {}", settings.store_path.display()))
}

/// Link the bridge as a secondary device: prints a QR code (and the URL) to
/// stdout and waits until it is scanned from the phone.
///
/// # Errors
/// Store or provisioning failures.
pub fn link_device(settings: SignalSettings) -> anyhow::Result<()> {
    run_local(async move {
        let store = open_store(&settings).await?;
        let (url_tx, url_rx) = oneshot::channel();
        let (linked, ()) = future::join(
            Manager::link_secondary_device(store, SignalServers::Production, settings.device_name.clone(), url_tx),
            async move {
                match url_rx.await {
                    Ok(url) => {
                        println!("On the bridge's phone, open Signal → Settings → Linked devices → +, and scan:\n");
                        if let Err(error) = qr2term::print_qr(url.to_string()) {
                            println!("(could not render a QR code: {error})");
                        }
                        println!("\nOr paste this link into a QR generator: {url}");
                    }
                    Err(_) => println!("linking was cancelled"),
                }
            },
        )
        .await;
        let manager = linked.context("linking failed")?;
        let aci = manager.registration_data().service_ids.aci();
        println!("Linked. Bridge account service id: {}", ServiceId::from(aci).service_id_string());
        Ok(())
    })
}

/// The Signal front end.
#[derive(Debug)]
pub struct SignalFrontend {
    settings: SignalSettings,
}

impl SignalFrontend {
    /// A front end over an already linked store.
    #[must_use]
    pub const fn new(settings: SignalSettings) -> Self {
        Self { settings }
    }
}

impl Frontend for SignalFrontend {
    fn network(&self) -> &str {
        NETWORK
    }

    fn run(self: Box<Self>, io: FrontendIo) -> FrontendFuture {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let spawned = std::thread::Builder::new().name("signal".into()).spawn(move || {
            let result = run_local(run(self.settings, io));
            let _ = done_tx.send(result);
        });
        Box::pin(async move {
            spawned.context("spawning the Signal thread")?;
            done_rx.await.context("Signal thread exited without a result")?
        })
    }
}

fn run_local<F: std::future::Future<Output = anyhow::Result<()>>>(future: F) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, future)
}

async fn run(settings: SignalSettings, io: FrontendIo) -> anyhow::Result<()> {
    let FrontendIo { events, outbound, shutdown } = io;
    let store = open_store(&settings).await?;
    let manager = Manager::load_registered(store)
        .await
        .context("no linked Signal account in the store; run `xmtp-bridge link-signal` first")?;
    let own = ServiceId::from(manager.registration_data().service_ids.aci());
    tracing::info!(account = %own.service_id_string(), "Signal front end started");

    // Hold sends until the first receive pass has drained the server queue,
    // as presage recommends, so sessions and profile keys are current.
    let (synced_tx, synced_rx) = watch::channel(false);
    let receiver =
        tokio::task::spawn_local(receive_loop(manager.clone(), own, events.clone(), synced_tx, shutdown.clone()));
    let sender = send_loop(manager, outbound, events, synced_rx, shutdown);
    let (received, sent) = future::join(receiver, sender).await;
    received.context("Signal receive task panicked")??;
    sent
}

async fn receive_loop(
    manager: Manager<SqliteStore, Registered>,
    own: ServiceId,
    events: mpsc::Sender<FrontendEvent>,
    synced: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    loop {
        let mut receiving = manager.clone();
        match receiving.receive_messages().await {
            Ok(stream) => {
                pin_mut!(stream);
                loop {
                    tokio::select! {
                        _ = shutdown.changed() => return Ok(()),
                        item = stream.next() => match item {
                            Some(Received::QueueEmpty) => {
                                let _ = synced.send(true);
                            }
                            Some(Received::Content(content)) => {
                                if let Some(inbound) = to_inbound(&content, &own)
                                    && events.send(FrontendEvent::Message(inbound)).await.is_err()
                                {
                                    return Ok(()); // router gone
                                }
                            }
                            Some(Received::DecryptionError(sender)) => {
                                tracing::warn!(sender = %sender.service_id_string(), "could not decrypt a Signal message");
                            }
                            Some(Received::Contacts) => {}
                            None => break,
                        }
                    }
                }
                tracing::warn!("Signal message stream ended; reconnecting");
            }
            Err(error) => tracing::error!(%error, "Signal receive failed; retrying"),
        }
        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            () = tokio::time::sleep(RECONNECT_DELAY) => {}
        }
    }
}

/// Translate a decrypted Signal message into an [`Inbound`], or `None` for
/// anything the bridge does not relay.
fn to_inbound(content: &Content, own: &ServiceId) -> Option<Inbound> {
    let sender = content.metadata.sender;
    if &sender == own {
        return None;
    }
    let ContentBody::DataMessage(DataMessage { body: Some(body), group_v2: None, timestamp, .. }) = &content.body
    else {
        return None;
    };
    let sender_id = sender.service_id_string();
    let timestamp =
        timestamp.unwrap_or_else(|| u64::try_from(content.metadata.client_timestamp.timestamp_millis()).unwrap_or(0));
    Some(Inbound {
        from: UserAddress::new(NETWORK, sender_id.clone()),
        // Signal identifies a message by (sender, timestamp).
        id: format!("{sender_id}/{timestamp}"),
        text: body.clone(),
    })
}

async fn send_loop(
    mut manager: Manager<SqliteStore, Registered>,
    mut outbound: mpsc::Receiver<Outbound>,
    events: mpsc::Sender<FrontendEvent>,
    mut synced: watch::Receiver<bool>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    tokio::select! {
        _ = shutdown.changed() => return Ok(()),
        ready = synced.wait_for(|synced| *synced) => { ready.context("receive loop stopped before syncing")?; }
    }
    loop {
        let message = tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            message = outbound.recv() => match message { Some(m) => m, None => return Ok(()) },
        };
        let result = deliver(&mut manager, &message).await;
        if let Some(ack) = message.ack {
            let event = match result {
                Ok(()) => FrontendEvent::Delivered(ack),
                Err(error) => FrontendEvent::Failed { ack, reason: format!("{error:#}") },
            };
            let _ = events.send(event).await;
        } else if let Err(error) = result {
            tracing::warn!(to = %message.to, error = %format!("{error:#}"), "notice not delivered");
        }
    }
}

async fn deliver(manager: &mut Manager<SqliteStore, Registered>, message: &Outbound) -> anyhow::Result<()> {
    let recipient = ServiceId::parse_from_service_id_string(&message.to.user)
        .with_context(|| format!("not a Signal service id: {}", message.to.user))?;
    for chunk in split_message(&message.text, MAX_BODY_BYTES) {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let timestamp = now_millis();
            let body = DataMessage { body: Some(chunk.clone()), timestamp: Some(timestamp), ..Default::default() };
            match manager.send_message(recipient, body, timestamp).await {
                Ok(()) => break,
                Err(error) if attempt < SEND_ATTEMPTS => {
                    tracing::warn!(to = %message.to, attempt, %error, "Signal send failed; retrying");
                    tokio::time::sleep(Duration::from_secs(2u64.pow(attempt))).await;
                }
                Err(error) => return Err(error).context("sending to Signal"),
            }
        }
    }
    Ok(())
}

fn now_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use presage::libsignal_service::content::Metadata;
    use presage::libsignal_service::prelude::Uuid;
    use presage::libsignal_service::protocol::{Aci, DeviceId};
    use presage::proto::GroupContextV2;

    use super::*;

    fn aci(n: u8) -> ServiceId {
        ServiceId::from(Aci::from_uuid_bytes([n; 16]))
    }

    fn content(sender: ServiceId, body: ContentBody) -> Content {
        let at = chrono_from_millis(1_700_000_000_000);
        Content {
            metadata: Metadata {
                sender,
                destination: aci(1),
                sender_device: DeviceId::new(1).expect("valid device id"),
                pni_verified: None,
                client_timestamp: at,
                server_timestamp: at,
                needs_receipt: false,
                unidentified_sender: true,
                was_plaintext: false,
                server_guid: Some(Uuid::nil()),
            },
            body,
        }
    }

    fn chrono_from_millis(ms: i64) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp_millis(ms).expect("valid timestamp")
    }

    fn text(body: &str, group: bool) -> ContentBody {
        ContentBody::DataMessage(DataMessage {
            body: Some(body.into()),
            timestamp: Some(42),
            group_v2: group.then(GroupContextV2::default),
            ..Default::default()
        })
    }

    #[test]
    fn relays_direct_text_with_a_stable_id() {
        let own = aci(1);
        let inbound = to_inbound(&content(aci(2), text("hi", false)), &own).expect("relayed");
        let sender = aci(2).service_id_string();
        assert_eq!(inbound.from, UserAddress::new(NETWORK, sender.clone()));
        assert_eq!(inbound.id, format!("{sender}/42"));
        assert_eq!(inbound.text, "hi");
        assert_eq!(
            ServiceId::parse_from_service_id_string(&inbound.from.user),
            Some(aci(2)),
            "reply address round-trips"
        );
    }

    #[test]
    fn ignores_own_group_and_non_text_messages() {
        let own = aci(1);
        assert!(to_inbound(&content(own, text("echo", false)), &own).is_none(), "own message");
        assert!(to_inbound(&content(aci(2), text("in a group", true)), &own).is_none(), "group message");
        let reaction = ContentBody::DataMessage(DataMessage { body: None, ..Default::default() });
        assert!(to_inbound(&content(aci(2), reaction), &own).is_none(), "no body");
    }
}
