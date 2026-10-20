# xmtp-bridge

A compatibility proxy between an XMTP agent (such as the dsh web3 agent's
`dsh-channel-xmtp`) and people on other messaging networks: Signal, and the
[web3 shoutbox](#shoutbox-front-end)'s XMTP rooms. The bridge is written in
Rust, uses token-bucket rate limiting in both directions, and opens no
listening port.

Licensed **AGPL-3.0-only** (required by presage and libsignal, which run
in-process).

```text
Signal users   ⇄ Signal front end                          ⇄ router ⇄ xmtp-bridge-agent ⇄ XMTP ⇄ agent
                 (presage, linked device)                    limits,   (child process,
shoutbox rooms ⇄ xmtp-bridge-room ⇄ shoutbox front end     ⇄ sessions  stdin/stdout)
                 (child process)    (triggers, per-person
                                     rate, drops)
```

## How it works

- **One agent session per user.** For each Signal user, the bridge creates an
  XMTP group containing only itself and the agent. The agent keeps one session
  per conversation (`xmtp-<conversationId>`), so users never share memory. The
  groups use admin-only permissions, so the agent cannot add anyone else.
- **Anyone can message it.** No allowlist. The rate limits below are the abuse
  controls.
- **The agent needs no changes.** The bridge looks like any other XMTP peer:
  plain text in, plain text out. The agent's busy acks and messages it sends on
  its own (background jobs, schedules) are relayed like any reply.
- **XMTP runs in a separate process.** `xmtp-bridge-agent` owns the XMTP
  client and talks to the bridge in JSON lines over pipes
  (`crates/bridge-core/src/protocol.rs`). libxmtp's static library ships its
  own copy of the Rust standard library and its own SQLCipher, so it never
  shares an address space with presage. A crash in one SDK cannot take down
  the other, and the worker never learns who the Signal users are.
- **Restarts lose nothing silently.**
  - Agent messages: each session stores a delivery cursor. After a restart,
    the worker re-reads every thread past its cursor, so replies produced
    while the bridge was down still arrive.
  - User messages: unacknowledged messages to the agent are re-sent when a
    restarted worker comes back, up to 3 attempts. After that the user gets a
    notice. A crash after a send but before its acknowledgement can deliver
    one message to the agent twice.
- **Signal side:** 1:1 text only. Group messages, stories, reactions, edits
  and attachments are ignored. Replies longer than about 2 KB are split.
  Attachments from the agent become a one-line notice.

**Version compatibility:** the agent's `@xmtp/node-sdk` 6.1.0 bundles
libxmtp 1.11.0, and the pinned `qntx-labs/xmtp` revision builds libxmtp
v1.11.0 too.

## Shoutbox front end

The [web3 shoutbox](../web3-shoutbox-platform-main) is a browser widget where
everyone on a page shares one XMTP group that an elected leader recreates
every few minutes. The bridge joins those rooms as a participant and relays
the lines worth answering to the agent:

```text
browser ⇄ XMTP room ⇄ xmtp-bridge-room ⇄ shoutbox front end ⇄ router ⇄ xmtp-bridge-agent ⇄ agent
```

- **Two XMTP identities.** `xmtp-bridge-room` holds the *room* identity
  (`[shoutbox] key_file`), the address shoutbox leaders add to their groups.
  `xmtp-bridge-agent` holds the *link* identity (`[xmtp] key_file`) that
  talks to the agent. The agent's side is never exposed in a public room.
- **Joining needs no GunDB.** Set the shoutbox's
  `VITE_SHOUTBOX_BRIDGE_ADDRESSES` to the room identity's address and its
  leaders add the bridge to every group they create, like any online
  visitor. XMTP group membership is the ground truth; the bridge streams the
  groups it is a member of.
- **The room is the "user".** One agent thread per sliding-window group, so
  the agent sees the conversation and answers into the room. Each forwarded
  line is prefixed with the sender's address (`0xabc…: text`). Under
  `[limits]`, `per_sender` and `xmtp_per_user` are therefore per room and
  `new_senders` caps how many new rooms start an agent thread.
- **It drops, it does not queue.** A busy room is mostly chatter:
  - `triggers = ["@agent"]`: only lines containing a trigger are forwarded
    (checked first, so chatter spends nobody's tokens);
  - `per_user`: lines one person may get through (default burst 2, 4/min);
    the rest are dropped silently;
  - `post_notices = false` (default): the bridge's own notices stay out of
    the room;
  - `limits.hold_while_offline = false`: nothing is held while the agent
    link is down. Replies in flight when the room worker restarts are
    reported failed, not retried.
- **Setup:** create a second key pair (`openssl rand -hex 32` twice, for
  `shoutbox.key_file` and `shoutbox.db_key_file`), uncomment `[shoutbox]` in
  the config, build the room worker (`cargo build --release -p
  agent-link-xmtp` produces both `xmtp-bridge-agent` and
  `xmtp-bridge-room`), run `xmtp-bridge check`, then start the shoutbox with
  the room identity's address in `VITE_SHOUTBOX_BRIDGE_ADDRESSES`. Both must
  be on the same XMTP network (`xmtp.env` ↔ `VITE_XMTP_ENV`). The room
  identity must have an XMTP inbox before a leader can add it; the first
  `xmtp-bridge run` registers it.
- **Trust model:** anyone can create a group containing the room address;
  the limits above are the only access control, same as for Signal.
  Tracked rooms and people are bounded in memory.

## Rate limiting

Each limit is a token bucket: `burst` actions back to back, refilled at
`tokens` per `per_secs` seconds (`crates/bridge-core/src/bucket.rs`).

| Bucket | Default | When empty |
| --- | --- | --- |
| `per_sender` | burst 3, 5/min | message dropped, sender gets a notice |
| `global` (all senders) | burst 10, 30/min | dropped, notice |
| `new_senders` (first messages) | burst 5, 20/hour | no group or session is created, notice |
| `outbound_per_recipient` | burst 3, 1/s | queued, not dropped |
| `outbound_global` (per network) | burst 10, 2/s | queued |

- **Inbound checks run in the order above** (`limiter.rs`). A denied message
  takes no token from any bucket. A sender who is over their own limit
  therefore never uses up the global or new-sender budget.
- **Outbound queues** (`pacer.rs`) are bounded per recipient and in total.
  Recipients are served round-robin, so one busy conversation cannot starve
  the others.
- **Per-key buckets are bounded in memory.** Full buckets are dropped first,
  which loses nothing because a fresh bucket is also full. If every tracked
  bucket is mid-refill, the least recently used one is evicted.
- **Notices are rate-limited too.** Each user gets at most one per
  `notice_cooldown_secs`.
- **Other limits:**
  - Messages over `max_message_chars` are rejected.
  - At most `max_sessions` sessions are kept, evicting the least recently
    active user. An evicted user starts a fresh agent session if they write
    again.
  - At most `max_inflight` messages are held while the agent is offline.

## Build

Requires the Rust toolchain pinned in `rust-toolchain.toml` (rustup installs
it on first use) and git.

```sh
scripts/build-xmtp-ffi.sh                 # builds libxmtp_ffi FROM SOURCE (several minutes)
cargo build --release -p agent-link-xmtp -p bridge
```

`scripts/build-xmtp-ffi.sh` checks out the exact `qntx-labs/xmtp` revision
pinned in `Cargo.toml` into `.deps/` and builds its C-ABI library.
`.cargo/config.toml` points `XMTP_FFI_DIR` at the result. With that variable
set, `xmtp-sys` never downloads its prebuilt binary from GitHub Releases, which
it cannot verify at this revision (the checksum map is empty). If the library
is missing, linking fails rather than falling back to the download. When you
bump the `xmtp` revision in `Cargo.toml`, rerun the script.

Tests (the pure core needs no native libraries):

```sh
cargo test                                # bridge-core + frontend-shoutbox (default members)
cargo test -p bridge-core -p frontend-shoutbox -p frontend-signal -p bridge
```

## Setup

1. **Use a dedicated Signal number** on a phone running Signal, not your
   personal account. The bridge becomes a linked device and receives
   everything sent to that account.
2. **Create the secrets.** Each is a file only you can read. The bridge
   refuses to start if any secret file is readable by group or others.

   ```sh
   mkdir -p secrets && chmod 700 secrets
   (umask 077
    openssl rand -hex 32 > secrets/xmtp-bridge.key     # bridge's XMTP identity (an Ethereum key; holds no funds)
    openssl rand -hex 32 > secrets/xmtp-db.key         # XMTP database key
    openssl rand -base64 32 > secrets/signal-store.pass)
   ```

3. **Configure.** Run `cp config.example.toml xmtp-bridge.toml`, then set
   `xmtp.agent` to the agent's address and `xmtp.env` to its network. Then run
   `target/release/xmtp-bridge check`.
4. **Link Signal.** Run `target/release/xmtp-bridge link-signal`. On the
   phone, open **Settings → Linked devices → +** and scan the QR code.
5. **Run.** Run `target/release/xmtp-bridge run`. Logs go to stderr, and
   `RUST_LOG=debug` gives more detail.

The bridge creates `data/` with mode 700. Its contents:

- `xmtp.db3`: encrypted with the XMTP database key.
- `signal.db3`: SQLCipher, encrypted with the store passphrase.
- `bridge-state.json`: mode 600. Maps Signal service ids to XMTP thread ids,
  which is personal data, so keep it private.

## Risks to know about

- **presage is an unofficial Signal client.** Signal does not support it, and
  it can break when Signal changes its servers, so update the pinned revision
  regularly. Accounts that send automated traffic can hit captchas or bans.
  The outbound buckets keep the bridge's sending rate modest.
- **The bridge sees plaintext.** It decrypts Signal messages and re-encrypts
  them for XMTP. The host running it is a trust point.
- **Open access:** anyone who knows the number can reach the agent, within
  the limits above.

## Adding a network

Implement `bridge_core::message::Frontend` (see `crates/frontend-signal` as the
example):

- Emit `FrontendEvent::Message` for user messages, with a stable id.
- For every `Outbound` that has an `ack`, report exactly one `Delivered` or
  `Failed`.
- Return when `shutdown` flips.

Then register the new front end in `crates/bridge/src/main.rs` next to Signal
and add its config section. Each network gets its own outbound pacer. A front
end that needs a child process can use `bridge_core::worker::supervise` (see
`crates/frontend-shoutbox`).

## Layout

| Path | What |
| --- | --- |
| `crates/bridge-core` | message model, token buckets, limiter, pacer, sessions, router state machine, worker protocols and supervisor (pure; 46 tests) |
| `crates/frontend-signal` | presage-based Signal front end and device linking |
| `crates/frontend-shoutbox` | web3 shoutbox front end: triggers, per-person rate, drives `xmtp-bridge-room` |
| `crates/agent-link-xmtp` | `xmtp-bridge-agent` (agent link) and `xmtp-bridge-room` (room member), the XMTP worker processes |
| `crates/bridge` | `xmtp-bridge`: config and wiring |
| `scripts/build-xmtp-ffi.sh` | builds libxmtp_ffi from source at the pinned revision |
