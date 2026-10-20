# Shoutbox setup: seat the bridge room identity in every leader-created group
#
# SUPERSEDED (code part): upstream web3-shoutbox-platform commit 82bd3c2
# ("Add configured bridge members to room groups") implemented
# VITE_SHOUTBOX_BRIDGE_ADDRESSES natively (`src/lib/bridgeMembers.ts`,
# wired into `createGroupAsLeader`). No patch needed on current shoutbox —
# just configure and redeploy. The hand-written diff below is kept for
# reference only.
#
# Apply on the machine that hosts the shoutbox (NOT the bridge host), then
# rebuild/redeploy it.
#
# Remote shoutbox env (all three required):
#   VITE_XMTP_ENV=production            # MUST match the bridge (dev != production network)
#   VITE_GUN_RELAY_PEERS=http://108.165.23.53:8765/gun
#   VITE_SHOUTBOX_BRIDGE_ADDRESSES=0x26a1525f38a1949a5cb8339fd5225bccd164f2e5
#
# Order matters: start the bridge first (`xmtp-bridge run` registers the room
# identity's XMTP inbox). Leaders can only add an address that already has an
# inbox on the same XMTP network.

# --- 1. src/config/env.ts: add to the schema --------------------------------
#
#   VITE_SHOUTBOX_BRIDGE_ADDRESSES: z.string().optional().default(''),
#
# and to the `raw` object in getEnv():
#
#   VITE_SHOUTBOX_BRIDGE_ADDRESSES: import.meta.env.VITE_SHOUTBOX_BRIDGE_ADDRESSES || undefined,
#
# --- 2. src/vite-env.d.ts: add the type --------------------------------------
#
#   readonly VITE_SHOUTBOX_BRIDGE_ADDRESSES: string
#
# --- 3. src/services/groupLifecycleService.ts: include bridge in creation ----
#
# In createGroupAsLeader, replace:
#
#     const addresses = onlineUsers
#       .filter((u) => u.isOnline)
#       .map((u) => u.address)
#
# with:
#
#     import { env } from '@/config/env'
#     ...
#     const addresses = onlineUsers
#       .filter((u) => u.isOnline)
#       .map((u) => u.address)
#     // Bridge room identity: always a member so it can relay @agent lines.
#     const bridgeAddresses = (env.VITE_SHOUTBOX_BRIDGE_ADDRESSES ?? '')
#       .split(',')
#       .map((a) => a.trim().toLowerCase())
#       .filter((a) => a.length > 0 && !addresses.map((x) => x.toLowerCase()).includes(a))
#     addresses.push(...bridgeAddresses)
#
# (createGroup lower-cases identifiers itself, so casing here is only for the
# dedupe check.)
#
# --- 4. .env.local.example: document it --------------------------------------
#
#   # Optional: bridge room identity (xmtp-bridge [shoutbox] key). Leaders add
#   # it to every group so @agent lines reach the agent. Must already have an
#   # XMTP inbox on VITE_XMTP_ENV (start the bridge once first).
#   VITE_SHOUTBOX_BRIDGE_ADDRESSES=
