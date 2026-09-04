# rsync queue example

This example parses the rsync daemon greeting and module name in
`js_preread`, chooses an upstream, and applies a strict FIFO connection limit
per upstream. Queue state lives in `ngx.shared.rsync_queue`, so every nginx
worker and every listener that includes the same configuration shares one
queue.

While waiting, a normal rsync client receives its initial queue position
immediately. It receives another status message whenever the total queued
count or its own position changes (within one queue poll, normally one second),
plus a heartbeat at least once per minute. These lines are valid rsync daemon
MOTD output; the upstream connection is opened only after the client reaches
the head of the queue and an active slot is available.
The four `rsync_queue_*_message` variables in `rsync.conf` customize client
output without rebuilding the module. They accept `{backend}`, `{max_active}`,
`{module}`, `{position}`, `{queued}`, and `{wait_seconds}` placeholders.

Copy `rsync.njs` to `/etc/nginx/njs/`, include `rsync.conf` and
`forbidden.conf` inside the nginx `stream` context, and adjust the module map,
upstream addresses, limits, and rsync-only forbidden addresses. The njs stream
module must include support for `s.send()` from `js_preread`, as provided by
this source tree.

- `rsync_max_active = 0` disables queueing for that upstream.
- A positive `rsync_max_active` enables the active connection limit.
- `rsync_max_queued = 0` makes the waiting queue unbounded.
- A positive `rsync_max_queued` rejects excess clients when the queue is full.
- `rsync_queue_entry_ttl` sets the active/waiting ticket lease in seconds. It
  defaults to `120`; values below `1` are clamped to `1`.

The active and waiting entries expire after a worker crash. Active entries are
refreshed while their proxied sessions remain open. The access log is written
once when the session ends and summarizes whether it waited (`queued`), where
it joined the queue (`initial_queue_position`), and how long it waited
(`wait_seconds`).

The independent `rsync_forbidden` `geo` table blocks only rsync clients. A
matching address is finalized before the daemon greeting and logged as `403`;
adding an address there does not change any HTTP access policy.

The ticket TTL is a stale-entry recovery timeout, not a maximum queue wait.
Healthy waiting tickets are renewed on every queue poll, and active tickets are
renewed periodically. Polling and active refresh intervals automatically
shorten to at most one quarter of the configured TTL, so TTL values below the
default 30-second active refresh interval remain safe.

## Access log fields

The example `rsync_example` log format records the following fields:

| Field | Meaning |
| --- | --- |
| `$remote_addr` | Client address seen by nginx. |
| `module` | Module name requested by the rsync client, or `-` when the handshake did not reach the module line. |
| `backend` | Upstream selected by the `$rsync_module` to `$rsync_backend` map. |
| `status` | Rsync result: `200` after a valid handshake and completed proxy session, `403` for an rsync-forbidden address, `429` for a queued/rate-limited session, `500` for an invalid or incomplete handshake, and `503` for queue-ticket expiry or upstream unavailability. |
| `queued` | `true` when the connection waited at any point; it remains `true` after admission. |
| `initial_queue_position` | Fixed one-based position when the connection joined the queue, or `0` when it never joined. It does not change as the queue moves. |
| `wait_seconds` | Time spent waiting in seconds, or `0` when the connection never queued. |

Keep `$rsync_initial_queue_position` in an access log format that is written when
the session ends. Evaluating this summary value also releases the session's
active or waiting ticket immediately and cancels its queue timers. The
shared-dictionary timeout remains a crash-recovery fallback.
