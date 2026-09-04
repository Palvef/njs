# rsync queue example

This example parses the rsync daemon greeting and module name in
`js_preread`, chooses an upstream, and applies a strict FIFO connection limit
per upstream. Queue state lives in `ngx.shared.rsync_queue`, so every nginx
worker and every listener that includes the same configuration shares one
queue.

While waiting, a normal rsync client receives its initial queue position
immediately and an updated status at least once per minute. These lines are
valid rsync daemon MOTD output; the upstream connection is opened only after
the client reaches the head of the queue and an active slot is available.
The four `rsync_queue_*_message` variables in `rsync.conf` customize client
output without rebuilding the module. They accept `{backend}`, `{max_active}`,
`{module}`, `{position}`, `{queued}`, and `{wait_seconds}` placeholders.

Copy `rsync.njs` to `/etc/nginx/njs/`, include `rsync.conf` inside the nginx
`stream` context, and adjust the module map, upstream addresses, and limits.
The njs stream module must include support for `s.send()` from `js_preread`,
as provided by this source tree.

- `rsync_max_active = 0` disables queueing for that upstream.
- A positive `rsync_max_active` enables the active connection limit.
- `rsync_max_queued = 0` makes the waiting queue unbounded.
- A positive `rsync_max_queued` rejects excess clients when the queue is full.

The active and waiting entries expire after a worker crash. Active entries are
refreshed while their proxied sessions remain open. Access logs expose whether
the request waited (`queued`) and the live number of waiting requests
(`queue_depth`).

## Access log fields

The example `rsync_example` log format records the following fields:

| Field | Meaning |
| --- | --- |
| `$remote_addr` | Client address seen by nginx. |
| `module` | Module name requested by the rsync client, or `-` when the handshake did not reach the module line. |
| `backend` | Upstream selected by the `$rsync_module` to `$rsync_backend` map. |
| `state` | Queue lifecycle state described below. |
| `queued` | `true` when the connection waited at any point; it remains `true` after admission. |
| `queue_depth` | Number of clients still waiting for the same backend when this session is logged, after this session's own ticket is released. |
| `position` | Most recently observed one-based queue position; reset to `0` after admission. |
| `wait_ms` | Time spent waiting in milliseconds, or `0` when the connection never queued. |

`state` can have these values:

| State | Meaning |
| --- | --- |
| `initializing` | Initial value before the preread handler starts. |
| `parsing` | Reading the rsync version and requested module. |
| `unlimited` | The selected backend has `rsync_max_active=0`, so no queue is applied. |
| `active` | The connection obtained an active slot without waiting. |
| `queued` | The connection is waiting for an active slot. |
| `admitted` | A previously queued connection obtained a slot. |
| `full` | The configured waiting queue was full and the connection was rejected. |
| `expired` | The waiting ticket expired before admission. |

Keep `$rsync_queue_depth` in an access log format that is written when the
session ends. Besides reporting the live depth, evaluating this variable
releases the session's active or waiting ticket immediately and cancels its
queue timers. The shared-dictionary timeout remains a crash-recovery fallback.
