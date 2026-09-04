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
