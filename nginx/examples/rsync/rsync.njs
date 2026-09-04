const PROTO_VER = 31;
const DEFAULT_QUEUE_POLL_SECONDS = 1;
const QUEUE_NOTICE_SECONDS = 60;
const DEFAULT_QUEUE_ENTRY_TTL_SECONDS = 120;
const MIN_QUEUE_ENTRY_TTL_SECONDS = 1;
const QUEUE_LOCK_TTL_SECONDS = 1;
const QUEUE_LOCK_RETRY_SECONDS = 0.01;
const DEFAULT_ACTIVE_REFRESH_SECONDS = 30;
const NEXT_TICKET_TTL_SECONDS = 86400;

const INIT = 0;
const VER_RECV = 1;
const MODULE_RECV = 2;

let state = INIT;
let protoVer;
let clientVer;
let requestedModule = '';
let handshakeSent = false;

let queueBackend = '';
let queueTicket;
let queueWaitKey = '';
let queueActiveKey = '';
let queuePollTimer;
let activeRefreshTimer;
let queueEntryTtlSeconds = DEFAULT_QUEUE_ENTRY_TTL_SECONDS;
let queuePollSeconds = DEFAULT_QUEUE_POLL_SECONDS;
let activeRefreshSeconds = DEFAULT_ACTIVE_REFRESH_SECONDS;
let queuedAtSeconds = 0;
let joinedAtPosition = 0;
let lastNoticeAtSeconds = 0;
let lastNoticePosition = 0;
let lastNoticeTotal = -1;
let cleanupRegistered = false;

let clientAddr = '';
let serverAddr = '';

function parseVersion(buf) {
    const match = buf.toString().match(/^@RSYNCD:\s*([0-9]+)(?:\.([0-9]+))?/);

    if (!match) {
        return {};
    }

    return {proto: Number(match[1]), sub: Number(match[2] || 0)};
}

function negotiateVersion(a, b) {
    let proto;
    let sub;

    if (a.proto > b.proto) {
        proto = b.proto;
        sub = b.sub ? 0 : b.sub;

    } else if (a.proto === b.proto) {
        proto = a.sub === b.sub ? a.proto : a.proto - 1;
        sub = a.sub === b.sub ? a.sub : 0;

    } else {
        proto = a.sub ? a.proto - 1 : a.proto;
        sub = 0;
    }

    return {proto, sub};
}

function readLine(buf, start) {
    const end = buf.indexOf('\n', start);

    if (end === -1) {
        return null;
    }

    let line = buf.slice(start, end);

    if (line.length && line[line.length - 1] === 0x0d) {
        line = line.slice(0, -1);
    }

    if (line.indexOf(0) !== -1) {
        return {error: true};
    }

    return {next: end + 1, line};
}

function queueDict() {
    return ngx.shared.rsync_queue;
}

function nowSeconds() {
    return Date.now() / 1000;
}

function timerDelay(seconds) {
    return seconds * 1000;
}

function configureQueueTiming(s) {
    const configured = Number(s.variables.rsync_queue_entry_ttl);
    const seconds = Number.isFinite(configured) && configured > 0
                    ? Math.max(MIN_QUEUE_ENTRY_TTL_SECONDS, configured)
                    : DEFAULT_QUEUE_ENTRY_TTL_SECONDS;

    queueEntryTtlSeconds = seconds;

    const renewalSeconds = Math.max(0.1, queueEntryTtlSeconds / 4);

    queuePollSeconds = Math.min(DEFAULT_QUEUE_POLL_SECONDS, renewalSeconds);
    activeRefreshSeconds = Math.min(DEFAULT_ACTIVE_REFRESH_SECONDS,
                                    renewalSeconds);
}

function prefixedKeys(prefix) {
    return queueDict().keys().filter(key => key.startsWith(prefix));
}

function ticketFromKey(key) {
    return Number(key.slice(key.lastIndexOf(':') + 1));
}

function sortedWaitKeys(backend) {
    const prefix = `wait:${backend}:`;

    return prefixedKeys(prefix).sort((a, b) => ticketFromKey(a)
                                                   - ticketFromKey(b));
}

function activeKeys(backend) {
    return prefixedKeys(`active:${backend}:`);
}

function setQueueVariable(s, name, value) {
    s.variables[name] = String(value);
}

function setSessionStatus(s, code) {
    setQueueVariable(s, 'rsync_status_code', code);
}

function updateWaitVariable(s) {
    setQueueVariable(s, 'rsync_queue_wait_seconds',
                     (nowSeconds() - queuedAtSeconds).toFixed(3));
}

function renderQueueMessage(s, variable, fallback, values) {
    let message = s.variables[variable] || fallback;

    message = message.replace(/\{([a-z_]+)\}/g, (match, name) => {
        return values[name] === undefined ? match : String(values[name]);
    });

    return message.endsWith('\n') ? message : `${message}\n`;
}

function queueMessageValues(s, position, total, waitSeconds) {
    return {
        backend: queueBackend,
        max_active: Number(s.variables.rsync_max_active) || 0,
        module: requestedModule,
        position,
        queued: total,
        wait_seconds: waitSeconds || 0,
    };
}

function cancelTimer(timer) {
    if (timer === undefined) {
        return;
    }

    try {
        clearTimeout(timer);
    } catch (e) {
        // The callback may already have been dequeued when the client closed.
    }
}

function cleanupQueueEntry() {
    if (queuePollTimer !== undefined) {
        cancelTimer(queuePollTimer);
        queuePollTimer = undefined;
    }

    if (activeRefreshTimer !== undefined) {
        cancelTimer(activeRefreshTimer);
        activeRefreshTimer = undefined;
    }

    const dict = queueDict();

    if (queueWaitKey) {
        dict.delete(queueWaitKey);
        queueWaitKey = '';
    }

    if (queueActiveKey) {
        dict.delete(queueActiveKey);
        queueActiveKey = '';
    }
}

function refreshActiveSlot() {
    if (!queueActiveKey) {
        activeRefreshTimer = undefined;
        return;
    }

    queueDict().set(queueActiveKey, 1, timerDelay(queueEntryTtlSeconds));
    activeRefreshTimer = setTimeout(refreshActiveSlot,
                                    timerDelay(activeRefreshSeconds));
}

function registerCleanup() {
    if (cleanupRegistered) {
        return;
    }

    cleanupRegistered = true;
    njs.on('exit', cleanupQueueEntry);
}

function withBackendLock(s, callback) {
    const dict = queueDict();
    const lockKey = `lock:${queueBackend}`;

    if (!dict.add(lockKey, 1, timerDelay(QUEUE_LOCK_TTL_SECONDS))) {
        queuePollTimer = setTimeout(() => {
            queuePollTimer = undefined;
            withBackendLock(s, callback);
        }, timerDelay(QUEUE_LOCK_RETRY_SECONDS));
        return;
    }

    try {
        callback();

    } finally {
        dict.delete(lockKey);
    }
}

function sendQueueNotice(s, position, total, initial) {
    const waitSeconds = Math.ceil(nowSeconds() - queuedAtSeconds);
    const values = queueMessageValues(s, position, total, waitSeconds);
    let message = '';

    if (initial) {
        message += renderQueueMessage(s, 'rsync_queue_busy_message',
                    'Upstream {backend} has reached the maximum number of '
                    + '{max_active} connections. Your request is being queued.',
                    values);
    }

    message += renderQueueMessage(s, 'rsync_queue_status_message',
                                  'Your position: {position}, Total queued: '
                                  + '{queued}', values);
    s.sendDownstream(message);
    s.log(`rsync queue: backend=${queueBackend} module=${requestedModule} `
          + `ticket=${queueTicket} position=${position} total=${total}`);
    lastNoticeAtSeconds = nowSeconds();
    lastNoticePosition = position;
    lastNoticeTotal = total;
}

function activate(s, wasQueued) {
    const dict = queueDict();

    if (queueWaitKey) {
        dict.delete(queueWaitKey);
        queueWaitKey = '';
    }

    queueActiveKey = `active:${queueBackend}:${queueTicket}`;
    dict.set(queueActiveKey, 1, timerDelay(queueEntryTtlSeconds));
    setSessionStatus(s, 200);

    if (wasQueued) {
        const waitedSeconds = nowSeconds() - queuedAtSeconds;
        updateWaitVariable(s);
        const values = queueMessageValues(s, 0,
                                          sortedWaitKeys(queueBackend).length,
                                          Math.ceil(waitedSeconds));
        s.sendDownstream(renderQueueMessage(s, 'rsync_queue_admitted_message',
                         'Queue slot acquired after {wait_seconds} seconds. '
                         + 'Connecting to the upstream.', values));
        s.log(`rsync queue admitted: backend=${queueBackend} `
              + `module=${requestedModule} ticket=${queueTicket} `
              + `wait_seconds=${waitedSeconds.toFixed(3)}`);
    }

    queuePollTimer = undefined;
    s.done();
}

function pollQueue(s) {
    withBackendLock(s, () => {
        const dict = queueDict();
        const maxActive = Number(s.variables.rsync_max_active) || 0;

        if (!queueWaitKey || !dict.has(queueWaitKey)) {
            s.sendDownstream('@ERROR: Queue entry expired; please retry.\n');
            setSessionStatus(s, 503);
            s.done(503);
            return;
        }

        dict.set(queueWaitKey, 1, timerDelay(queueEntryTtlSeconds));

        const waits = sortedWaitKeys(queueBackend);
        const index = waits.indexOf(queueWaitKey);
        const position = index + 1;

        if (index === 0 && activeKeys(queueBackend).length < maxActive) {
            activate(s, true);
            return;
        }

        updateWaitVariable(s);

        if (position !== lastNoticePosition
            || waits.length !== lastNoticeTotal
            || nowSeconds() - lastNoticeAtSeconds >= QUEUE_NOTICE_SECONDS)
        {
            sendQueueNotice(s, position, waits.length, false);
        }

        queuePollTimer = setTimeout(() => {
            queuePollTimer = undefined;
            pollQueue(s);
        }, timerDelay(queuePollSeconds));
    });
}

function acquireQueue(s) {
    queueBackend = s.variables.rsync_backend || 'default';
    configureQueueTiming(s);
    const maxActive = Number(s.variables.rsync_max_active) || 0;
    const maxQueued = Number(s.variables.rsync_max_queued) || 0;

    if (maxActive <= 0) {
        setSessionStatus(s, 200);
        s.done();
        return;
    }

    registerCleanup();

    withBackendLock(s, () => {
        const dict = queueDict();
        const waits = sortedWaitKeys(queueBackend);
        const nextKey = `next:${queueBackend}`;

        queueTicket = dict.incr(nextKey, 1, 0,
                                timerDelay(NEXT_TICKET_TTL_SECONDS));

        if (waits.length === 0
            && activeKeys(queueBackend).length < maxActive)
        {
            activate(s, false);
            return;
        }

        if (maxQueued > 0 && waits.length >= maxQueued) {
            setSessionStatus(s, 429);
            const values = queueMessageValues(s, 0, waits.length, 0);
            s.sendDownstream(renderQueueMessage(s, 'rsync_queue_full_message',
                             '@ERROR: Server queue is full for upstream '
                             + '{backend}; please retry later.', values));
            s.warn(`rsync queue full: backend=${queueBackend} `
                   + `module=${requestedModule}`);
            s.done(429);
            return;
        }

        queuedAtSeconds = nowSeconds();
        joinedAtPosition = waits.length + 1;
        queueWaitKey = `wait:${queueBackend}:${queueTicket}`;
        dict.set(queueWaitKey, 1, timerDelay(queueEntryTtlSeconds));
        setSessionStatus(s, 429);
        setQueueVariable(s, 'rsync_queue_queued', 'true');
        updateWaitVariable(s);
        sendQueueNotice(s, waits.length + 1, waits.length + 1, true);
        queuePollTimer = setTimeout(() => {
            queuePollTimer = undefined;
            pollQueue(s);
        }, timerDelay(queuePollSeconds));
    });
}

function monitorQueuedClient(s) {
    s.on('upstream', (data, flags) => {
        if (!flags.last) {
            return;
        }

        cleanupQueueEntry();
        setSessionStatus(s, 429);
        s.done(429);
    });
}

function preread(s) {
    let readPos = 0;

    if (s.variables.rsync_forbidden === '1') {
        setSessionStatus(s, 403);
        s.done(403);
        return;
    }

    if (!handshakeSent) {
        s.send(`@RSYNCD: ${PROTO_VER}.0\n`);
        handshakeSent = true;
        registerCleanup();
    }

    s.on('upstream', (data, flags) => {
        while (true) {
            if (state === MODULE_RECV) {
                s.off('upstream');
                monitorQueuedClient(s);
                acquireQueue(s);
                return;
            }

            // During preread, njs supplies the complete buffered client data
            // on every callback rather than only the newly arrived suffix.
            const result = readLine(data, readPos);

            if (result === null) {
                if (flags.last) {
                    setSessionStatus(s, 500);
                    s.done(500);
                }

                return;
            }

            if (result.error) {
                s.sendDownstream('@ERROR: protocol startup error\n');
                setSessionStatus(s, 500);
                s.done(500);
                return;
            }

            readPos = result.next;

            if (state === INIT) {
                clientVer = parseVersion(result.line);

                if (clientVer.proto === undefined) {
                    s.sendDownstream('@ERROR: protocol startup error\n');
                    setSessionStatus(s, 500);
                    s.done(500);
                    return;
                }

                if (clientVer.proto === 30 && clientVer.sub === 0
                    && result.line.toString().indexOf('.') === -1)
                {
                    s.sendDownstream('@ERROR: your client is speaking an '
                                     + 'incompatible beta of protocol 30\n');
                    setSessionStatus(s, 500);
                    s.done(500);
                    return;
                }

                const negotiated = negotiateVersion(clientVer,
                                                     {proto: PROTO_VER, sub: 0});
                protoVer = negotiated.proto;
                state = VER_RECV;

            } else {
                requestedModule = result.line.toString();
                s.variables.rsync_module = requestedModule;
                state = MODULE_RECV;
            }
        }
    });
}

function parseProxyV1(line) {
    const fields = line.toString().trim().split(/\s+/);

    if (fields.length >= 6 && fields[0] === 'PROXY') {
        clientAddr = `${fields[2]}:${fields[4]}`;
        serverAddr = `${fields[3]}:${fields[5]}`;
    }
}

function filter(s) {
    let uploadBuffer = Buffer.alloc(0);
    let downloadBuffer = Buffer.alloc(0);
    let proxyVersion = 0;

    if (queueActiveKey && activeRefreshTimer === undefined) {
        activeRefreshTimer = setTimeout(refreshActiveSlot,
                                        timerDelay(activeRefreshSeconds));
    }

    s.on('upstream', (data, flags) => {
        uploadBuffer = Buffer.concat([uploadBuffer, data]);

        if (proxyVersion === 0) {
            if (uploadBuffer.length < 6) {
                return;
            }

            if (uploadBuffer.slice(0, 6).equals(Buffer.from('PROXY '))) {
                proxyVersion = 1;

            } else if (uploadBuffer.slice(0, 6)
                                   .equals(Buffer.from([0x0d, 0x0a, 0x0d,
                                                        0x0a, 0x00, 0x0d])))
            {
                proxyVersion = 2;

            } else {
                proxyVersion = -1;
            }
        }

        if (proxyVersion === 1) {
            const end = uploadBuffer.indexOf(Buffer.from([0x0d, 0x0a]));

            if (end === -1) {
                return;
            }

            parseProxyV1(uploadBuffer.slice(0, end));
            s.send(uploadBuffer.slice(0, end + 2));
            uploadBuffer = uploadBuffer.slice(end + 2);
            proxyVersion = -1;
        }

        if (proxyVersion === 2) {
            if (uploadBuffer.length < 16) {
                return;
            }

            const length = uploadBuffer.readUInt16BE(14);

            if (uploadBuffer.length < 16 + length) {
                return;
            }

            s.send(uploadBuffer.slice(0, 16 + length));
            uploadBuffer = uploadBuffer.slice(16 + length);
            proxyVersion = -1;
        }

        const line = readLine(uploadBuffer, 0);

        if (line === null || line.error) {
            return;
        }

        s.send(`@RSYNCD: ${protoVer}${protoVer < 30 ? '' : '.0'}\n`);
        s.send(uploadBuffer.slice(line.next), {last: flags.last});
        s.off('upstream');
    });

    s.on('downstream', (data, flags) => {
        downloadBuffer = Buffer.concat([downloadBuffer, data]);
        const line = readLine(downloadBuffer, 0);

        if (line === null || line.error) {
            return;
        }

        s.send(downloadBuffer.slice(line.next), {last: flags.last});
        s.off('downstream');
    });
}

function moduleName() {
    return requestedModule;
}

function negVer() {
    return protoVer === undefined ? '-' : `${protoVer}${protoVer < 30 ? '' : '.0'}`;
}

function cliVer() {
    return clientVer === undefined || clientVer.proto === undefined
           ? '-'
           : `${clientVer.proto}.${clientVer.sub}`;
}

function clientEndpoint(s) {
    return clientAddr || `${s.variables.remote_addr}:${s.variables.remote_port}`;
}

function serverEndpoint(s) {
    return serverAddr || `${s.variables.server_addr}:${s.variables.server_port}`;
}

function initialQueuePosition() {
    // This function is evaluated by the access log when the stream session
    // finishes. Releasing the ticket here also cancels refresh/poll timers.
    cleanupQueueEntry();

    return joinedAtPosition;
}

export default {preread, filter, moduleName, negVer, cliVer, clientEndpoint,
                serverEndpoint, initialQueuePosition};
