const fs = require("fs");
const http = require("http");
const path = require("path");
const { spawn, spawnSync } = require("child_process");
const crypto = require("crypto");

const HOST = "127.0.0.1";
const DEFAULT_PORT = 7861;
const PORT = Number.parseInt(process.env.PORT || "", 10) || DEFAULT_PORT;
const ROOT_DIR = __dirname;
const STATIC_DIR = path.join(ROOT_DIR, "static");
const DEFAULT_DATA_DIR = path.join(ROOT_DIR, "data");
const DATA_DIR = path.resolve(process.env.DATA_DIR || DEFAULT_DATA_DIR);
const IDENTITY_FILE = path.join(DATA_DIR, "local-identity.json");
const BOOTSTRAP_FILE = path.join(DATA_DIR, "bootstrap.json");
const FORUM_FILE = path.join(DATA_DIR, "forum.json");
const LOG_FILE = path.join(DATA_DIR, "logs.jsonl");
const BRIDGE_PATH = path.join(ROOT_DIR, "bridge.py");
const APP_VERSION = "0.1.0";
const INSTANCE_LABEL = String(process.env.INSTANCE_LABEL || path.basename(DATA_DIR) || "meshforum").trim();
// Legacy hint used for rough chunk-count estimates.
// Actual transfer slicing is based on UTF-8 byte size of serialized chunk envelopes.
const FORUM_CHUNK_SIZE = Number(process.env.FORUM_CHUNK_SIZE || 150);
const MESH_TEXT_PAYLOAD_MAX_BYTES = Number(process.env.MESH_TEXT_PAYLOAD_MAX_BYTES || 220);
const MESH_TEXT_PAYLOAD_HEADROOM_BYTES = Number(process.env.MESH_TEXT_PAYLOAD_HEADROOM_BYTES || 12);
const MESH_TEXT_PAYLOAD_SAFE_BYTES = Math.max(64, MESH_TEXT_PAYLOAD_MAX_BYTES - MESH_TEXT_PAYLOAD_HEADROOM_BYTES);
const CHUNK_TOTAL_HINT = 9999;
// LoRa airtime per packet is 1-5 seconds depending on modem preset.
// We wait for ACK before next chunk, so this is just a post-ACK guard gap.
const FORUM_CHUNK_DELAY_MS = Number(process.env.FORUM_CHUNK_DELAY_MS || 500);
// Bridge-level retry count if firmware ACK times out.
const FORUM_CHUNK_RETRY = Number(process.env.FORUM_CHUNK_RETRY || 1);
// Delay between chunk retries in ms.
const FORUM_CHUNK_RETRY_DELAY_MS = Number(process.env.FORUM_CHUNK_RETRY_DELAY_MS || 2000);
const FORUM_PROTO_LEGACY = "meshforum/1";
const FORUM_PROTO_COMPACT = "mf1";

fs.mkdirSync(DATA_DIR, { recursive: true });

function readJson(filePath, fallback) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return fallback;
  }
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2));
}

function writeJsonAsync(filePath, value) {
  return fs.promises.writeFile(filePath, JSON.stringify(value, null, 2), 'utf8');
}

function readLogFile(limit = 500) {
  try {
    const raw = fs.readFileSync(LOG_FILE, "utf8");
    const lines = raw.split(/\r?\n/).filter(Boolean);
    return lines.slice(-limit).map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return {
          id: crypto.randomUUID(),
          ts: new Date().toISOString(),
          kind: "error",
          summary: "invalid log line",
          payload: { line },
        };
      }
    });
  } catch {
    return [];
  }
}

function generateIdentity() {
  const keyPair = crypto.generateKeyPairSync("ed25519");
  const authorId = crypto.randomBytes(8).toString("hex");
  return {
    authorId,
    displayName: `Node-${authorId.slice(0, 6)}`,
    publicKeyPem: keyPair.publicKey.export({ type: "spki", format: "pem" }),
    privateKeyPem: keyPair.privateKey.export({ type: "pkcs8", format: "pem" }),
    createdAt: new Date().toISOString(),
  };
}

function loadIdentity() {
  const existing = readJson(IDENTITY_FILE, null);
  if (existing && existing.authorId && existing.publicKeyPem && existing.privateKeyPem) {
    return existing;
  }
  const created = generateIdentity();
  writeJson(IDENTITY_FILE, created);
  return created;
}

function defaultBootstrapState() {
  return {
    selectedNodeId: null,
    selectedNodeLabel: null,
    lastHelloAt: null,
    lastHelloMessageId: null,
    lastHelloAckAt: null,
    peerKnowledge: {},
  };
}

const localIdentity = loadIdentity();
let bootstrapState = readJson(BOOTSTRAP_FILE, defaultBootstrapState());
if (!bootstrapState.peerKnowledge || typeof bootstrapState.peerKnowledge !== "object") {
  bootstrapState.peerKnowledge = {};
}
let observedNodes = [];
let bridgeLogs = [];
let bridgeProcess = null;
let bridgeStdoutBuffer = "";
let bridgeStderrBuffer = "";
let incomingChunkTransfers = new Map();
let meshSendQueue = Promise.resolve();
let syncOutbound = {
  active: false,
  sessionId: null,
  totalTransfers: 0,
  completedTransfers: 0,
  totalChunks: 0,
  sentChunks: 0,
  totalBytes: 0,
  sentBytes: 0,
  startedAt: null,
  finishedAt: null,
  target: null,
};
let syncInbound = {
  active: false,
  sessionId: null,
  totalTransfers: 0,
  completedTransfers: 0,
  totalChunks: 0,
  receivedChunks: 0,
  totalBytes: 0,
  receivedBytes: 0,
  transfersCompleted: 0,
  chunksReceived: 0,
  lastAt: null,
  lastType: null,
  startedAt: null,
  finishedAt: null,
  currentTransfer: null,
  pendingTransfers: 0,
  source: null,
};
let syncProgress = {
  active: false,
  sessionId: null,
  peerId: null,
  phase: "idle",
  direction: "idle",
  completed: 0,
  total: 0,
  completedBytes: 0,
  totalBytes: 0,
  startedAt: null,
  finishedAt: null,
  updatedAt: null,
  label: "Idle",
};
const syncSessions = new Map();
const startedAt = new Date().toISOString();
let shortIdCounter = 0;
let forumState = readJson(FORUM_FILE, {
  topics: [],
  posts: [],
  seenEnvelopeIds: [],
});

let meshtasticStatus = {
  connected: false,
  mode: "starting",
  error: null,
  port: null,
  localNodeId: null,
  localUserId: null,
  localDisplayName: null,
  pythonAvailable: false,
  bridgeRunning: false,
  availablePorts: [],
};

// Batch processing for inbound messages (prevents blocking on disk I/O)
const INBOUND_BATCH_TIMEOUT_MS = 100;
const INBOUND_BATCH_MAX_SIZE = 50;
let inboundProcessingQueue = Promise.resolve();
let forumStateSaveScheduled = false;
let forumStateSavePending = false;

function saveForumState() {
  writeJson(FORUM_FILE, forumState);
}

function scheduleBatchedForumStateSave() {
  forumStateSavePending = true;

  if (forumStateSaveScheduled) {
    return; // Already scheduled, will save on next batch
  }

  forumStateSaveScheduled = true;

  // Debounce writes: wait INBOUND_BATCH_TIMEOUT_MS before actually writing
  setTimeout(async () => {
    forumStateSaveScheduled = false;
    if (forumStateSavePending) {
      forumStateSavePending = false;
      try {
        await writeJsonAsync(FORUM_FILE, forumState);
      } catch (error) {
        pushLog("error", `forum state save failed: ${error.message}`);
        // Mark as pending again to retry
        forumStateSavePending = true;
        scheduleBatchedForumStateSave();
      }
    }
  }, INBOUND_BATCH_TIMEOUT_MS);
}

function rememberEnvelopeId(envelopeId) {
  forumState.seenEnvelopeIds = [...new Set([...(forumState.seenEnvelopeIds || []), envelopeId])].slice(-2000);
}

function sortForumState() {
  forumState.topics = [...(forumState.topics || [])].sort((a, b) => {
    return String(b.bumpedAt || b.createdAt || "").localeCompare(String(a.bumpedAt || a.createdAt || ""));
  });
  forumState.posts = [...(forumState.posts || [])].sort((a, b) => {
    return String(a.createdAt || "").localeCompare(String(b.createdAt || ""));
  });
}

function buildForumPayload() {
  sortForumState();
  return {
    topics: forumState.topics.map((topic) => ({
      ...topic,
      posts: forumState.posts.filter((post) => post.topicId === topic.topicId),
    })),
  };
}

function upsertTopic(topic) {
  const existingIndex = forumState.topics.findIndex((item) => item.topicId === topic.topicId);
  if (existingIndex >= 0) {
    forumState.topics[existingIndex] = {
      ...forumState.topics[existingIndex],
      ...topic,
    };
  } else {
    forumState.topics.push(topic);
  }
}

function upsertPost(post) {
  const existingIndex = forumState.posts.findIndex((item) => item.postId === post.postId);
  if (existingIndex >= 0) {
    forumState.posts[existingIndex] = {
      ...forumState.posts[existingIndex],
      ...post,
    };
  } else {
    forumState.posts.push(post);
  }
}

function nowIsoFromTs(ts) {
  return new Date(ts || Date.now()).toISOString();
}

function toBase36(value) {
  return Math.max(0, Number(value) || 0).toString(36);
}

function createShortId(prefix = "") {
  shortIdCounter = (shortIdCounter + 1) % 1679616;
  const ts = toBase36(Date.now()).slice(-5);
  const counter = shortIdCounter.toString(36).padStart(3, "0");
  const randomPart = crypto.randomBytes(3).toString("hex");
  const authorPrefix = String(localIdentity.authorId || "").slice(0, 2);
  return `${prefix}${ts}${counter}${authorPrefix}${randomPart}`;
}

function compactForumType(type) {
  switch (type) {
    case "topic_create":
      return "tc";
    case "post_create":
      return "pc";
    case "sync_start":
      return "ss";
    case "sync_request":
      return "sr";
    case "sync_offer":
      return "so";
    case "sync_offer_ack":
      return "sa";
    case "sync_turn":
      return "st";
    case "sync_done":
      return "sd";
    case "hello":
      return "h";
    case "hello_ack":
      return "ha";
    default:
      return type;
  }
}

function decodeForumEnvelope(input) {
  if (!input || typeof input !== "object") {
    return null;
  }

  if (input.p === FORUM_PROTO_LEGACY && input.t) {
    return input;
  }

  if (input.p !== FORUM_PROTO_COMPACT || !input.t) {
    return null;
  }

  const decoded = {
    p: FORUM_PROTO_LEGACY,
    t: input.t,
  };

  if (input.i) decoded.id = input.i;
  if (input.s != null) decoded.ts = input.s;
  if (input.n) decoded.n = input.n;
  if (input.a) decoded.a = input.a;
  if (input.f) decoded.from = input.f;
  if (input.m) decoded.syncSessionId = input.m;

  if (input.t === "tc") {
    decoded.t = "topic_create";
    decoded.topicId = input.x;
    decoded.title = input.h;
    decoded.body = input.b || "";
    if (input.y) decoded.postId = input.y;
    return decoded;
  }

  if (input.t === "pc") {
    decoded.t = "post_create";
    decoded.topicId = input.x;
    decoded.postId = input.y;
    decoded.body = input.b || "";
    return decoded;
  }

  if (input.t === "ss") {
    decoded.t = "sync_start";
    decoded.syncSessionId = input.m;
    decoded.totalTransfers = Number(input.e || 0);
    decoded.totalChunks = Number(input.c || 0);
    decoded.totalBytes = Number(input.z || 0);
    return decoded;
  }

  if (input.t === "c") {
    decoded.t = "chunk";
    decoded.x = input.x;
    decoded.k = input.k;
    decoded.i = Number(input.i || 0);
    decoded.n = Number(input.n || 0);
    decoded.d = String(input.d || "");
    return decoded;
  }

  if (input.t === "h") {
    decoded.t = "hello";
    return decoded;
  }

  if (input.t === "ha") {
    decoded.t = "hello_ack";
    return decoded;
  }

  if (input.t === "sr") {
    decoded.t = "sync_request";
    return decoded;
  }

  if (input.t === "so") {
    decoded.t = "sync_offer";
    decoded.syncSessionId = input.m;
    decoded.updatesCount = Number(input.u || 0);
    return decoded;
  }

  if (input.t === "sa") {
    decoded.t = "sync_offer_ack";
    decoded.syncSessionId = input.m;
    decoded.updatesCount = Number(input.u || 0);
    return decoded;
  }

  if (input.t === "st") {
    decoded.t = "sync_turn";
    decoded.syncSessionId = input.m;
    return decoded;
  }

  if (input.t === "sd") {
    decoded.t = "sync_done";
    decoded.syncSessionId = input.m;
    return decoded;
  }

  return null;
}

function encodeForumEnvelope(envelope) {
  if (!envelope || !envelope.t) {
    return envelope;
  }

  if (envelope.t === "topic_create") {
    return {
      p: FORUM_PROTO_COMPACT,
      t: "tc",
      i: envelope.id,
      s: envelope.ts,
      n: envelope.n,
      a: envelope.a,
      f: envelope.from,
      m: envelope.syncSessionId,
      x: envelope.topicId,
      h: envelope.title,
      y: envelope.postId,
      b: envelope.body || "",
    };
  }

  if (envelope.t === "post_create") {
    return {
      p: FORUM_PROTO_COMPACT,
      t: "pc",
      i: envelope.id,
      s: envelope.ts,
      n: envelope.n,
      a: envelope.a,
      f: envelope.from,
      m: envelope.syncSessionId,
      x: envelope.topicId,
      y: envelope.postId,
      b: envelope.body || "",
    };
  }

  if (envelope.t === "sync_start") {
    return {
      p: FORUM_PROTO_COMPACT,
      t: "ss",
      i: envelope.id,
      s: envelope.ts,
      f: envelope.from,
      m: envelope.syncSessionId,
      e: envelope.totalTransfers || 0,
      c: envelope.totalChunks || 0,
      z: envelope.totalBytes || 0,
    };
  }

  if (envelope.t === "sync_request") {
    return {
      p: FORUM_PROTO_COMPACT,
      t: "sr",
      i: envelope.id,
      f: envelope.from,
    };
  }

  if (envelope.t === "sync_offer") {
    return {
      p: FORUM_PROTO_COMPACT,
      t: "so",
      i: envelope.id,
      f: envelope.from,
      m: envelope.syncSessionId,
      u: envelope.updatesCount || 0,
    };
  }

  if (envelope.t === "sync_offer_ack") {
    return {
      p: FORUM_PROTO_COMPACT,
      t: "sa",
      i: envelope.id,
      f: envelope.from,
      m: envelope.syncSessionId,
      u: envelope.updatesCount || 0,
    };
  }

  if (envelope.t === "sync_turn") {
    return {
      p: FORUM_PROTO_COMPACT,
      t: "st",
      i: envelope.id,
      f: envelope.from,
      m: envelope.syncSessionId,
    };
  }

  if (envelope.t === "sync_done") {
    return {
      p: FORUM_PROTO_COMPACT,
      t: "sd",
      i: envelope.id,
      f: envelope.from,
      m: envelope.syncSessionId,
    };
  }

  if (envelope.t === "chunk") {
    return {
      p: FORUM_PROTO_COMPACT,
      t: "c",
      x: envelope.x,
      k: envelope.k,
      i: envelope.i,
      n: envelope.n,
      d: envelope.d,
      m: envelope.syncSessionId,
    };
  }

  if (envelope.t === "hello") {
    return {
      p: FORUM_PROTO_COMPACT,
      t: "h",
      i: envelope.id,
      s: envelope.ts,
      n: envelope.n,
      a: envelope.a,
      f: envelope.from,
    };
  }

  if (envelope.t === "hello_ack") {
    return {
      p: FORUM_PROTO_COMPACT,
      t: "ha",
      i: envelope.id,
      s: envelope.ts,
      n: envelope.n,
      a: envelope.a,
      f: envelope.from,
    };
  }

  return envelope;
}

function updateOutboundProgress(patch) {
  syncOutbound = {
    ...syncOutbound,
    ...patch,
  };
}

function completeInboundSession() {
  syncInbound = {
    ...syncInbound,
    active: false,
    finishedAt: new Date().toISOString(),
    currentTransfer: null,
    pendingTransfers: incomingChunkTransfers.size,
  };
}

function noteInboundEnvelope(envelope, bytes = 0) {
  const isSessionEnvelope = Boolean(envelope.syncSessionId) && syncInbound.sessionId && envelope.syncSessionId === syncInbound.sessionId;
  const nextCompletedTransfers = isSessionEnvelope
    ? Math.min((syncInbound.completedTransfers || 0) + 1, syncInbound.totalTransfers || 0)
    : syncInbound.completedTransfers || 0;
  const nextReceivedBytes = isSessionEnvelope
    ? Math.min((syncInbound.receivedBytes || 0) + bytes, syncInbound.totalBytes || Number.MAX_SAFE_INTEGER)
    : syncInbound.receivedBytes || 0;

  syncInbound = {
    ...syncInbound,
    completedTransfers: nextCompletedTransfers,
    receivedBytes: nextReceivedBytes,
    transfersCompleted: nextCompletedTransfers,
    lastAt: new Date().toISOString(),
    lastType: envelope.t || null,
  };

  if (isSessionEnvelope && syncInbound.totalTransfers > 0 && nextCompletedTransfers >= syncInbound.totalTransfers) {
    completeInboundSession();
  }
}

function applyTopicCreate(envelope, { remote = false } = {}) {
  if (!envelope.topicId || !envelope.title) {
    return false;
  }
  if (forumState.seenEnvelopeIds.includes(envelope.id)) {
    return false;
  }
  const body = String(envelope.body || "").trim();
  const topic = {
    topicId: String(envelope.topicId),
    title: String(envelope.title).trim(),
    body: body || undefined,
    bodyPreview: body.slice(0, 140),
    authorId: String(envelope.a || ""),
    authorName: String(envelope.n || "unknown"),
    createdAt: envelope.createdAt || nowIsoFromTs(envelope.ts),
    bumpedAt: envelope.createdAt || nowIsoFromTs(envelope.ts),
    initialPostId: envelope.postId ? String(envelope.postId) : undefined,
    sourcePeerId: remote ? String(envelope.from || "") : (meshtasticStatus.localUserId || meshtasticStatus.localNodeId || ""),
  };
  upsertTopic(topic);

  if (envelope.postId && body) {
    upsertPost({
      postId: String(envelope.postId),
      topicId: String(envelope.topicId),
      body,
      authorId: String(envelope.a || ""),
      authorName: String(envelope.n || "unknown"),
      createdAt: envelope.createdAt || nowIsoFromTs(envelope.ts),
      sourcePeerId: remote ? String(envelope.from || "") : (meshtasticStatus.localUserId || meshtasticStatus.localNodeId || ""),
    });
  }

  rememberEnvelopeId(envelope.id);
  if (remote) {
    scheduleBatchedForumStateSave(); // Async batched save for remote messages
  } else {
    saveForumState(); // Sync save for local messages (don't block on remote)
  }
  pushLog(remote ? "forum" : "app", `${remote ? "received" : "created"} topic ${topic.title}`);
  return true;
}

function applyPostCreate(envelope, { remote = false } = {}) {
  if (!envelope.topicId || !envelope.postId || !envelope.body) {
    return false;
  }
  if (forumState.seenEnvelopeIds.includes(envelope.id)) {
    return false;
  }
  const createdAt = envelope.createdAt || nowIsoFromTs(envelope.ts);
  const post = {
    postId: String(envelope.postId),
    topicId: String(envelope.topicId),
    body: String(envelope.body).trim(),
    authorId: String(envelope.a || ""),
    authorName: String(envelope.n || "unknown"),
    createdAt,
    sourcePeerId: remote ? String(envelope.from || "") : (meshtasticStatus.localUserId || meshtasticStatus.localNodeId || ""),
  };
  upsertPost(post);
  const topic = forumState.topics.find((item) => item.topicId === post.topicId);
  if (topic) {
    topic.bumpedAt = createdAt;
    if (!topic.bodyPreview) {
      topic.bodyPreview = post.body.slice(0, 140);
    }
  }
  rememberEnvelopeId(envelope.id);
  if (remote) {
    scheduleBatchedForumStateSave(); // Async batched save for remote messages
  } else {
    saveForumState(); // Sync save for local messages (don't block on remote)
  }
  pushLog(remote ? "forum" : "app", `${remote ? "received" : "created"} post in ${post.topicId}`);
  return true;
}

function buildEnvelope(type, extra = {}) {
  return {
    p: FORUM_PROTO_LEGACY,
    t: type,
    id: createShortId(),
    ts: Date.now(),
    n: localIdentity.displayName.slice(0, 24),
    a: localIdentity.authorId,
    from: meshtasticStatus.localUserId || meshtasticStatus.localNodeId || "unknown",
    ...extra,
  };
}

function serializeEnvelope(envelope) {
  return JSON.stringify(encodeForumEnvelope(envelope));
}

function utf8ByteLength(value) {
  return Buffer.byteLength(String(value || ""), "utf8");
}

function fitsMeshPayload(text) {
  return utf8ByteLength(text) <= MESH_TEXT_PAYLOAD_SAFE_BYTES;
}

function buildChunkEnvelopeRaw({ transferId, transferType, index, total, data, syncSessionId }) {
  return {
    p: FORUM_PROTO_LEGACY,
    t: "chunk",
    x: transferId,
    k: compactForumType(transferType),
    i: index,
    n: total,
    d: data,
    syncSessionId: syncSessionId || undefined,
  };
}

function buildChunkTextForSizing({ transferId, transferType, index, total, data, syncSessionId }) {
  return serializeEnvelope(buildChunkEnvelopeRaw({
    transferId,
    transferType,
    index,
    total,
    data,
    syncSessionId,
  }));
}

function splitSerializedForChunking(serialized, { transferType, syncSessionId, transferIdForSizing }) {
  const parts = [];
  if (!serialized) {
    return parts;
  }

  let cursor = 0;
  let index = 0;
  while (cursor < serialized.length) {
    let low = cursor + 1;
    let high = serialized.length;
    let best = cursor;

    while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      const candidate = serialized.slice(cursor, mid);
      const candidateText = buildChunkTextForSizing({
        transferId: transferIdForSizing,
        transferType,
        index,
        total: CHUNK_TOTAL_HINT,
        data: candidate,
        syncSessionId,
      });
      if (utf8ByteLength(candidateText) <= MESH_TEXT_PAYLOAD_SAFE_BYTES) {
        best = mid;
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }

    if (best === cursor) {
      throw new Error(`chunking failed: payload cannot fit into ${MESH_TEXT_PAYLOAD_SAFE_BYTES} bytes`);
    }

    parts.push(serialized.slice(cursor, best));
    cursor = best;
    index += 1;
  }

  return parts;
}

function summarizeSerializedEnvelope(envelope, serialized) {
  const bytes = utf8ByteLength(serialized);
  const fitsRaw = fitsMeshPayload(serialized);
  let chunks = 1;
  if (!fitsRaw) {
    const chunkParts = splitSerializedForChunking(serialized, {
      transferType: envelope.t,
      syncSessionId: envelope.syncSessionId || null,
      transferIdForSizing: "x000000000000",
    });
    chunks = Math.max(1, chunkParts.length);
  }

  return {
    envelope,
    serialized,
    bytes,
    chunks,
  };
}

function sendRawTextToPeer(destinationId, text) {
  sendBridge({
    type: "send_text",
    payload: {
      destinationId,
      text,
      wantAck: true,
    },
  });
}

function sendEnvelopeToPeer_legacy(destinationId, envelope) {
  const serialized = JSON.stringify(envelope);
  return enqueueMeshSend(async () => {
    if (serialized.length <= FORUM_CHUNK_SIZE) {
      sendBridge({
        type: "send_text",
        payload: {
          destinationId,
          text: serialized,
          wantAck: true,
        },
      });
      return;
    }

    const transferId = crypto.randomUUID();
    const total = Math.ceil(serialized.length / FORUM_CHUNK_SIZE);
    for (let index = 0; index < total; index += 1) {
      const chunkEnvelope = {
        p: "meshforum/1",
        t: "chunk",
        x: transferId,
        k: envelope.t,
        i: index,
        n: total,
        d: serialized.slice(index * FORUM_CHUNK_SIZE, (index + 1) * FORUM_CHUNK_SIZE),
        from: meshtasticStatus.localUserId || meshtasticStatus.localNodeId || "unknown",
      };
      const chunkText = JSON.stringify(chunkEnvelope);
      // wantAck: true → firmware retransmits if no ACK from receiver
      // waitForAck: true → bridge blocks until ACK/timeout before reading next command
      // retryOnAckTimeout: 1 → bridge-level retry if firmware-level retransmission also fails
      sendBridge({
        type: "send_text",
        payload: {
          destinationId,
          text: chunkText,
          wantAck: true,
        },
      });
      await sleep(FORUM_CHUNK_DELAY_MS); // small gap before next chunk write to bridge stdin
    }
  });
}

function sendSerializedEnvelopeToPeer(destinationId, summary, options = {}) {
  const { onChunkSent, onTransferComplete, forceRaw = false } = options;
  const { envelope, serialized, bytes, chunks } = summary;
  return enqueueMeshSend(async () => {
    if (forceRaw || fitsMeshPayload(serialized)) {
      if (!fitsMeshPayload(serialized)) {
        throw new Error(`raw payload too large: ${utf8ByteLength(serialized)} bytes > ${MESH_TEXT_PAYLOAD_SAFE_BYTES} safe bytes`);
      }
      sendRawTextToPeer(destinationId, serialized);
      if (typeof onChunkSent === "function") {
        onChunkSent({ bytes, chunks: 1, transferType: envelope.t });
      }
      if (typeof onTransferComplete === "function") {
        onTransferComplete({ bytes, chunks, transferType: envelope.t });
      }
      return;
    }

    const transferId = createShortId("x");
    const chunkParts = splitSerializedForChunking(serialized, {
      transferType: envelope.t,
      syncSessionId: envelope.syncSessionId || null,
      transferIdForSizing: transferId,
    });
    const total = chunkParts.length;
    for (let index = 0; index < total; index += 1) {
      const chunkEnvelope = buildChunkEnvelopeRaw({
        transferId,
        transferType: envelope.t,
        index,
        total,
        data: chunkParts[index],
        syncSessionId: envelope.syncSessionId || null,
      });
      const chunkText = serializeEnvelope(chunkEnvelope);
      const chunkTextBytes = utf8ByteLength(chunkText);
      if (chunkTextBytes > MESH_TEXT_PAYLOAD_SAFE_BYTES) {
        throw new Error(`chunk payload too large: ${chunkTextBytes} bytes > ${MESH_TEXT_PAYLOAD_SAFE_BYTES} safe bytes`);
      }
      // waitForAck: bridge blocks on ACK before reading next command — prevents flooding
      // retryOnAckTimeout: bridge retries the chunk if firmware ACK times out
      sendBridge({
        type: "send_text",
        payload: {
          destinationId,
          text: chunkText,
          wantAck: true,
          waitForAck: true,
          retryOnAckTimeout: FORUM_CHUNK_RETRY,
          ackTimeoutRetryDelayMs: FORUM_CHUNK_RETRY_DELAY_MS,
        },
      });
      if (typeof onChunkSent === "function") {
        onChunkSent({ bytes: utf8ByteLength(chunkEnvelope.d), chunks: 1, transferType: envelope.t });
      }
      await sleep(FORUM_CHUNK_DELAY_MS);
    }
    if (typeof onTransferComplete === "function") {
      onTransferComplete({ bytes, chunks, transferType: envelope.t });
    }
  });
}

function sendEnvelopeToPeer(destinationId, envelope, options = {}) {
  const serialized = serializeEnvelope(envelope);
  return sendSerializedEnvelopeToPeer(destinationId, summarizeSerializedEnvelope(envelope, serialized), options);
}

function syncForumEnvelopeToSelectedPeer(envelope) {
  if (!bootstrapState.selectedNodeId || !meshtasticStatus.connected) {
    return;
  }
  sendEnvelopeToPeer(bootstrapState.selectedNodeId, envelope).then(() => {
    markPeerKnowsEnvelope(bootstrapState.selectedNodeId, envelope);
    pushLog("outbound", `forum ${envelope.t} sent to ${bootstrapState.selectedNodeId}`, {
      destinationId: bootstrapState.selectedNodeId,
      envelopeId: envelope.id,
    });
  }).catch((error) => {
    pushLog("error", `forum send failed: ${error.message}`);
  });
}

// Sends all local forum data to a specific peer (used for both push and responding to sync_request)
function createSyncSession({ sessionId, peerId, role, localEnvelopes = [], remoteCount = 0 }) {
  const session = {
    sessionId,
    peerId,
    role,
    localEnvelopes,
    remoteCount: Number(remoteCount || 0),
    startedAt: new Date().toISOString(),
    waitingForTurn: role === "responder",
    sentLocal: false,
    remoteDone: false,
  };
  syncSessions.set(sessionId, session);
  return session;
}

async function sendSyncDataBatch(session, envelopes, direction) {
  const summarized = envelopes.map((env) => summarizeSerializedEnvelope(env, serializeEnvelope(env)));
  const totalTransfers = summarized.length;
  const totalChunks = summarized.reduce((sum, item) => sum + item.chunks, 0);
  const totalBytes = summarized.reduce((sum, item) => sum + item.bytes, 0);

  startSyncProgress({
    sessionId: session.sessionId,
    peerId: session.peerId,
    phase: direction === "sending" ? "sending" : "receiving",
    direction,
    total: totalTransfers,
    totalBytes,
    label: direction === "sending" ? "Sending updates" : "Receiving updates",
  });

  syncOutbound = {
    active: direction === "sending",
    sessionId: session.sessionId,
    totalTransfers,
    completedTransfers: 0,
    totalChunks,
    sentChunks: 0,
    totalBytes,
    sentBytes: 0,
    startedAt: new Date().toISOString(),
    finishedAt: null,
    target: session.peerId,
  };

  for (const item of summarized) {
    await sendSerializedEnvelopeToPeer(session.peerId, item, {
      onChunkSent: ({ bytes }) => {
        updateOutboundProgress({
          sentChunks: Math.min(syncOutbound.sentChunks + 1, syncOutbound.totalChunks),
          sentBytes: Math.min(syncOutbound.sentBytes + bytes, syncOutbound.totalBytes),
        });
        updateSyncProgress({
          completedBytes: Math.min((syncProgress.completedBytes || 0) + bytes, syncProgress.totalBytes || Number.MAX_SAFE_INTEGER),
          label:             `Sending ${Math.min(syncOutbound.sentBytes + bytes, syncOutbound.totalBytes || 0)}/${syncOutbound.totalBytes || 0}B`,
        });
      },
      onTransferComplete: () => {
        const completedTransfers = Math.min(syncOutbound.completedTransfers + 1, syncOutbound.totalTransfers);
        updateOutboundProgress({
          completedTransfers,
          active: completedTransfers < syncOutbound.totalTransfers,
          finishedAt: completedTransfers >= syncOutbound.totalTransfers ? new Date().toISOString() : syncOutbound.finishedAt,
        });
        updateSyncProgress({
          completed: completedTransfers,
          label: `Sending updates ${completedTransfers}/${syncOutbound.totalTransfers}`,
        });
        markPeerKnowsEnvelope(session.peerId, item.envelope);
      },
    });
  }

  updateOutboundProgress({
    active: false,
    finishedAt: new Date().toISOString(),
  });
  updateSyncProgress({
    completed: totalTransfers,
    completedBytes: totalBytes,
    phase: "waiting",
    label: direction === "sending" ? "Waiting for peer turn" : "Receiving complete",
  });
}

async function initiatorSendAndPassTurn(session) {
  await sendSyncDataBatch(session, session.localEnvelopes, "sending");
  session.sentLocal = true;
  const turn = buildEnvelope("sync_turn", { syncSessionId: session.sessionId });
  await sendEnvelopeToPeer(session.peerId, turn);
  pushLog("forum", `sync turn passed to ${session.peerId}`, {
    sessionId: session.sessionId,
    localSent: session.localEnvelopes.length,
    remoteExpected: session.remoteCount,
  });
  if (session.remoteCount === 0) {
    const done = buildEnvelope("sync_done", { syncSessionId: session.sessionId });
    await sendEnvelopeToPeer(session.peerId, done);
    session.remoteDone = true;
    syncSessions.delete(session.sessionId);
    finishSyncProgress("Sync complete");
  } else {
    syncInbound = {
      ...syncInbound,
      active: true,
      sessionId: session.sessionId,
      source: session.peerId,
      totalTransfers: session.remoteCount,
      completedTransfers: 0,
      totalChunks: 0,
      receivedChunks: 0,
      totalBytes: 0,
      receivedBytes: 0,
      startedAt: new Date().toISOString(),
      finishedAt: null,
      currentTransfer: null,
      pendingTransfers: 0,
    };
    updateSyncProgress({
      phase: "receiving",
      direction: "receiving",
      total: session.remoteCount,
      completed: 0,
      totalBytes: 0,
      completedBytes: 0,
      label: `Waiting peer updates 0/${session.remoteCount}`,
    });
  }
}

async function responderSendAfterTurn(session) {
  await sendSyncDataBatch(session, session.localEnvelopes, "sending");
  const done = buildEnvelope("sync_done", { syncSessionId: session.sessionId });
  await sendEnvelopeToPeer(session.peerId, done);
  session.sentLocal = true;
  session.remoteDone = true;
  syncSessions.delete(session.sessionId);
  finishSyncProgress("Sync complete");
}

function syncAllForumToSelectedPeer() {
  if (syncProgress.active) {
    throw new Error("sync already in progress");
  }
  if (!bootstrapState.selectedNodeId) {
    throw new Error("no selected bootstrap peer");
  }
  if (!meshtasticStatus.connected) {
    throw new Error("Meshtastic bridge is not connected");
  }
  const peerId = bootstrapState.selectedNodeId;
  const sessionId = createShortId("s");
  const localEnvelopes = collectMissingEnvelopesForPeer(peerId, sessionId);
  const session = createSyncSession({
    sessionId,
    peerId,
    role: "initiator",
    localEnvelopes,
    remoteCount: 0,
  });
  startSyncProgress({
    sessionId,
    peerId,
    phase: "negotiating",
    direction: "sending",
    total: localEnvelopes.length,
    totalBytes: 0,
    label: `Sync offer: I have ${localEnvelopes.length} updates`,
  });

  const offer = buildEnvelope("sync_offer", {
    syncSessionId: sessionId,
    updatesCount: localEnvelopes.length,
  });
  sendEnvelopeToPeer(peerId, offer).catch((error) => {
    syncSessions.delete(session.sessionId);
    updateSyncProgress({
      active: false,
      phase: "error",
      label: `Sync error: ${error.message}`,
      finishedAt: new Date().toISOString(),
    });
    pushLog("error", `sync offer send failed: ${error.message}`);
  });
  pushLog("forum", `sync_offer sent to ${peerId}`, {
    sessionId,
    localUpdates: localEnvelopes.length,
  });
  return localEnvelopes.length;
}

function handleForumEnvelope(envelope, payload) {
  if (!envelope || !envelope.t) {
    return;
  }

  if (envelope.t === "sync_offer") {
    const requester = payload?.sender || envelope.from;
    const sessionId = String(envelope.syncSessionId || "");
    const incomingCount = Math.max(0, Number(envelope.updatesCount || 0));
    if (!requester || !sessionId) {
      pushLog("error", "sync_offer missing sender/sessionId");
      return;
    }
    const localEnvelopes = collectMissingEnvelopesForPeer(requester, sessionId);
    const session = createSyncSession({
      sessionId,
      peerId: requester,
      role: "responder",
      localEnvelopes,
      remoteCount: incomingCount,
    });
    syncInbound = {
      ...syncInbound,
      active: incomingCount > 0,
      sessionId,
      source: requester,
      totalTransfers: incomingCount,
      completedTransfers: 0,
      totalChunks: 0,
      receivedChunks: 0,
      totalBytes: 0,
      receivedBytes: 0,
      startedAt: new Date().toISOString(),
      finishedAt: null,
      currentTransfer: null,
      pendingTransfers: 0,
    };
    startSyncProgress({
      sessionId,
      peerId: requester,
      phase: incomingCount > 0 ? "receiving" : "negotiating",
      direction: incomingCount > 0 ? "receiving" : "sending",
      total: incomingCount > 0 ? incomingCount : localEnvelopes.length,
      totalBytes: 0,
      label: `Peer has ${incomingCount}, I have ${localEnvelopes.length}`,
    });
    const ack = buildEnvelope("sync_offer_ack", {
      syncSessionId: sessionId,
      updatesCount: localEnvelopes.length,
    });
    sendEnvelopeToPeer(requester, ack).catch((error) => {
      pushLog("error", `sync_offer_ack send failed: ${error.message}`);
    });
    pushLog("forum", `sync_offer from ${requester}`, {
      sessionId,
      incomingCount,
      outgoingCount: localEnvelopes.length,
      role: session.role,
    });
    return;
  }

  if (envelope.t === "sync_offer_ack") {
    const peerId = payload?.sender || envelope.from;
    const sessionId = String(envelope.syncSessionId || "");
    const session = syncSessions.get(sessionId);
    if (!peerId || !session || session.role !== "initiator" || session.peerId !== peerId) {
      return;
    }
    session.remoteCount = Math.max(0, Number(envelope.updatesCount || 0));
    pushLog("forum", `sync_offer_ack from ${peerId}`, {
      sessionId,
      remoteCount: session.remoteCount,
      localCount: session.localEnvelopes.length,
    });
    initiatorSendAndPassTurn(session).catch((error) => {
      syncSessions.delete(sessionId);
      updateSyncProgress({
        active: false,
        phase: "error",
        label: `Sync error: ${error.message}`,
        finishedAt: new Date().toISOString(),
      });
      pushLog("error", `initiator send failed: ${error.message}`);
    });
    return;
  }

  if (envelope.t === "sync_turn") {
    const peerId = payload?.sender || envelope.from;
    const sessionId = String(envelope.syncSessionId || "");
    const session = syncSessions.get(sessionId);
    if (!peerId || !session || session.role !== "responder" || session.peerId !== peerId) {
      return;
    }
    responderSendAfterTurn(session).catch((error) => {
      syncSessions.delete(sessionId);
      updateSyncProgress({
        active: false,
        phase: "error",
        label: `Sync error: ${error.message}`,
        finishedAt: new Date().toISOString(),
      });
      pushLog("error", `responder send failed: ${error.message}`);
    });
    return;
  }

  if (envelope.t === "sync_done") {
    const peerId = payload?.sender || envelope.from;
    const sessionId = String(envelope.syncSessionId || "");
    const session = syncSessions.get(sessionId);
    if (session) {
      session.remoteDone = true;
      syncSessions.delete(sessionId);
    }
    syncInbound = {
      ...syncInbound,
      active: false,
      finishedAt: new Date().toISOString(),
      currentTransfer: null,
      pendingTransfers: incomingChunkTransfers.size,
    };
    finishSyncProgress(`Sync complete with ${peerId || "peer"}`);
    pushLog("forum", `sync_done from ${peerId || "unknown"}`, { sessionId });
    return;
  }

  if (envelope.t === "sync_start") {
    syncInbound = {
      ...syncInbound,
      active: true,
      sessionId: envelope.syncSessionId || null,
      totalTransfers: Number(envelope.totalTransfers || 0),
      completedTransfers: 0,
      totalChunks: Number(envelope.totalChunks || 0),
      receivedChunks: 0,
      totalBytes: Number(envelope.totalBytes || 0),
      receivedBytes: 0,
      transfersCompleted: 0,
      chunksReceived: 0,
      lastAt: new Date().toISOString(),
      lastType: envelope.t,
      startedAt: new Date().toISOString(),
      finishedAt: null,
      currentTransfer: null,
      pendingTransfers: 0,
      source: payload?.sender || envelope.from || "unknown",
    };
    pushLog("forum", `sync_start from ${syncInbound.source}`, {
      sessionId: syncInbound.sessionId,
      totalTransfers: syncInbound.totalTransfers,
      totalChunks: syncInbound.totalChunks,
      totalBytes: syncInbound.totalBytes,
    });
    return;
  }

  if (envelope.t === "chunk") {
    const transferId = String(envelope.x || "");
    const total = Number(envelope.n || 0);
    const index = Number(envelope.i || 0);
    if (!transferId || !total || index < 0 || index >= total) {
      return;
    }

    const current = incomingChunkTransfers.get(transferId) || {
      total,
      parts: new Array(total).fill(null),
      receivedBytes: 0,
    };
    const data = String(envelope.d || "");
    if (current.parts[index] === null) {
      current.receivedBytes += utf8ByteLength(data);
    }
    current.parts[index] = data;
    incomingChunkTransfers.set(transferId, current);

    const received = current.parts.filter((p) => p !== null).length;
    const chunkKind = String(envelope.k || "");
    const isSyncStartChunk = chunkKind === "sync_start" || chunkKind === "ss";
    const isSessionChunk = Boolean(syncInbound.active) && !isSyncStartChunk;

    // Update current transfer progress for any regular chunk (whether session is fully active or not)
    if (!isSyncStartChunk) {
      const updates = {
        lastAt: new Date().toISOString(),
        pendingTransfers: incomingChunkTransfers.size,
        currentTransfer: {
          received,
          total,
          transferId: transferId.slice(0, 8),
        },
      };

      // Add session counters if this is a session chunk
      if (isSessionChunk) {
        updates.chunksReceived = (syncInbound.chunksReceived || 0) + 1;
        updates.receivedChunks = Math.min((syncInbound.receivedChunks || 0) + 1, syncInbound.totalChunks || Number.MAX_SAFE_INTEGER);
        updates.receivedBytes = Math.min((syncInbound.receivedBytes || 0) + utf8ByteLength(data), syncInbound.totalBytes || Number.MAX_SAFE_INTEGER);
      }

      syncInbound = {
        ...syncInbound,
        ...updates,
      };
    }

    if (current.parts.every((item) => typeof item === "string")) {
      incomingChunkTransfers.delete(transferId);
      if (isSessionChunk) {
        syncInbound = {
          ...syncInbound,
          currentTransfer: null,
          pendingTransfers: incomingChunkTransfers.size,
        };
      }
      try {
        const reconstructed = JSON.parse(current.parts.join(""));
        const normalized = decodeForumEnvelope(reconstructed);
        if (normalized) {
          normalized._receivedViaChunk = true;
          syncInbound.lastType = normalized.t || null;
          handleForumEnvelope(normalized, payload);
        }
      } catch (error) {
        pushLog("error", `chunk reassembly failed: ${error.message}`, { transferId });
      }
    }
    return;
  }

  if (envelope.t === "hello") {
    if (payload?.sender) {
      const reply = buildEnvelope("hello_ack");
      try {
        sendEnvelopeToPeer(String(payload.sender), reply);
        pushLog("forum", `hello from ${payload.sender}, hello_ack sent`, { sender: payload.sender });
      } catch (error) {
        pushLog("error", `hello_ack send failed: ${error.message}`);
      }
    }
    return;
  }

  if (envelope.t === "hello_ack") {
    bootstrapState = {
      ...bootstrapState,
      lastHelloAckAt: new Date().toISOString(),
    };
    saveBootstrapState();
    pushLog("forum", `hello_ack from ${payload?.sender || envelope.from || "unknown"}`);
    return;
  }

  if (envelope.t === "sync_request") {
    pushLog("forum", "legacy sync_request received (ignored)");
    return;
  }

  if (envelope.t === "topic_create") {
    const sender = payload?.sender || envelope.from || null;
    if (applyTopicCreate(envelope, { remote: true })) {
      noteInboundEnvelope(envelope, envelope._receivedViaChunk ? 0 : utf8ByteLength(serializeEnvelope(envelope)));
      if (sender) {
        markPeerKnowsEnvelope(sender, envelope);
      }
      if (envelope.syncSessionId && syncProgress.active && syncProgress.sessionId === envelope.syncSessionId) {
        const completed = Math.min((syncProgress.completed || 0) + 1, syncProgress.total || Number.MAX_SAFE_INTEGER);
        updateSyncProgress({
          direction: "receiving",
          phase: "receiving",
          completed,
          label: `Receiving updates ${completed}/${syncProgress.total || "?"}`,
        });
      }
    }
    return;
  }

  if (envelope.t === "post_create") {
    const sender = payload?.sender || envelope.from || null;
    if (applyPostCreate(envelope, { remote: true })) {
      noteInboundEnvelope(envelope, envelope._receivedViaChunk ? 0 : utf8ByteLength(serializeEnvelope(envelope)));
      if (sender) {
        markPeerKnowsEnvelope(sender, envelope);
      }
      if (envelope.syncSessionId && syncProgress.active && syncProgress.sessionId === envelope.syncSessionId) {
        const completed = Math.min((syncProgress.completed || 0) + 1, syncProgress.total || Number.MAX_SAFE_INTEGER);
        updateSyncProgress({
          direction: "receiving",
          phase: "receiving",
          completed,
          label: `Receiving updates ${completed}/${syncProgress.total || "?"}`,
        });
      }
    }
  }
}

function pushLog(kind, summary, payload = null) {
  const entry = {
    id: crypto.randomUUID(),
    ts: new Date().toISOString(),
    kind,
    summary,
    payload,
  };
  bridgeLogs.push(entry);
  bridgeLogs = bridgeLogs.slice(-200);
  try {
    fs.appendFileSync(LOG_FILE, `${JSON.stringify(entry)}\n`, "utf8");
  } catch {}
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function saveBootstrapState() {
  writeJson(BOOTSTRAP_FILE, bootstrapState);
}

function getPeerKnowledgeRecord(peerId) {
  const normalizedPeerId = String(peerId || "").trim();
  if (!normalizedPeerId) {
    return null;
  }
  if (!bootstrapState.peerKnowledge || typeof bootstrapState.peerKnowledge !== "object") {
    bootstrapState.peerKnowledge = {};
  }
  const existing = bootstrapState.peerKnowledge[normalizedPeerId];
  if (existing && Array.isArray(existing.topics) && Array.isArray(existing.posts)) {
    return existing;
  }
  const created = {
    topics: [],
    posts: [],
    updatedAt: null,
  };
  bootstrapState.peerKnowledge[normalizedPeerId] = created;
  return created;
}

function rememberPeerItem(list, value, cap = 10000) {
  const text = String(value || "").trim();
  if (!text) {
    return false;
  }
  if (list.includes(text)) {
    return false;
  }
  list.push(text);
  if (list.length > cap) {
    list.splice(0, list.length - cap);
  }
  return true;
}

function markPeerKnowsEnvelope(peerId, envelope) {
  const record = getPeerKnowledgeRecord(peerId);
  if (!record || !envelope || !envelope.t) {
    return false;
  }
  let changed = false;
  if (envelope.t === "topic_create" && envelope.topicId) {
    changed = rememberPeerItem(record.topics, envelope.topicId) || changed;
    if (envelope.postId) {
      changed = rememberPeerItem(record.posts, envelope.postId) || changed;
    }
  }
  if (envelope.t === "post_create" && envelope.postId) {
    changed = rememberPeerItem(record.posts, envelope.postId) || changed;
  }
  if (changed) {
    record.updatedAt = new Date().toISOString();
    saveBootstrapState();
  }
  return changed;
}

function updateSyncProgress(patch = {}) {
  syncProgress = {
    ...syncProgress,
    ...patch,
    updatedAt: new Date().toISOString(),
  };
}

function startSyncProgress({ sessionId, peerId, phase, direction, total = 0, totalBytes = 0, label }) {
  syncProgress = {
    active: true,
    sessionId: sessionId || null,
    peerId: peerId || null,
    phase: phase || "negotiating",
    direction: direction || "idle",
    completed: 0,
    total: Math.max(0, Number(total || 0)),
    completedBytes: 0,
    totalBytes: Math.max(0, Number(totalBytes || 0)),
    startedAt: new Date().toISOString(),
    finishedAt: null,
    updatedAt: new Date().toISOString(),
    label: label || "Syncing",
  };
}

function finishSyncProgress(label = "Sync complete") {
  syncProgress = {
    ...syncProgress,
    active: false,
    phase: "done",
    direction: "idle",
    finishedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    label,
  };
}

function findInitialPostForTopic(topic) {
  if (!topic) {
    return null;
  }
  if (topic.initialPostId) {
    const explicit = forumState.posts.find((post) => post.postId === topic.initialPostId);
    if (explicit) {
      return explicit;
    }
  }
  return forumState.posts
    .filter((post) => post.topicId === topic.topicId)
    .sort((a, b) => String(a.createdAt || "").localeCompare(String(b.createdAt || "")))[0] || null;
}

function buildTopicEnvelopeFromState(topic, sessionId) {
  const initialPost = findInitialPostForTopic(topic);
  const body = String(topic.body || initialPost?.body || topic.bodyPreview || "");
  return buildEnvelope("topic_create", {
    syncSessionId: sessionId,
    topicId: topic.topicId,
    title: topic.title,
    postId: topic.initialPostId || initialPost?.postId || undefined,
    body,
    ts: new Date(topic.createdAt).getTime(),
  });
}

function buildPostEnvelopeFromState(post, sessionId) {
  return buildEnvelope("post_create", {
    syncSessionId: sessionId,
    topicId: post.topicId,
    postId: post.postId,
    body: post.body,
    ts: new Date(post.createdAt).getTime(),
  });
}

function collectMissingEnvelopesForPeer(peerId, sessionId) {
  const record = getPeerKnowledgeRecord(peerId) || { topics: [], posts: [] };
  const knownTopics = new Set(record.topics || []);
  const knownPosts = new Set(record.posts || []);
  const envelopes = [];

  for (const topic of forumState.topics) {
    const initialPost = findInitialPostForTopic(topic);
    const initialPostId = topic.initialPostId || initialPost?.postId || null;
    if (!knownTopics.has(topic.topicId)) {
      envelopes.push(buildTopicEnvelopeFromState(topic, sessionId));
      continue;
    }
    if (initialPostId && !knownPosts.has(initialPostId) && initialPost) {
      envelopes.push(buildPostEnvelopeFromState(initialPost, sessionId));
    }
  }
  for (const post of forumState.posts) {
    const parentTopic = forumState.topics.find((topic) => topic.topicId === post.topicId);
    const initialPost = parentTopic ? findInitialPostForTopic(parentTopic) : null;
    const isInitialTopicPost = Boolean(initialPost && initialPost.postId === post.postId);
    if (isInitialTopicPost) {
      continue;
    }
    if (!knownPosts.has(post.postId)) {
      envelopes.push(buildPostEnvelopeFromState(post, sessionId));
    }
  }
  return envelopes;
}

function sanitizeIdentity(identity) {
  return {
    authorId: identity.authorId,
    displayName: identity.displayName,
    publicKeyPem: identity.publicKeyPem,
    createdAt: identity.createdAt,
  };
}

function contentType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  switch (ext) {
    case ".html":
      return "text/html; charset=utf-8";
    case ".css":
      return "text/css; charset=utf-8";
    case ".js":
      return "application/javascript; charset=utf-8";
    case ".json":
      return "application/json; charset=utf-8";
    case ".svg":
      return "image/svg+xml";
    case ".png":
      return "image/png";
    case ".ico":
      return "image/x-icon";
    case ".woff2":
      return "font/woff2";
    case ".ttf":
      return "font/ttf";
    default:
      return "application/octet-stream";
  }
}

function sendJson(res, statusCode, payload) {
  const body = JSON.stringify(payload, null, 2);
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(body, "utf8"),
    "Cache-Control": "no-store",
  });
  res.end(body);
}

function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk.toString("utf8");
      if (body.length > 1024 * 1024) {
        reject(new Error("request body too large"));
      }
    });
    req.on("end", () => {
      if (!body.trim()) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(body));
      } catch {
        reject(new Error("invalid json body"));
      }
    });
    req.on("error", reject);
  });
}

function trySpawnSync(command, args) {
  try {
    const result = spawnSync(command, args, {
      stdio: "pipe",
      encoding: "utf8",
      timeout: 5000,
    });
    return result && result.status === 0;
  } catch {
    return false;
  }
}

function findPythonLauncher() {
  const candidates = process.platform === "win32"
    ? [
        { command: "cmd", args: ["/c", "python"] },
        { command: "cmd", args: ["/c", "py", "-3"] },
      ]
    : [
        { command: "python3", args: [] },
        { command: "python", args: [] },
      ];

  for (const candidate of candidates) {
    if (trySpawnSync(candidate.command, [...candidate.args, "--version"])) {
      return candidate;
    }
  }
  return null;
}

function openBrowser(url) {
  if (process.env.NO_OPEN_BROWSER === "1") {
    return;
  }

  try {
    if (process.platform === "win32") {
      const child = spawn("cmd", ["/c", "start", "", url], {
        detached: true,
        stdio: "ignore",
      });
      child.unref();
      return;
    }

    if (process.platform === "darwin") {
      const child = spawn("open", [url], { detached: true, stdio: "ignore" });
      child.unref();
      return;
    }

    const child = spawn("xdg-open", [url], { detached: true, stdio: "ignore" });
    child.unref();
  } catch (error) {
    pushLog("app", `browser open failed: ${error.message}`);
  }
}

function sendBridge(message) {
  if (!bridgeProcess || bridgeProcess.killed || !bridgeProcess.stdin.writable) {
    throw new Error("bridge is not running");
  }
  bridgeProcess.stdin.write(`${JSON.stringify(message)}\n`);
}

function enqueueMeshSend(task) {
  const queued = meshSendQueue.catch(() => {}).then(task);
  meshSendQueue = queued;
  return queued;
}

function enqueueInboundProcessing(task) {
  const queued = inboundProcessingQueue.catch(() => {}).then(task);
  inboundProcessingQueue = queued;
  return queued;
}

function mergeMeshtasticStatus(patch) {
  meshtasticStatus = { ...meshtasticStatus, ...patch };
}

function parseForumEnvelope(text) {
  try {
    return decodeForumEnvelope(JSON.parse(String(text || "")));
  } catch {}
  return null;
}

function handleBridgeMessage(message) {
  const payload = message && typeof message === "object" ? message.payload || {} : {};
  switch (message.type) {
    case "status":
      mergeMeshtasticStatus({ ...payload, bridgeRunning: true });
      break;
    case "nodes":
      observedNodes = Array.isArray(payload.nodes) ? payload.nodes : [];
      // Keep bootstrap label fresh whenever node list updates
      if (bootstrapState.selectedNodeId) {
        const peer = observedNodes.find(
          (n) => String(n.userId || n.id || "") === String(bootstrapState.selectedNodeId)
        );
        if (peer) {
          const freshLabel = String(peer.longName || peer.shortName || "").trim();
          if (freshLabel) {
            bootstrapState = { ...bootstrapState, selectedNodeLabel: freshLabel };
            saveBootstrapState();
          }
        }
      }
      break;
    case "inbound": {
      const forumEnvelope = parseForumEnvelope(payload.text);
      if (forumEnvelope) {
        handleForumEnvelope(forumEnvelope, payload);
      }
      pushLog(
        "inbound",
        forumEnvelope
          ? `forum ${forumEnvelope.t} from ${payload.sender || "unknown"}`
          : `text from ${payload.sender || "unknown"}`,
        payload,
      );
      break;
    }
    case "sent":
      pushLog("outbound", `sent DM to ${payload.destinationId || "unknown"}`, payload);
      break;
    case "error":
      pushLog("error", payload.message || "bridge error", payload);
      if (payload.message) {
        mergeMeshtasticStatus({ error: payload.message });
      }
      break;
    default:
      pushLog("bridge", `bridge event: ${message.type || "unknown"}`, payload);
      break;
  }
}

function startBridge() {
  const launcher = findPythonLauncher();
  if (!launcher) {
    mergeMeshtasticStatus({
      pythonAvailable: false,
      bridgeRunning: false,
      mode: "error",
      error: "Python 3.11+ not found in PATH",
    });
    pushLog("error", "Python launcher not found");
    return;
  }

  mergeMeshtasticStatus({
    pythonAvailable: true,
    bridgeRunning: false,
    mode: "starting",
    error: null,
  });

  bridgeProcess = spawn(launcher.command, [...launcher.args, BRIDGE_PATH], {
    cwd: ROOT_DIR,
    env: { ...process.env, PYTHONUNBUFFERED: "1" },
    stdio: ["pipe", "pipe", "pipe"],
  });

  bridgeProcess.stdout.on("data", (chunk) => {
    bridgeStdoutBuffer += chunk.toString("utf8");
    let newlineIndex = bridgeStdoutBuffer.indexOf("\n");
    while (newlineIndex >= 0) {
      const line = bridgeStdoutBuffer.slice(0, newlineIndex).trim();
      bridgeStdoutBuffer = bridgeStdoutBuffer.slice(newlineIndex + 1);
      if (line) {
        try {
          handleBridgeMessage(JSON.parse(line));
        } catch (error) {
          pushLog("error", `bridge stdout parse failed: ${error.message}`, { line });
        }
      }
      newlineIndex = bridgeStdoutBuffer.indexOf("\n");
    }
  });

  bridgeProcess.stderr.on("data", (chunk) => {
    bridgeStderrBuffer += chunk.toString("utf8");
    const lines = bridgeStderrBuffer.split(/\r?\n/);
    bridgeStderrBuffer = lines.pop() || "";
    for (const line of lines) {
      const text = line.trim();
      if (text) {
        pushLog("stderr", text);
      }
    }
  });

  bridgeProcess.on("exit", (code) => {
    mergeMeshtasticStatus({
      connected: false,
      bridgeRunning: false,
      mode: "stopped",
      error: `bridge exited with code ${code}`,
    });
    pushLog("error", `bridge exited with code ${code}`);
    bridgeProcess = null;
  });
}

function buildHelloMessage() {
  const messageId = crypto.randomUUID();
  const envelope = {
    p: "meshforum/1",
    t: "hello",
    id: messageId,
    ts: Date.now(),
    n: localIdentity.displayName.slice(0, 24),
    a: localIdentity.authorId,
    from: meshtasticStatus.localUserId || meshtasticStatus.localNodeId || "unknown",
  };
  return {
    messageId,
    text: JSON.stringify(envelope),
  };
}

function serveStatic(req, res, pathname) {
  let filePath = path.join(STATIC_DIR, pathname === "/" ? "index.html" : pathname.slice(1));
  if (!filePath.startsWith(STATIC_DIR)) {
    sendJson(res, 403, { error: "forbidden" });
    return;
  }

  try {
    const stat = fs.statSync(filePath);
    if (stat.isDirectory()) {
      filePath = path.join(filePath, "index.html");
    }
    const data = fs.readFileSync(filePath);
    res.writeHead(200, {
      "Content-Type": contentType(filePath),
      "Content-Length": data.length,
      "Cache-Control": "no-store",
    });
    res.end(data);
  } catch {
    sendJson(res, 404, { error: "not found" });
  }
}

const server = http.createServer(async (req, res) => {
  const requestUrl = new URL(req.url, `http://${req.headers.host || `${HOST}:${PORT}`}`);
  const pathname = requestUrl.pathname;

  try {
    if (req.method === "GET" && pathname === "/api/status") {
      // Enrich bootstrap label with the freshest name from observedNodes
      const enrichedBootstrap = { ...bootstrapState };
      if (bootstrapState.selectedNodeId) {
        const peer = observedNodes.find(
          (n) => String(n.userId || n.id || "") === String(bootstrapState.selectedNodeId)
        );
        if (peer) {
          const freshLabel = String(peer.longName || peer.shortName || "").trim();
          if (freshLabel) enrichedBootstrap.selectedNodeLabel = freshLabel;
        }
      }
      return sendJson(res, 200, {
        app: {
          name: "meshforum",
          version: APP_VERSION,
          instanceLabel: INSTANCE_LABEL,
          startedAt,
          host: HOST,
          port: PORT,
          platform: process.platform,
          dataDir: DATA_DIR,
          logFile: LOG_FILE,
          meshtasticPort: process.env.MESHTASTIC_PORT || null,
        },
        identity: sanitizeIdentity(localIdentity),
        meshtastic: meshtasticStatus,
        bootstrap: enrichedBootstrap,
        observedNodeCount: observedNodes.length,
        forum: {
          topicCount: forumState.topics.length,
          postCount: forumState.posts.length,
        },
      });
    }

    if (req.method === "GET" && pathname === "/api/sync-status") {
      return sendJson(res, 200, {
        sync: syncProgress,
        outbound: {
          ...syncOutbound,
          total: syncOutbound.totalTransfers,
          sent: syncOutbound.completedTransfers,
        },
        inbound: {
          ...syncInbound,
          total: syncInbound.totalTransfers,
          sent: syncInbound.completedTransfers,
        },
      });
    }

    if (req.method === "GET" && pathname === "/api/bootstrap/candidates") {
      return sendJson(res, 200, { nodes: observedNodes });
    }

    if (req.method === "GET" && pathname === "/api/logs") {
      return sendJson(res, 200, { logs: bridgeLogs });
    }

    if (req.method === "GET" && pathname === "/api/logs/file") {
      return sendJson(res, 200, {
        path: LOG_FILE,
        logs: readLogFile(),
      });
    }

    if (req.method === "GET" && pathname === "/api/forum") {
      return sendJson(res, 200, buildForumPayload());
    }

    if (req.method === "POST" && pathname === "/api/profile") {
      const body = await readRequestBody(req);
      const displayName = String(body.displayName || "").trim();
      if (!displayName || displayName.length > 24) {
        return sendJson(res, 400, { error: "displayName must be 1..24 chars" });
      }
      localIdentity.displayName = displayName;
      writeJson(IDENTITY_FILE, localIdentity);
      pushLog("app", `display name updated to ${displayName}`);
      return sendJson(res, 200, { ok: true, identity: sanitizeIdentity(localIdentity) });
    }

    if (req.method === "POST" && pathname === "/api/bootstrap/refresh") {
      try {
        sendBridge({ type: "refresh_nodes" });
      } catch (error) {
        return sendJson(res, 503, { error: error.message });
      }
      return sendJson(res, 200, { ok: true });
    }

    if (req.method === "POST" && pathname === "/api/bootstrap/connect") {
      const body = await readRequestBody(req);
      const targetNodeId = String(body.targetNodeId || "").trim();
      if (!targetNodeId) {
        return sendJson(res, 400, { error: "targetNodeId is required" });
      }

      const selectedNode = observedNodes.find((node) => String(node.userId || node.id || "") === targetNodeId);
      if (!selectedNode) {
        return sendJson(res, 404, { error: "selected node not found in observed node list" });
      }
      if (
        targetNodeId === String(meshtasticStatus.localUserId || "") ||
        targetNodeId === String(meshtasticStatus.localNodeId || "")
      ) {
        return sendJson(res, 400, { error: "cannot select local node as bootstrap peer" });
      }

      if (!meshtasticStatus.connected) {
        return sendJson(res, 503, { error: "Meshtastic bridge is not connected" });
      }

      const hello = buildHelloMessage();
      sendEnvelopeToPeer(targetNodeId, JSON.parse(hello.text)).catch((error) => {
        pushLog("error", `bootstrap hello send failed: ${error.message}`);
      });

      bootstrapState = {
        selectedNodeId: targetNodeId,
        selectedNodeLabel: String(selectedNode.longName || selectedNode.shortName || targetNodeId),
        lastHelloAt: new Date().toISOString(),
        lastHelloMessageId: hello.messageId,
      };
      saveBootstrapState();
      pushLog("outbound", `bootstrap hello sent to ${targetNodeId}`, {
        targetNodeId,
        messageId: hello.messageId,
      });

      return sendJson(res, 200, { ok: true, bootstrap: bootstrapState });
    }

    if (req.method === "POST" && pathname === "/api/bootstrap/sync-selected") {
      if (!meshtasticStatus.connected) {
        return sendJson(res, 503, { error: "Meshtastic bridge is not connected" });
      }
      try {
        const sentCount = syncAllForumToSelectedPeer();
        return sendJson(res, 200, { ok: true, sentCount });
      } catch (error) {
        return sendJson(res, 400, { error: error.message });
      }
    }

    if (req.method === "POST" && pathname === "/api/topics") {
      const body = await readRequestBody(req);
      const title = String(body.title || "").trim();
      const postBody = String(body.body || "").trim();
      if (!title || title.length > 120) {
        return sendJson(res, 400, { error: "title must be 1..120 chars" });
      }
      if (!postBody || postBody.length > 1200) {
        return sendJson(res, 400, { error: "body must be 1..1200 chars" });
      }

      const topicId = createShortId("t");
      const postId = createShortId("p");
      const topicEnvelope = buildEnvelope("topic_create", {
        topicId,
        title,
        body: postBody,
        postId,
      });
      applyTopicCreate(topicEnvelope);

      syncForumEnvelopeToSelectedPeer(topicEnvelope);
      return sendJson(res, 200, { ok: true, forum: buildForumPayload() });
    }

    if (req.method === "POST" && pathname.startsWith("/api/topics/") && pathname.endsWith("/posts")) {
      const topicId = pathname.slice("/api/topics/".length, -"/posts".length);
      const topic = forumState.topics.find((item) => item.topicId === topicId);
      if (!topic) {
        return sendJson(res, 404, { error: "topic not found" });
      }
      const body = await readRequestBody(req);
      const postBody = String(body.body || "").trim();
      if (!postBody || postBody.length > 1200) {
        return sendJson(res, 400, { error: "body must be 1..1200 chars" });
      }
      const postEnvelope = buildEnvelope("post_create", {
        topicId,
        postId: createShortId("p"),
        body: postBody,
      });
      applyPostCreate(postEnvelope);
      syncForumEnvelopeToSelectedPeer(postEnvelope);
      return sendJson(res, 200, { ok: true, forum: buildForumPayload() });
    }

    serveStatic(req, res, pathname);
  } catch (error) {
    sendJson(res, 500, { error: String(error.message || error) });
  }
});

server.listen(PORT, HOST, () => {
  const url = `http://${HOST}:${PORT}`;
  pushLog("app", `server listening on ${url}`);
  startBridge();
  openBrowser(url);
  console.log(`MeshForum listening on ${url}`);
});

process.on("SIGINT", () => {
  if (bridgeProcess && !bridgeProcess.killed) {
    bridgeProcess.kill();
  }
  server.close(() => process.exit(0));
});
