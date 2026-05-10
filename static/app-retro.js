async function fetchJson(url, options) {
  const response = await fetch(url, options);
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || `HTTP ${response.status}`);
  }
  return payload;
}

let forumState = { topics: [] };
let selectedTopicId = null;
let syncPollTimer = null;
let inboundFadeTimer = null;

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function setFooterStatus(text) {
  const el = document.getElementById("footer-status");
  if (el) {
    el.textContent = text;
  }
}

function renderKv(target, entries) {
  target.innerHTML = entries.map(([key, value]) => {
    const rendered = value == null || value === "" ? "n/a" : String(value);
    return `<dt>${escapeHtml(key)}</dt><dd>${escapeHtml(rendered)}</dd>`;
  }).join("");
}

function nodeLabel(node) {
  return node.longName || node.shortName || node.userId || node.id || "unknown";
}

function formatDate(value) {
  if (!value) {
    return "n/a";
  }
  try {
    return new Date(value).toLocaleString();
  } catch {
    return String(value);
  }
}

function renderNodes(nodes, meshtastic) {
  const root = document.getElementById("nodes");
  const count = document.getElementById("node-count");
  count.textContent = `${nodes.length} node${nodes.length === 1 ? "" : "s"}`;

  if (!nodes.length) {
    root.className = "scroll-pane nodes empty";
    root.textContent = "No nodes discovered yet.";
    return;
  }

  root.className = "scroll-pane nodes";
  root.innerHTML = nodes.map((node) => {
    const targetId = node.userId || node.id || "";
    const isLocal = targetId && (
      targetId === String(meshtastic.localUserId || "") ||
      targetId === String(meshtastic.localNodeId || "")
    );
    const disabled = !meshtastic.connected || !targetId || isLocal ? "disabled" : "";
    return `
      <article class="node-card">
        <p><strong>${escapeHtml(nodeLabel(node))}</strong></p>
        <div class="node-meta">
          <div>User ID: <code>${escapeHtml(node.userId || "n/a")}</code></div>
          <div>Mesh num: <code>${escapeHtml(node.id || "n/a")}</code></div>
          <div>Last heard: ${escapeHtml(node.lastHeard || "n/a")}</div>
          <div>Hops away: ${escapeHtml(node.hopsAway ?? "n/a")}</div>
        </div>
        <div class="node-actions"><button type="button" data-target="${targetId}" ${disabled}>${isLocal ? "Local node" : "Hello"}</button></div>
      </article>
    `;
  }).join("");

  for (const button of root.querySelectorAll("button[data-target]")) {
    button.addEventListener("click", async () => {
      button.disabled = true;
      try {
        setFooterStatus(`HELLO ${button.dataset.target}`);
        await fetchJson("/api/bootstrap/connect", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ targetNodeId: button.dataset.target }),
        });
        await refresh();
      } catch (error) {
        alert(error.message);
        setFooterStatus(`ERROR | ${error.message}`);
      } finally {
        button.disabled = false;
      }
    });
  }
}

function renderLogs(logs) {
  const root = document.getElementById("logs");
  if (!logs.length) {
    root.className = "scroll-pane logs empty";
    root.textContent = "No events yet.";
    return;
  }

  root.className = "scroll-pane logs";
  root.innerHTML = logs.slice().reverse().map((entry) => `
    <article class="log-card">
      <p><strong>${escapeHtml(entry.kind)}</strong>: ${escapeHtml(entry.summary)}</p>
      <div class="log-meta">${escapeHtml(entry.ts)}</div>
    </article>
  `).join("");
}

function pickSelectedTopic() {
  if (!forumState.topics.length) {
    selectedTopicId = null;
    return null;
  }
  const existing = forumState.topics.find((topic) => topic.topicId === selectedTopicId);
  if (existing) {
    return existing;
  }
  selectedTopicId = forumState.topics[0].topicId;
  return forumState.topics[0];
}

function renderTopics(topics) {
  const root = document.getElementById("topics");
  const count = document.getElementById("topic-count");
  count.textContent = `${topics.length} topic${topics.length === 1 ? "" : "s"}`;

  if (!topics.length) {
    root.className = "scroll-pane topics empty";
    root.textContent = "No topics yet.";
    return;
  }

  root.className = "scroll-pane topics";
  root.innerHTML = topics.map((topic) => `
    <article class="topic-card ${topic.topicId === selectedTopicId ? "active" : ""}" data-topic-id="${topic.topicId}">
      <p><strong>${escapeHtml(topic.title)}</strong></p>
      <div class="topic-preview">${escapeHtml(topic.authorName || "unknown")} | ${topic.posts.length} posts | ${escapeHtml(formatDate(topic.bumpedAt || topic.createdAt))}</div>
      <div class="topic-preview">${escapeHtml(topic.bodyPreview || "")}</div>
    </article>
  `).join("");

  for (const card of root.querySelectorAll("[data-topic-id]")) {
    card.addEventListener("click", () => {
      selectedTopicId = card.dataset.topicId;
      renderForum();
    });
  }
}

function renderPosts(topic) {
  const title = document.getElementById("thread-title");
  const root = document.getElementById("posts");
  const replySubmit = document.getElementById("reply-submit");

  if (!topic) {
    title.textContent = "Select a topic";
    root.className = "scroll-pane posts empty";
    root.textContent = "No topic selected.";
    replySubmit.disabled = true;
    return;
  }

  title.textContent = topic.title;
  replySubmit.disabled = false;
  if (!topic.posts.length) {
    root.className = "scroll-pane posts empty";
    root.textContent = "No posts yet.";
    return;
  }

  root.className = "scroll-pane posts";
  root.innerHTML = topic.posts.map((post) => `
    <article class="post-card">
      <p>${escapeHtml(post.body).replace(/\n/g, "<br>")}</p>
      <div class="post-meta">${escapeHtml(post.authorName || "unknown")} | ${escapeHtml(formatDate(post.createdAt))}</div>
    </article>
  `).join("");
  requestAnimationFrame(() => {
    root.scrollTop = root.scrollHeight;
  });
}

// ── sync progress ──────────────────────────────────────────

function setSyncProgress(sync) {
  const wrap = document.getElementById("sync-progress");
  const fill = document.getElementById("sync-bar-fill");
  const counter = document.getElementById("sync-progress-counter");
  const label = document.getElementById("sync-progress-label");
  const inboundWrap = document.getElementById("inbound-progress");
  if (!wrap || !fill || !counter || !label) return;
  if (inboundWrap) inboundWrap.classList.add("hidden");

  const active = Boolean(sync?.active);
  const total = Number(sync?.total ?? 0);
  const completed = Number(sync?.completed ?? 0);
  const totalBytes = Number(sync?.totalBytes ?? 0);
  const completedBytes = Number(sync?.completedBytes ?? 0);
  const direction = String(sync?.direction || "idle");
  const finishedAt = sync?.finishedAt;
  const text = String(sync?.label || "");

  if (!active && !finishedAt && total === 0 && completed === 0) {
    wrap.classList.add("hidden");
    return;
  }

  const pct = total > 0 ? Math.round((completed / total) * 100) : (active ? 8 : 100);
  wrap.classList.remove("hidden");
  fill.style.width = `${pct}%`;
  counter.textContent = `${completed}/${total}`;

  if (text) {
    label.textContent = text;
  } else if (active) {
    label.textContent = direction === "receiving"
      ? (totalBytes > 0 ? `Receiving ${completedBytes}/${totalBytes}B` : "Receiving...")
      : (totalBytes > 0 ? `Sending ${completedBytes}/${totalBytes}B` : "Sending...");
  } else {
    label.textContent = finishedAt ? "Sync done" : "Idle";
  }
}

function stopSyncPolling() {
  clearInterval(syncPollTimer);
  syncPollTimer = null;
}

async function refreshSyncStatus() {
  const status = await fetchJson("/api/sync-status");
  if (status?.sync) {
    setSyncProgress(status.sync);
  } else {
    const outbound = status?.outbound || {};
    setSyncProgress({
      active: Boolean(outbound.active),
      total: Number(outbound.totalTransfers ?? outbound.total ?? 0),
      completed: Number(outbound.completedTransfers ?? outbound.sent ?? 0),
      totalBytes: Number(outbound.totalBytes ?? 0),
      completedBytes: Number(outbound.sentBytes ?? 0),
      direction: outbound.active ? "sending" : "idle",
      finishedAt: outbound.finishedAt || null,
      label: outbound.active ? "Sending updates" : (outbound.finishedAt ? "Sync done" : "Idle"),
    });
  }
  return status;
}

function startSyncPolling() {
  stopSyncPolling();
  setSyncProgress({ active: true, total: 1, completed: 0, direction: "sending", label: "Starting sync..." });
  syncPollTimer = setInterval(async () => {
    try {
      const status = await refreshSyncStatus();
      const sync = status.sync;
      const done = sync && !sync.active && sync.finishedAt;
      if (done) {
        stopSyncPolling();
        setTimeout(() => {
          setSyncProgress({ active: false, total: 0, completed: 0, finishedAt: null });
        }, 5000);
      }
    } catch {}
  }, 500);
}
function pollInboundOnce() {
  refreshSyncStatus().catch(() => {});
}

function renderForum() {
  const topic = pickSelectedTopic();
  renderTopics(forumState.topics || []);
  renderPosts(topic);
  if (topic) {
    setFooterStatus(`TOPIC | ${topic.title}`);
  }
}

async function refresh() {
  const [status, candidates, logs, forum] = await Promise.all([
    fetchJson("/api/status"),
    fetchJson("/api/bootstrap/candidates"),
    fetchJson("/api/logs"),
    fetchJson("/api/forum"),
  ]);

  // instance badge
  const badge = document.getElementById("instance-badge");
  if (badge) badge.textContent = status.app.instanceLabel || "NODE";

  // meshtastic status dot + info
  const dot = document.getElementById("mesh-status-dot");
  if (dot) {
    dot.className = status.meshtastic.connected ? "hero-status-dot connected" : "hero-status-dot disconnected";
    dot.textContent = status.meshtastic.connected ? "◉" : "◎";
  }
  const meshInfo = document.getElementById("hero-mesh-info");
  if (meshInfo) {
    const localName = status.meshtastic.localDisplayName || status.meshtastic.localUserId || "—";
    meshInfo.textContent = status.meshtastic.connected ? localName : "disconnected";
  }

  // sync peer target
  const bootstrapNode = document.getElementById("hero-bootstrap-node");
  if (bootstrapNode) {
    bootstrapNode.textContent = status.bootstrap.selectedNodeLabel || status.bootstrap.selectedNodeId || "none";
  }

  // profile input (don't override while user is typing)
  const nameInput = document.getElementById("display-name");
  if (nameInput && document.activeElement !== nameInput) {
    nameInput.value = status.identity.displayName || "";
  }
  forumState = forum;
  renderForum();
  renderNodes(candidates.nodes || [], status.meshtastic);
  renderLogs(logs.logs || []);
  document.title = `${status.app.instanceLabel} | BlackBox Board`;
  setFooterStatus(`READY | ${status.app.instanceLabel} | ${status.meshtastic.localUserId || "NO-MESH-ID"} | TOPICS ${status.forum.topicCount}`);
  pollInboundOnce();
}

document.getElementById("refresh-nodes").addEventListener("click", async () => {
  try {
    setFooterStatus("REFRESHING NODES");
    await fetchJson("/api/bootstrap/refresh", { method: "POST" });
    await refresh();
  } catch (error) {
    alert(error.message);
    setFooterStatus(`ERROR | ${error.message}`);
  }
});

document.getElementById("profile-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const displayName = document.getElementById("display-name").value.trim();
  const saveBtn = event.submitter || event.target.querySelector("button[type=submit]");
  const originalText = saveBtn ? saveBtn.textContent : "";
  try {
    setFooterStatus("SAVING PROFILE...");
    if (saveBtn) { saveBtn.disabled = true; saveBtn.textContent = "..."; }
    await fetchJson("/api/profile", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ displayName }),
    });
    if (saveBtn) { saveBtn.textContent = "SAVED"; }
    setFooterStatus(`PROFILE SAVED | ${displayName}`);
    await refresh();
  } catch (error) {
    setFooterStatus(`ERROR | ${error.message}`);
    alert(error.message);
  } finally {
    if (saveBtn) { saveBtn.disabled = false; saveBtn.textContent = originalText; }
  }
});

document.getElementById("new-topic-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const title = document.getElementById("new-topic-title").value.trim();
  const body = document.getElementById("new-topic-body").value.trim();
  try {
    setFooterStatus("CREATING TOPIC");
    const payload = await fetchJson("/api/topics", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ title, body }),
    });
    forumState = payload.forum;
    document.getElementById("new-topic-title").value = "";
    document.getElementById("new-topic-body").value = "";
    renderForum();
    await refresh();
  } catch (error) {
    alert(error.message);
    setFooterStatus(`ERROR | ${error.message}`);
  }
});

document.getElementById("reply-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!selectedTopicId) {
    return;
  }
  const body = document.getElementById("reply-body").value.trim();
  try {
    setFooterStatus("SENDING REPLY");
    const payload = await fetchJson(`/api/topics/${selectedTopicId}/posts`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ body }),
    });
    forumState = payload.forum;
    document.getElementById("reply-body").value = "";
    renderForum();
    await refresh();
  } catch (error) {
    alert(error.message);
    setFooterStatus(`ERROR | ${error.message}`);
  }
});

document.getElementById("sync-selected").addEventListener("click", async () => {
  const btn = document.getElementById("sync-selected");
  btn.disabled = true;
  try {
    setFooterStatus("SYNCING SELECTED PEER…");
    startSyncPolling();
    await fetchJson("/api/bootstrap/sync-selected", { method: "POST" });
    setFooterStatus("SYNC QUEUED — sending via radio…");
    await refresh();
  } catch (error) {
    clearInterval(syncPollTimer);
    setSyncProgress({ active: false, total: 0, completed: 0, label: "Sync failed" });
    alert(error.message);
    setFooterStatus(`ERROR | ${error.message}`);
  } finally {
    btn.disabled = false;
  }
});

document.getElementById("cmd-refresh").addEventListener("click", () => {
  document.getElementById("refresh-nodes").click();
});

document.getElementById("cmd-sync").addEventListener("click", () => {
  document.getElementById("sync-selected").click();
});

document.getElementById("cmd-save-profile").addEventListener("click", () => {
  document.getElementById("profile-form").requestSubmit();
});

document.getElementById("cmd-create-topic").addEventListener("click", () => {
  document.getElementById("new-topic-form").requestSubmit();
});

document.getElementById("cmd-reply").addEventListener("click", () => {
  document.getElementById("reply-form").requestSubmit();
});

refresh().catch((error) => {
  document.getElementById("logs").className = "scroll-pane logs empty";
  document.getElementById("logs").textContent = error.message;
  setFooterStatus(`ERROR | ${error.message}`);
});

setInterval(() => {
  refresh().catch(() => {});
}, 5000);
