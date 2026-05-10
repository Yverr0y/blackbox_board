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

function renderKv(target, entries) {
  target.innerHTML = entries.map(([key, value]) => {
    const rendered = value == null || value === "" ? "n/a" : String(value);
    return `<dt>${key}</dt><dd>${rendered}</dd>`;
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
    root.className = "nodes empty";
    root.textContent = "No nodes discovered yet.";
    return;
  }

  root.className = "nodes";
  root.innerHTML = nodes.map((node) => {
    const targetId = node.userId || node.id || "";
    const isLocal = targetId && (
      targetId === String(meshtastic.localUserId || "") ||
      targetId === String(meshtastic.localNodeId || "")
    );
    const disabled = !meshtastic.connected || !targetId || isLocal ? "disabled" : "";
    return `
      <article class="node-card">
        <p><strong>${nodeLabel(node)}</strong></p>
        <div class="node-meta">
          <div>User ID: <code>${node.userId || "n/a"}</code></div>
          <div>Mesh num: <code>${node.id || "n/a"}</code></div>
          <div>Last heard: ${node.lastHeard || "n/a"}</div>
          <div>Hops away: ${node.hopsAway ?? "n/a"}</div>
        </div>
        <div class="node-actions"><button type="button" data-target="${targetId}" ${disabled}>${isLocal ? "Local node" : "Hello"}</button></div>
      </article>
    `;
  }).join("");

  for (const button of root.querySelectorAll("button[data-target]")) {
    button.addEventListener("click", async () => {
      button.disabled = true;
      try {
        await fetchJson("/api/bootstrap/connect", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ targetNodeId: button.dataset.target }),
        });
        await refresh();
      } catch (error) {
        alert(error.message);
      } finally {
        button.disabled = false;
      }
    });
  }
}

function renderLogs(logs) {
  const root = document.getElementById("logs");
  if (!logs.length) {
    root.className = "logs empty";
    root.textContent = "No bridge events yet.";
    return;
  }

  root.className = "logs";
  root.innerHTML = logs.slice().reverse().map((entry) => `
    <article class="log-card">
      <p><strong>${entry.kind}</strong>: ${entry.summary}</p>
      <time>${entry.ts}</time>
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
    root.className = "topics empty";
    root.textContent = "No topics yet.";
    return;
  }

  root.className = "topics";
  root.innerHTML = topics.map((topic) => `
    <article class="topic-card ${topic.topicId === selectedTopicId ? "active" : ""}" data-topic-id="${topic.topicId}">
      <p><strong>${topic.title}</strong></p>
      <div class="topic-preview">${topic.authorName || "unknown"} · ${topic.posts.length} posts · ${formatDate(topic.bumpedAt || topic.createdAt)}</div>
      <div class="topic-preview">${topic.bodyPreview || ""}</div>
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
    root.className = "posts empty";
    root.textContent = "No topic selected.";
    replySubmit.disabled = true;
    return;
  }

  title.textContent = topic.title;
  replySubmit.disabled = false;
  if (!topic.posts.length) {
    root.className = "posts empty";
    root.textContent = "No posts yet.";
    return;
  }

  root.className = "posts";
  root.innerHTML = topic.posts.map((post) => `
    <article class="post-card">
      <p>${post.body}</p>
      <div class="post-meta">${post.authorName || "unknown"} · ${formatDate(post.createdAt)}</div>
    </article>
  `).join("");
}

function renderForum() {
  const topic = pickSelectedTopic();
  renderTopics(forumState.topics || []);
  renderPosts(topic);
}

async function refresh() {
  const [status, candidates, logs, forum] = await Promise.all([
    fetchJson("/api/status"),
    fetchJson("/api/bootstrap/candidates"),
    fetchJson("/api/logs"),
    fetchJson("/api/forum"),
  ]);

  renderKv(document.getElementById("app-status"), [
    ["Instance", status.app.instanceLabel],
    ["Version", status.app.version],
    ["Port", status.app.port],
    ["Started", formatDate(status.app.startedAt)],
    ["Data dir", status.app.dataDir],
  ]);

  renderKv(document.getElementById("identity-status"), [
    ["Author ID", status.identity.authorId],
    ["Display", status.identity.displayName],
    ["Created", formatDate(status.identity.createdAt)],
    ["Topics", status.forum.topicCount],
  ]);

  renderKv(document.getElementById("meshtastic-status"), [
    ["Connected", status.meshtastic.connected],
    ["Mode", status.meshtastic.mode],
    ["Local user ID", status.meshtastic.localUserId],
    ["Device port", status.meshtastic.port],
    ["Local name", status.meshtastic.localDisplayName],
    ["Error", status.meshtastic.error],
  ]);

  renderKv(document.getElementById("bootstrap-status"), [
    ["Selected node", status.bootstrap.selectedNodeLabel || status.bootstrap.selectedNodeId],
    ["Last hello", formatDate(status.bootstrap.lastHelloAt)],
    ["Ack", formatDate(status.bootstrap.lastHelloAckAt)],
    ["Message ID", status.bootstrap.lastHelloMessageId],
  ]);

  document.getElementById("display-name").value = status.identity.displayName || "";
  forumState = forum;
  renderForum();
  renderNodes(candidates.nodes || [], status.meshtastic);
  renderLogs(logs.logs || []);
}

document.getElementById("refresh-nodes").addEventListener("click", async () => {
  try {
    await fetchJson("/api/bootstrap/refresh", { method: "POST" });
    await refresh();
  } catch (error) {
    alert(error.message);
  }
});

document.getElementById("profile-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const displayName = document.getElementById("display-name").value.trim();
  try {
    await fetchJson("/api/profile", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ displayName }),
    });
    await refresh();
  } catch (error) {
    alert(error.message);
  }
});

document.getElementById("new-topic-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const title = document.getElementById("new-topic-title").value.trim();
  const body = document.getElementById("new-topic-body").value.trim();
  try {
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
  }
});

document.getElementById("reply-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!selectedTopicId) {
    return;
  }
  const body = document.getElementById("reply-body").value.trim();
  try {
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
  }
});

document.getElementById("sync-selected").addEventListener("click", async () => {
  try {
    await fetchJson("/api/bootstrap/sync-selected", { method: "POST" });
    await refresh();
  } catch (error) {
    alert(error.message);
  }
});

refresh().catch((error) => {
  document.getElementById("logs").className = "logs empty";
  document.getElementById("logs").textContent = error.message;
});

setInterval(() => {
  refresh().catch(() => {});
}, 5000);
