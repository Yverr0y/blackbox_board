const { spawn } = require("child_process");
const path = require("path");

const ROOT = path.join(__dirname, "..");

// If MESH_PORT_A / MESH_PORT_B are set, use them directly.
// Otherwise auto-detect: Node-A gets index 0, Node-B gets index 1.
const MESH_PORT_A = process.env.MESH_PORT_A || "";
const MESH_PORT_B = process.env.MESH_PORT_B || "";

function startNode(port, dataDirName, label, meshPort, meshPortIndex, openBrowser) {
  const env = {
    ...process.env,
    PORT: String(port),
    DATA_DIR: path.join(ROOT, dataDirName),
    INSTANCE_LABEL: label,
    NO_OPEN_BROWSER: openBrowser ? "0" : "1",
  };

  if (meshPort) {
    env.MESHTASTIC_PORT = meshPort;
  } else {
    env.MESH_PORT_INDEX = String(meshPortIndex);
    delete env.MESHTASTIC_PORT;
  }

  const proc = spawn(process.execPath, [path.join(ROOT, "server.js")], {
    env,
    cwd: ROOT,
    stdio: "inherit",
  });

  const meshLabel = meshPort ? meshPort : `auto[${meshPortIndex}]`;
  console.log(`[${label}] starting on port ${port} → http://127.0.0.1:${port}  (mesh: ${meshLabel})`);

  proc.on("exit", (code) => {
    console.log(`[${label}] exited with code ${code}`);
  });

  return proc;
}

const nodeA = startNode(7861, "data-node-a", "Node-A", MESH_PORT_A, 0, true);

setTimeout(() => {
  const nodeB = startNode(7862, "data-node-b", "Node-B", MESH_PORT_B, 1, true);

  function shutdown() {
    console.log("\nShutting down both nodes...");
    if (!nodeA.killed) nodeA.kill();
    if (!nodeB.killed) nodeB.kill();
    setTimeout(() => process.exit(0), 400);
  }

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}, 600);

process.on("SIGINT", () => {});
process.on("SIGTERM", () => {});
