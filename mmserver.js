/**
 * Updated mediasoup server (Option A: multiple independent streams per room)
 *
 * Key changes:
 * - Support multiple independent streams in the same room, identified by agentId + targetUserId.
 * - Transports and producers carry appData.agentId and appData.targetUserId.
 * - stopAgentStreaming is selective (can stop by agentId and/or targetUserId) and is idempotent.
 * - room.activeStreams map tracks active streams per room for easier cleanup and bookkeeping.
 *
 * Drop this file in place of your previous server file.
 */

import express from "express";
import https from "https";
import fs from "fs";
import { Server } from "socket.io";
import mediasoup from "mediasoup";
import os from "os";
const app = express();

/* ---------------- BASIC ROUTES ---------------- */
app.get("/", (req, res) => res.send("✅ Mediasoup server alive"));
app.post("/status", (req, res) =>
  res.json({ status: "ok", uptime: process.uptime() })
);

/* ---------------- CONFIG ---------------- */
const PUBLIC_ANNOUNCED_IP = process.env.ANNOUNCED_IP || "74.225.130.88";
const SSL_KEY_PATH =
  process.env.SSL_KEY ||
  "/home/azureuser/mediasoup-server/ssl-certificates/privkey.pem";
const SSL_CERT_PATH =
  process.env.SSL_CERT ||
  "/home/azureuser/mediasoup-server/ssl-certificates/fullchain.pem";
const PRODUCER_RTP_WAIT_MS = parseInt(process.env.PRODUCER_RTP_WAIT_MS || "8000", 10);

const sslOptions = {
  key: fs.existsSync(SSL_KEY_PATH) ? fs.readFileSync(SSL_KEY_PATH) : undefined,
  cert: fs.existsSync(SSL_CERT_PATH)
    ? fs.readFileSync(SSL_CERT_PATH)
    : undefined,
};

let httpsServer;
if (sslOptions.key && sslOptions.cert) {
  httpsServer = https.createServer(sslOptions, app);
} else {
  httpsServer = https.createServer({}, app);
  console.warn(
    "⚠️ SSL certs not found; server running without TLS. Set SSL_KEY & SSL_CERT for production."
  );
}

const allowedOrigins = [
  "https://app.meramonitor.com",
  "https://dev.meramonitor.com",
  "https://qa.meramonitor.com",
  "https://client.actionview.ai",
  "https://tracker.vryno.com",
  "https://app.mymeramonitor.com",
  "http://localhost:3000",
];

const io = new Server(httpsServer, {
  cors: {
    origin: allowedOrigins,
    methods: ["GET", "POST", "OPTIONS", "PUT", "DELETE", "PATCH", "HEAD"],
    credentials: true,
  },
  pingInterval: 25000,
  pingTimeout: 60000,
});

io.engine.on("connection_error", (err) => {
  console.error("Engine connection_error:", err);
});

const PORT = parseInt(process.env.PORT || "3000", 10);
httpsServer.listen(PORT, () =>
  console.log(`✅ Server running at http://localhost:${PORT}`)
);

/* ---------------- MEDIASOUP WORKERS ---------------- */
const workers = [];
const mediaCodecs = [
  { kind: "audio", mimeType: "audio/opus", clockRate: 48000, channels: 2 },
  {
    kind: "video",
    mimeType: "video/H264",
    clockRate: 90000,
    parameters: { "packetization-mode": 1 },
  },
];

let workerIdx = 0;

async function createWorker() {
  const worker = await mediasoup.createWorker({
    rtcMinPort: 40000,
    rtcMaxPort: 50000,
  });

  worker._idx = workers.length;     // 🔴 identity
  worker._rooms = new Set();        // 🔴 tracking
  worker._createdAt = Date.now();

  worker.on("died", () => {
    console.error(`❌ Mediasoup worker ${worker._idx} died`);
    process.exit(1);
  });

  workers.push(worker);
  return worker;
}

async function getWorker() {
  if (!workers.length) {
    const count = Math.max(1, os.cpus().length - 1);
    for (let i = 0; i < count; i++) {
      await createWorker();
    }
    console.log(`🧠 Workers created: ${workers.length}`);
  }

  const w = workers[workerIdx];
  workerIdx = (workerIdx + 1) % workers.length;
  return w;
}


/* ---------------- MULTI-TENANT PRESENCE STORES ---------------- */
const agents = new Map();
const admins = new Map();
const onlineUsers = new Map();
const rooms = new Map(); // orgId -> Map(roomName -> room)

/* ---------------- HELPERS ---------------- */
function ensureMap(parent, key) {
  if (!parent.has(key)) parent.set(key, new Map());
  return parent.get(key);
}
function ensureSet(parent, key) {
  if (!parent.has(key)) parent.set(key, new Set());
  return parent.get(key);
}

function getOnlineAgents(orgId) {
  const orgAgents = agents.get(orgId);
  if (!orgAgents) return [];
  return Array.from(orgAgents, ([agentId, socketId]) => ({
    userId: agentId,
    socketId,
    agentId: agentId + "_" + Math.floor(Math.random() * 9) + 1
  }));
}

function broadcastOnlineAgents(orgId) {
  const list = getOnlineAgents(orgId);
  io.to(`org:${orgId}`).emit("online-agent-users", { orgId, users: list });
  console.log(`📡 Broadcast online agents org=${orgId}:`, list.length);
}

function logTotalUserCounts(context = "") {
  let totalAdmins = 0;
  let totalUsers = 0;

  for (const [, orgAdmins] of admins.entries()) {
    totalAdmins += orgAdmins.size;
  }

  for (const [, orgUsers] of onlineUsers.entries()) {
    for (const [, socketSet] of orgUsers.entries()) {
      totalUsers += orgUsers.size;
    }
  }

  console.log(
    `📊 CONNECTION STATS${context ? " [" + context + "]" : ""} → ` +
    `users=${totalUsers}, admins=${totalAdmins}`
  );
}
async function createRoom(orgId, roomName) {
  const orgRooms = ensureMap(rooms, orgId);
  if (orgRooms.has(roomName)) return orgRooms.get(roomName);

  const worker = await getWorker();
  const router = await worker.createRouter({ mediaCodecs });

  const room = {
    router,
    workerId: worker._idx,   // 🔴 permanent binding
    transports: new Map(),
    producers: new Map(),
    consumers: new Map(),
    activeStreams: new Map(),
    rtpTimeouts: new Map(),  // transportId -> timeoutId
  };

  worker._rooms.add(`${orgId}:${roomName}`);
  orgRooms.set(roomName, room);

  console.log(
    `🧠 Room created org=${orgId} room=${roomName} → worker=${worker._idx}`
  );

  return room;
}


/* ---------------- PORT RESOLUTION HELPERS ---------------- */
async function resolvePortFromTransport(t) {
  try {
    if (!t) return null;
    if (t.tuple && t.tuple.localPort) return t.tuple.localPort;
    // fallbacks - some mediasoup versions store socket references internally
    if (t._rtpSocket && typeof t._rtpSocket.address === "function") {
      const a = t._rtpSocket.address();
      if (a && a.port) return a.port;
    }
    if (t._data?.rtpSocket && typeof t._data.rtpSocket.address === "function") {
      const a = t._data.rtpSocket.address();
      if (a && a.port) return a.port;
    }
    if (
      t._data?.rtcpSocket &&
      typeof t._data.rtcpSocket.address === "function"
    ) {
      const a = t._data.rtcpSocket.address();
      if (a && a.port) return a.port;
    }
    if (t.listener && typeof t.listener.address === "function") {
      const a = t.listener.address();
      if (a && a.port) return a.port;
    }
  } catch (e) {
    console.warn("resolvePortFromTransport error:", e?.message ?? e);
  }
  return null;
}

async function logRoomProducers(room) {
  try {
    console.log(`\n==== Producers in room (count=${room.producers.size}) ====`);

    for (const [id, p] of room.producers.entries()) {
      console.log(`PRODUCER: ${id}`);
      console.log(`  kind: ${p.kind}`);
      console.log(`  closed: ${p.closed}`);
      console.log(`  appData: ${JSON.stringify(p.appData)}`);
      try {
        const stats = await p.getStats();
        console.log(`  stats: ${JSON.stringify(stats)}`);
      } catch (e) {
        console.log(`  stats: not available`);
      }
      console.log(
        `  rtpParameters: ${JSON.stringify(p.rtpParameters || {}, null, 2)}`
      );
    }
    console.log(`==== end producers ====`);

  } catch (e) {
    console.error("logRoomProducers error", e.message);
  }
}

/* ---------------------------------------------------------
   Helper to create producers only after transport tuple (RTP) is observed
   - Uses transport 'tuple' event (comedia true) to decide when remote endpoint bound
   - Also sets a timeout to fail gracefully if no RTP arrives in time
----------------------------------------------------------*/
async function createProducerWhenRtpArrives({ room, transport, kind, rtpOptions }) {
  if (!transport || !room) return null;
  const tId = transport.id;

  // If already produced for this kind and same appData (agent+target), skip
  if (rtpOptions?.appData) {
    const { agentId, targetUserId } = rtpOptions.appData;
    for (const p of room.producers.values()) {
      if (p.appData?.kind === kind && p.appData?.agentId === agentId && p.appData?.targetUserId === targetUserId) {
        console.log(`Producer for kind=${kind} already exists in room for agent=${agentId} target=${targetUserId}`);
        return null;
      }
    }
  }

  // Helper to clear timeout if present
  function clearRtpTimeout() {
    const existing = room.rtpTimeouts.get(tId);
    if (existing) {
      clearTimeout(existing);
      room.rtpTimeouts.delete(tId);
    }
  }

  // Helper to setup producer event listeners
  function attachProducerListeners(prod) {
    if (!prod) return;
    
    // Remove producer when transport closes
    prod.on("transportclose", () => {
      console.log(`Producer ${prod.id} transportclose - removing from room`);
      room.producers.delete(prod.id);
    });
    
    // Monitor producer state
    prod.on("score", (score) => {
      console.log(`Producer ${prod.id} score:`, score);
    });
  }

  // If tuple already known, create immediately
  if (transport.tuple || transport.tuple?.localPort) {
    clearRtpTimeout();
    try {
      const prod = await transport.produce(rtpOptions);
      room.producers.set(prod.id, prod);
      attachProducerListeners(prod);
      console.log(`✅ ${kind} producer created (immediate):`, prod.id);
      return prod;
    } catch (err) {
      console.error(`❌ immediate ${kind} produce failed:`, err?.message ?? err);
      return null;
    }
  }

  // otherwise wait for 'tuple' event
  return new Promise((resolve) => {
    let resolved = false;

    function doResolve(prod) {
      if (resolved) return;
      resolved = true;
      clearRtpTimeout();
      transport.removeListener("tuple", onTuple);
      resolve(prod);
    }

    async function onTuple() {
      console.log(`\n✅✅✅ Transport ${tId} TUPLE EVENT received for kind=${kind}`);
      console.log(`   RTP connection established from agent`);
      try {
        const prod = await transport.produce(rtpOptions);
        room.producers.set(prod.id, prod);
        attachProducerListeners(prod);
        console.log(`✅ ${kind} producer created after tuple:`, prod.id);
        doResolve(prod);
      } catch (err) {
        console.error(`❌ ${kind} produce failed after tuple:`, err?.message ?? err);
        doResolve(null);
      }
    }

    transport.on("tuple", onTuple);

    // Timeout fallback: if no tuple in time, cleanup and resolve null
    const timeoutId = setTimeout(() => {
      console.warn(`\n⏱️⏱️⏱️ NO RTP TUPLES for transport ${tId} (kind=${kind}) after ${PRODUCER_RTP_WAIT_MS}ms`);
      console.warn(`   This means the agent never sent RTP packets to the server`);
      console.warn(`   Check: Is FFmpeg running on the agent? Is it sending to ${PUBLIC_ANNOUNCED_IP}:${transport.tuple?.localPort || '?'}?`);
      transport.removeListener("tuple", onTuple);
      // best-effort close transport (caller may re-create later)
      try { transport.close(); } catch (e) { }
      room.transports.delete(tId);
      doResolve(null);
    }, PRODUCER_RTP_WAIT_MS);

    room.rtpTimeouts.set(tId, timeoutId);
  });
}

/* ---------------------------------------------------------
   Selective stop: close only transports/producers that match
   a given agentId and/or targetUserId.
   - args: orgId, roomName, roomParam, opts { agentId?, targetUserId? }
   - if neither agentId nor targetUserId provided, acts like full cleanup.
---------------------------------------------------------*/
async function stopAgentStreaming(orgId, roomName, roomParam, opts = {}) {
  try {
    const { agentId: optAgentId, targetUserId: optTargetUserId } = opts;
    const orgRooms = rooms.get(orgId);
    const room = roomParam || (orgRooms && orgRooms.get(roomName));
    if (!room) return;

    // Avoid re-entrancy
    if (room._stopping) return;
    room._stopping = true;

    // Try to notify the agent(s) if we have agent info in opts or in room active streams
    const notifiedAgents = new Set();
    if (optAgentId) {
      const orgAgents = agents.get(orgId);
      const socketId = orgAgents?.get(optAgentId);
      if (socketId) {
        try {
          console.log(`Emitting stop-ffmpeg to agent socket ${socketId} for room ${roomName} (agent=${optAgentId})`);
          io.to(socketId).emit("stop-ffmpeg", { orgId, roomName, targetUserId: optTargetUserId });
          notifiedAgents.add(optAgentId);
        } catch (e) {
          console.warn("stopAgentStreaming notify error", e?.message ?? e);
        }
      }
    } else if (optTargetUserId) {
      // try to notify any agents streaming to this target
      for (const [streamKey, meta] of room.activeStreams.entries()) {
        if (meta.targetUserId === optTargetUserId && meta.agentId) {
          const orgAgents = agents.get(orgId);
          const socketId = orgAgents?.get(meta.agentId);
          if (socketId && !notifiedAgents.has(meta.agentId)) {
            try {
              console.log(`Emitting stop-ffmpeg to agent socket ${socketId} for room ${roomName} (agent=${meta.agentId})`);
              io.to(socketId).emit("stop-ffmpeg", { orgId, roomName, targetUserId: optTargetUserId });
              notifiedAgents.add(meta.agentId);
            } catch (e) {
              console.warn("stopAgentStreaming notify error", e?.message ?? e);
            }
          }
        }
      }
    }

    // Helper to decide whether an item (transport/prod) should be closed
    const shouldClose = (appData = {}) => {
      // if opts specify agentId/targetUserId then both must match when present
      if (optAgentId && appData.agentId && appData.agentId !== optAgentId) return false;
      if (optTargetUserId && appData.targetUserId && appData.targetUserId !== optTargetUserId) return false;
      // If transport/prod has no agentId/targetUserId but opts are present,
      // be conservative and skip closing it so we don't unknowingly kill other streams.
      if ((optAgentId || optTargetUserId) && (!appData.agentId && !appData.targetUserId)) return false;
      return true;
    };

    // Close producers that match
    for (const [pid, prod] of Array.from(room.producers.entries())) {
      try {
        if (!prod || !prod.appData) continue;
        if (!optAgentId && !optTargetUserId) {
          // full cleanup path
          prod.close();
          room.producers.delete(pid);
          continue;
        }
        if (shouldClose(prod.appData)) {
          prod.close();
          room.producers.delete(pid);
          console.log(`Closed producer ${pid} (matched stop criteria).`);
        }
      } catch (e) {
        console.warn("Error closing producer", pid, e?.message ?? e);
      }
    }

    // Close transports that match. Also cleanup any rtpTimeouts for closed transports.
    for (const [tid, tr] of Array.from(room.transports.entries())) {
      try {
        const appData = tr.appData || {};
        if (!optAgentId && !optTargetUserId) {
          try { tr.close(); } catch (e) { }
          room.transports.delete(tid);
          const existing = room.rtpTimeouts.get(tid);
          if (existing) { clearTimeout(existing); room.rtpTimeouts.delete(tid); }
          continue;
        }
        if (shouldClose(appData)) {
          try { tr.close(); } catch (e) { }
          room.transports.delete(tid);
          const existing = room.rtpTimeouts.get(tid);
          if (existing) { clearTimeout(existing); room.rtpTimeouts.delete(tid); }
          console.log(`Closed transport ${tid} (matched stop criteria).`);
        }
      } catch (e) {
        console.warn("Error closing transport", tid, e?.message ?? e);
      }
    }

    // Clean room.activeStreams entries that match
    for (const [streamKey, meta] of Array.from(room.activeStreams.entries())) {
      if (optAgentId && meta.agentId && meta.agentId !== optAgentId) continue;
      if (optTargetUserId && meta.targetUserId && meta.targetUserId !== optTargetUserId) continue;
      // remove producers by id if present
      if (meta.videoProducerId && room.producers.has(meta.videoProducerId)) {
        try { const p = room.producers.get(meta.videoProducerId); p.close(); } catch (e) { }
        room.producers.delete(meta.videoProducerId);
      }
      if (meta.audioProducerId && room.producers.has(meta.audioProducerId)) {
        try { const p = room.producers.get(meta.audioProducerId); p.close(); } catch (e) { }
        room.producers.delete(meta.audioProducerId);
      }
      room.activeStreams.delete(streamKey);
      console.log(`Removed activeStreams entry ${streamKey}`);
    }

    // Reset room-level ephemeral state if full cleanup
    if (!optAgentId && !optTargetUserId) {
      delete room.currentAgentId;
      delete room.currentAgentSocket;
      delete room.currentTargetUserId;
      room.rtpReady = { video: false, audio: false };
    } else {
      // if we were stopping a specific stream and it matches the room's currentAgentId/target, clear them
      if (optAgentId && room.currentAgentId === optAgentId) delete room.currentAgentId;
      if (optTargetUserId && room.currentTargetUserId === optTargetUserId) delete room.currentTargetUserId;
      if (room.currentAgentSocket && optAgentId && room.currentAgentId === optAgentId) delete room.currentAgentSocket;
      // Do not wipe rtpReady entirely — other streams might still be active.
    }

    // Broadcast stopped stream event (include agent/target if known)
    io.to(`org:${orgId}:room:${roomName}`).emit("stream-stopped", {
      orgId,
      roomName,
      agentId: optAgentId || null,
      targetUserId: optTargetUserId || null,
    });

    room._stopping = false;
  } catch (err) {
    console.error("stopAgentStreaming error", err?.message ?? err);
    if (roomParam) roomParam._stopping = false;
  }
}

/* ============================================================
   SOCKET CONNECTION HANDLER
   - This implementation waits for RTP (tuple event) before producing.
   - Transports / producers are tagged with agentId & targetUserId.
============================================================ */
io.on("connection", (socket) => {
  // console.log("🔌 Socket connected:", socket.id);
  socket.on("connect_error", (err) => console.error("socket connect_error", err));
  socket.on("error", (err) => console.error("socket error", err));

  const { role, orgId, userId, agentId } = socket.handshake.query || {};
  if (!orgId) {
    console.warn("❌ Missing orgId. Disconnecting:", socket.id);
    return socket.disconnect(true);
  }
  socket.join(`org:${orgId}`);

  if (role === "admin") {
    const orgAdmins = ensureSet(admins, orgId);
    // 🔒 HARD DEDUPLICATION
    // if (orgAdmins.has(agentId)) {
    //   io.sockets.sockets.get(orgAdmins.get(agentId))?.disconnect(true);
    // }

    orgAdmins.add(socket.id);
    //console.log(`🛡️ Admin ONLINE: org=${orgId} socket=${socket.id}`);
    socket.emit("online-agent-users", { orgId, users: getOnlineAgents(orgId) });
    socket.on("disconnect", () => {
      orgAdmins.delete(socket.id);
      // console.log(`🛡️ Admin OFFLINE: org=${orgId} socket=${socket.id}`);
    });
    return;
  }

  if (role === "agent") {
    if (!agentId) {
      console.error(`❌ Agent connection rejected: missing agentId`);
      return socket.disconnect(true);
    }

    const orgAgents = ensureMap(agents, orgId);

    //🔒 HARD DEDUPLICATION
    if (orgAgents.has(agentId)) {
      io.sockets.sockets.get(orgAgents.get(agentId))?.disconnect(true);
    }
    orgAgents.set(agentId, socket.id);
    const orgUsers = ensureMap(onlineUsers, orgId);
    const socketSet = ensureSet(orgUsers, agentId);
    socketSet.add(socket.id);
    console.log(`🤖 Agent ONLINE: org=${orgId} agentId=${agentId} socket=${socket.id}`);
    console.log(`   Total agents in org: ${orgAgents.size}`);
    broadcastOnlineAgents(orgId);

    socket.on("disconnect", () => {
      if (orgAgents.get(agentId) === socket.id) {
        orgAgents.delete(agentId);
        console.log(`🤖 Agent OFFLINE: org=${orgId} agentId=${agentId}`);
        console.log(`   Remaining agents in org: ${orgAgents.size}`);
        broadcastOnlineAgents(orgId);
      }
    });

    logTotalUserCounts("on connection");
    return;
  }


  if (userId) {
    const orgUsers = ensureMap(onlineUsers, orgId);
    const socketSet = ensureSet(orgUsers, userId);
    socketSet.add(socket.id);
    console.log(`👤 User ONLINE: org=${orgId} userId=${userId} socket=${socket.id}`);
    io.to(`org:${orgId}`).emit("user-online", { orgId, userId });
    socket.on("user-disconnect", () => {
      const sockets = orgUsers.get(userId);
      if (sockets) {
        sockets.delete(socket.id);
        if (sockets.size === 0) {
          orgUsers.delete(userId);
          io.to(`org:${orgId}`).emit("user-offline", { orgId, userId });
          console.log(`👤 User OFFLINE: org=${orgId} userId=${userId}`);

          const orgRooms = rooms.get(orgId);
          if (orgRooms) {
            for (const [roomName, room] of orgRooms.entries()) {
              // Stop only streams targeting this user
              let anyStopped = false;
              for (const meta of room.activeStreams.values()) {
                if (meta.targetUserId === userId) { anyStopped = true; break; }
              }
              if (anyStopped) {
                console.log(`Stopping agent streaming for room ${roomName} because target user ${userId} went offline`);
                stopAgentStreaming(orgId, roomName, room, { targetUserId: userId });
              }
            }
          }
        }
      }
    });

  }
  // logTotalUserCounts("on connection");
  socket.on("ffmpeg-started", ({ ok, roomName, orgId: payloadOrg, targetUserId }) => {
    console.log(`✅ FFmpeg started ack - room=${roomName} org=${payloadOrg} target=${targetUserId}`);
    io.to(`org:${payloadOrg}:room:${roomName}`).emit("producer-ready", {
      roomName,
      orgId: payloadOrg,
      ready: true,
    });
  });

  socket.on("ffmpeg-error", ({ error, roomName }) => {
    console.error(`❌ FFmpeg error reported - room=${roomName} error=${error}`);
    io.to(`org:${orgId}:room:${roomName}`).emit("producer-error", {
      error,
      roomName,
    });
  });

  /* ---------------- request-start-stream ----------------
     UI -> server: create Plain transports and instruct agent to start ffmpeg.
     Create producers ONLY after tuple (RTP) arrives, using createProducerWhenRtpArrives.
  ----------------------------------------------------------*/
  /* ---------------- request-start-stream ----------------
     UI -> server: create Plain transports and instruct agent to start ffmpeg.
     Create producers ONLY after tuple (RTP) arrives, using createProducerWhenRtpArrives.
  ----------------------------------------------------------*/
  socket.on("request-start-stream", async ({ targetUserId, roomName, preferredAgentId, provider }, callback) => {
    console.log(`\n📡 request-start-stream RECEIVED: org=${orgId} room=${roomName} target=${targetUserId} provider=${provider}`);
    try {
      if (!targetUserId) return callback ? callback({ error: "targetUserId required" }) : null;
      if (!roomName) return callback ? callback({ error: "roomName required" }) : null;

      const orgUsers = onlineUsers.get(orgId);
      const targetSockets = orgUsers?.get(targetUserId);
      if (!targetSockets || targetSockets.size === 0) return callback ? callback({ error: "Target user offline" }) : null;

      const orgAgents = agents.get(orgId);
      console.log(`🔍 Checking for agents in org=${orgId}`);
      console.log(`   Agents registered: ${orgAgents?.size ?? 0}`);
      if (orgAgents?.size > 0) {
        for (const [aId, socketId] of orgAgents.entries()) {
          console.log(`   ✓ Agent available: agentId=${aId}`);
        }
      }
      
      if (!orgAgents || orgAgents.size === 0) {
        console.error(`❌ NO AGENTS AVAILABLE for org=${orgId}. Agent must connect with role='agent' in socket query.`);
        console.error(`   Check agent code: socket.io.connect(url, { query: { role: 'agent', orgId, agentId } })`);
        return callback ? callback({ error: "No available window agent for this org" }) : null;
      }

      let agentSocketId = null;
      let selectedAgentId = null;
      if (preferredAgentId && orgAgents.has(preferredAgentId)) {
        agentSocketId = orgAgents.get(preferredAgentId);
        selectedAgentId = preferredAgentId;
      } else {
        agentSocketId = Array.from(orgAgents.values())[0];
        selectedAgentId = Array.from(orgAgents.keys())[0];
      }
      if (!agentSocketId) return callback ? callback({ error: "No available window agent for this org" }) : null;

      let room = rooms.get(orgId)?.get(roomName);
      if (!room) room = await createRoom(orgId, roomName);
      const router = room.router;

      // CLEANUP old plain transports and producers for the SAME agent+target (not others)
      // (This ensures re-start attempts for same stream won't leave stale transports.)
      const existingKey = `${selectedAgentId}:${targetUserId}:${roomName}`;
      if (room.activeStreams.has(existingKey)) {
        const meta = room.activeStreams.get(existingKey);
        console.log(`Existing active stream for ${existingKey} found - cleaning its transports/producers before re-create`);
        // stop only that existing stream
        stopAgentStreaming(orgId, roomName, room, { agentId: selectedAgentId, targetUserId });
      }

      console.log("\n🔧 Creating PlainRtpTransports with comedia=true...");

      // CREATE plain transports with comedia enabled and include agentId & targetUserId in appData
      const videoTransport = await router.createPlainTransport({
        listenIp: { ip: "0.0.0.0", announcedIp: PUBLIC_ANNOUNCED_IP },
        rtcpMux: true,
        comedia: true, // auto-detect remote endpoint
        enableSrtp: false,
        enableSctp: false,
        appData: {
          type: "plain-video",
          orgId,
          roomName,
          agentId: selectedAgentId,
          targetUserId,
        },
      });

      const audioTransport = await router.createPlainTransport({
        listenIp: { ip: "0.0.0.0", announcedIp: PUBLIC_ANNOUNCED_IP },
        rtcpMux: true,
        comedia: true,
        enableSrtp: false,
        enableSctp: false,
        appData: {
          type: "plain-audio",
          orgId,
          roomName,
          agentId: selectedAgentId,
          targetUserId,
        },
      });

      // Store transports
      room.transports.set(videoTransport.id, videoTransport);
      room.transports.set(audioTransport.id, audioTransport);

      // Cleanup listeners when transports close
      videoTransport.on("close", () => {
        console.log(`Video plain transport closed - cleaning up`);
        room.transports.delete(videoTransport.id);
      });
      
      audioTransport.on("close", () => {
        console.log(`Audio plain transport closed - cleaning up`);
        room.transports.delete(audioTransport.id);
      });

      // Add trace listeners for debugging (non-blocking)
      videoTransport.on("trace", (trace) => {
        if (trace.type === "rtp" || trace.type === "tuple") {
          console.log(`[VIDEO TRACE] ${trace.type}:`, JSON.stringify(trace));
        }
      });
      audioTransport.on("trace", (trace) => {
        if (trace.type === "rtp" || trace.type === "tuple") {
          console.log(`[AUDIO TRACE] ${trace.type}:`, JSON.stringify(trace));
        }
      });

      // best-effort port resolution (sync or async handling)
      let videoPort = await resolvePortFromTransport(videoTransport) || videoTransport.tuple?.localPort || null;
      let audioPort = await resolvePortFromTransport(audioTransport) || audioTransport.tuple?.localPort || null;

      console.log("\n✅ PlainTransports created (no RTP yet):");
      console.log(`   Video listening on: ${PUBLIC_ANNOUNCED_IP}:${videoPort}`);
      console.log(`   Audio listening on: ${PUBLIC_ANNOUNCED_IP}:${audioPort}`);
      console.log("   Waiting for RTP (tuple) before creating producers...\n");

      // Track room state (not authoritative, but helpful)
      room.currentAgentId = selectedAgentId;
      room.currentAgentSocket = agentSocketId;
      room.currentTargetUserId = targetUserId;

      // Build unique SSRCs for this stream (avoid collisions)
      const base = Date.now() % 100000;
      const videoSsrc = base + Math.floor(Math.random() * 1000) + 1000;
      const audioSsrc = base + Math.floor(Math.random() * 1000) + 2000;

      // BUILD payload for agent (send ports obtained above)
      const agentPayload = {
        orgId,
        roomName,
        targetUserId,
        agentId: selectedAgentId,
        provider: provider || "default",
        video: {
          rtpPort: videoPort,
          announcedIp: PUBLIC_ANNOUNCED_IP,
          payloadType: 102,
          ssrc: videoSsrc,
        },
        audio: {
          rtpPort: audioPort,
          announcedIp: PUBLIC_ANNOUNCED_IP,
          payloadType: 100,
          ssrc: audioSsrc,
        },
      };

      console.log(`\n📤 DEBUG: About to send start-ffmpeg`);
      console.log(`   agentSocketId="${agentSocketId}"`);
      console.log(`   selectedAgentId="${selectedAgentId}"`);
      console.log(`   videoPort=${videoPort}, audioPort=${audioPort}`);
      
      console.log("📤 Sending start-ffmpeg to agent:", agentPayload);
      const emitResult = io.to(agentSocketId).emit("start-ffmpeg", agentPayload);
      console.log(`📤 emit result: ${emitResult}`);

      // Notify target user
      for (const userSocketId of targetSockets) {
        io.to(userSocketId).emit("stream-starting", {
          orgId,
          roomName,
          agentId: selectedAgentId,
          message: "Waiting for video stream...",
        });
      }

      // CREATE PRODUCERS WHEN RTP ARRIVES (video + audio)
      const videoRtpOptions = {
        kind: "video",
        rtpParameters: {
          codecs: [
            {
              mimeType: "video/H264",
              clockRate: 90000,
              payloadType: 102,
              parameters: {
                "packetization-mode": 1,
                "profile-level-id": "42e01f",
              },
            },
          ],
          encodings: [{ ssrc: videoSsrc }],
          rtcp: { cname: `video-${Date.now()}` },
        },
        appData: { kind: "video", roomName, orgId, agentId: selectedAgentId, targetUserId },
      };

      const audioRtpOptions = {
        kind: "audio",
        rtpParameters: {
          codecs: [
            {
              mimeType: "audio/opus",
              clockRate: 48000,
              channels: 2,
              payloadType: 100,
            },
          ],
          encodings: [{ ssrc: audioSsrc }],
          rtcp: { cname: `audio-${Date.now()}` },
        },
        appData: { kind: "audio", roomName, orgId, agentId: selectedAgentId, targetUserId },
      };

      // Asynchronously wait for RTP; if fails, inform clients
      const videoProducerPromise = createProducerWhenRtpArrives({
        room,
        transport: videoTransport,
        kind: "video",
        rtpOptions: videoRtpOptions,
      }).then((videoProducer) => {
        if (!videoProducer) {
          console.warn("No video producer created (RTP not received)");
          io.to(`org:${orgId}:room:${roomName}`).emit("producer-error", { roomName, error: "video RTP not received" });
          return null;
        }
        return videoProducer;
      });

      const audioProducerPromise = createProducerWhenRtpArrives({
        room,
        transport: audioTransport,
        kind: "audio",
        rtpOptions: audioRtpOptions,
      }).then((audioProducer) => {
        if (!audioProducer) {
          console.warn("No audio producer created (RTP not received)");
          return null;
        }
        return audioProducer;
      });

      // Once producers created (or timed out), notify clients
      const [videoProducer, audioProducer] = await Promise.all([videoProducerPromise, audioProducerPromise]);

      // If producers created, ensure appData is present and add to room.activeStreams
      const streamKey = `${selectedAgentId}:${targetUserId}:${roomName}`;
      const meta = {
        agentId: selectedAgentId,
        targetUserId,
        roomName,
        videoProducerId: videoProducer?.id || null,
        audioProducerId: audioProducer?.id || null,
        createdAt: Date.now(),
      };

      if (videoProducer) {
        videoProducer.appData = Object.assign(videoProducer.appData || {}, { agentId: selectedAgentId, targetUserId, roomName, kind: "video" });
        // Producer already added in createProducerWhenRtpArrives, just update appData
      }
      if (audioProducer) {
        audioProducer.appData = Object.assign(audioProducer.appData || {}, { agentId: selectedAgentId, targetUserId, roomName, kind: "audio" });
        // Producer already added in createProducerWhenRtpArrives, just update appData
      }

      // store meta only if at least one producer exists
      if (meta.videoProducerId || meta.audioProducerId) {
        room.activeStreams.set(streamKey, meta);
        console.log(`Registered activeStream ${streamKey}`, meta);
      }

      // Log producers
      (async () => { try { await logRoomProducers(room); } catch (e) { } })();

      console.log("\n✅ STREAM SETUP COMPLETED (producers created if RTP arrived)");

      io.to(`org:${orgId}:room:${roomName}`).emit("newProducer", {
        videoProducerId: videoProducer?.id || null,
        audioProducerId: audioProducer?.id || null,
        kind: "video",
        agentId: selectedAgentId,
        provider: provider || "default",
        targetUserId,
      });

      for (const userSocketId of targetSockets) {
        io.to(userSocketId).emit("stream-ready", {
          orgId,
          roomName,
          videoProducerId: videoProducer?.id || null,
          audioProducerId: audioProducer?.id || null,
        });
      }

      if (callback) {
        callback({
          ok: true,
          orgId,
          roomName,
          videoProducerId: videoProducer?.id || null,
          audioProducerId: audioProducer?.id || null,
          agentId: selectedAgentId,
          targetUserId,
        });
      }
    } catch (err) {
      console.error("\n❌ request-start-stream ERROR:", err);
      if (callback) callback({ error: err?.message ?? String(err) });
    }
  });

  /* ------------------ request-stop-stream (from UI) ------------------ */
  socket.on("request-stop-stream", ({ targetUserId, roomName, agentId: requestedAgentId }, cb) => {
    try {
      console.log("📨 request-stop-stream:", { orgId, targetUserId, roomName, requestedAgentId });
      const orgRooms = rooms.get(orgId);
      const room = orgRooms?.get(roomName);

      // If agent socket known for given agentId, emit stop-ffmpeg to that agent
      let agentSocketId = null;
      if (requestedAgentId) {
        agentSocketId = agents.get(orgId)?.get(requestedAgentId);
      } else if (room?.currentAgentId) {
        agentSocketId = room.currentAgentSocket || agents.get(orgId)?.get(room.currentAgentId);
      }

      if (agentSocketId) {
        io.to(agentSocketId).emit("stop-ffmpeg", { orgId, roomName, targetUserId });
      } else {
        console.log("No agent socket found - trying to stop locally by closing transports for target:", targetUserId, "agent:", requestedAgentId);
        // Prefer stopping by both agentId and targetUserId if agentId provided
        if (requestedAgentId) {
          stopAgentStreaming(orgId, roomName, room, { agentId: requestedAgentId, targetUserId });
        } else {
          stopAgentStreaming(orgId, roomName, room, { targetUserId });
        }
      }
      if (cb) cb({ ok: true });
    } catch (e) {
      console.error("request-stop-stream error:", e);
      if (cb) cb({ error: e.message });
    }
  });

  /* ------------------- mediasoup: join room ------------------- */
  socket.on("joinRoom", async ({ roomName, userId }, callback) => {
    try {
      if (!roomName) return callback({ error: "roomName required" });
      let room = rooms.get(orgId)?.get(roomName);
      if (!room) room = await createRoom(orgId, roomName);
      socket.join(`org:${orgId}:room:${roomName}`);
      callback({ rtpCapabilities: room.router.rtpCapabilities });
    } catch (err) {
      console.error("joinRoom error:", err);
      callback({ error: err.message });
    }
  });

  /* ------------------- create consumer transport ------------------- */
  socket.on("createConsumerTransport", async ({ roomName }, callback) => {
    try {
      const room = rooms.get(orgId)?.get(roomName);
      if (!room) return callback({ error: "Room not found" });
      const transport = await room.router.createWebRtcTransport({
        listenIps: [{ ip: "0.0.0.0", announcedIp: PUBLIC_ANNOUNCED_IP }],
        enableUdp: true,
        enableTcp: true,
        preferUdp: true,
        appData: {
          type: "consumer",
          socketId: socket.id,
          connected: false,
          orgId,
          roomName,
        },
      });
      room.transports.set(transport.id, transport);
      callback({
        id: transport.id,
        iceParameters: transport.iceParameters,
        iceCandidates: transport.iceCandidates,
        dtlsParameters: transport.dtlsParameters,
      });
    } catch (err) {
      console.error("createConsumerTransport error:", err);
      callback({ error: err.message });
    }
  });

  /* ------------------- connect consumer transport ------------------- */
  socket.on("connectConsumerTransport", async ({ dtlsParameters, roomName }, callback) => {
    try {
      const room = rooms.get(orgId)?.get(roomName);
      if (!room) return callback({ error: "Room not found" });
      const transport = Array.from(room.transports.values()).find(
        (t) => t.appData?.type === "consumer" && t.appData?.socketId === socket.id
      );
      if (!transport) return callback({ error: "Transport not found" });
      if (!transport.appData.connected) {
        transport.appData.connected = true;
        await transport.connect({ dtlsParameters });
      }
      callback();
    } catch (err) {
      console.error("connectConsumerTransport error:", err);
      callback({ error: err.message });
    }
  });

  /* ------------------- consume producers ------------------- */
  socket.on("consume", async ({ roomName, rtpCapabilities }, callback) => {
    try {
      const room = rooms.get(orgId)?.get(roomName);
      if (!room) return callback({ error: "Room not found" });

      const router = room.router;

      const consumerTransport = Array.from(room.transports.values()).find(
        (t) => t.appData?.type === "consumer" && t.appData?.socketId === socket.id
      );

      if (!consumerTransport) return callback({ error: "Consumer transport not found" });

      const produced = Array.from(room.producers.values());
      console.log(`🎬 CONSUME: Found ${produced.length} producers in room "${roomName}"`);
      
      if (!produced.length) {
        console.warn(`⚠️  No producers available! Room has ${room.producers.size} producers, but Array.from() returned 0`);
        console.log("Producer IDs:", Array.from(room.producers.keys()));
        return callback({ consumers: [] });
      }

      const consumersInfo = [];

      for (const producer of produced) {
        const can = router.canConsume({
          producerId: producer.id,
          rtpCapabilities,
        });
        console.log(`📺 Checking producer ${producer.id} (kind=${producer.kind}): canConsume=${can}`);
        if (!can) {
          console.log(`   ❌ Cannot consume producer ${producer.id}`);
          continue;
        }

        try {
          const consumer = await consumerTransport.consume({
            producerId: producer.id,
            rtpCapabilities,
            paused: true,
          });

          room.consumers.set(consumer.id, consumer);
          console.log(`✅ Created consumer ${consumer.id} from producer ${producer.id}`);

          try { await consumer.resume(); } catch (e) { 
            console.error(`Failed to resume consumer ${consumer.id}:`, e.message);
          }

          consumer.on("transportclose", () => {
            console.log(`Consumer ${consumer.id} transport closed`);
            room.consumers.delete(consumer.id);
          });
          consumer.on("producerclose", () => {
            console.log(`Consumer ${consumer.id} producer closed`);
            try { consumer.close(); } catch { }
            room.consumers.delete(consumer.id);
          });

          consumersInfo.push({
            id: consumer.id,
            producerId: consumer.producerId,
            kind: consumer.kind,
            rtpParameters: consumer.rtpParameters,
          });
        } catch (err) {
          console.error(`Failed to consume producer ${producer.id}:`, err.message);
        }
      }

      console.log(`📦 Returning ${consumersInfo.length} consumers to client`);
      callback({ consumers: consumersInfo });
    } catch (err) {
      console.error("consume error:", err);
      callback({ error: err.message });
    }
  });

  /* =============== DISCONNECT HANDLER =============== */
  socket.on("disconnect", () => {
    //console.log(`\n🔴 Socket disconnected: ${socket.id}`);
    const orgUsers = onlineUsers.get(orgId);
    if (orgUsers?.has(userId)) {
      const sockets = orgUsers.get(userId);
      sockets.delete(socket.id);
      if (sockets.size === 0) {
        orgUsers.delete(userId);
        io.to(`org:${orgId}`).emit("user-offline", { orgId, userId });
        console.log(`👤 User OFFLINE: org=${orgId} userId=${userId}`);

        const orgRooms = rooms.get(orgId);
        if (orgRooms) {
          for (const [roomName, room] of orgRooms.entries()) {
            let anyTarget = false;
            for (const meta of room.activeStreams.values()) {
              if (meta.targetUserId === userId) { anyTarget = true; break; }
            }
            if (anyTarget) {
              console.log(`Stopping agent streaming for room ${roomName} because target user ${userId} went offline`);
              stopAgentStreaming(orgId, roomName, room, { targetUserId: userId });
            }
          }
        }
      }
    }

    const orgAgents = agents.get(orgId);
    if (orgAgents) {
      for (const [agentId, socketId] of orgAgents.entries()) {
        if (socketId === socket.id) {
          //console.log(`Agent socket disconnected for agentId=${agentId} org=${orgId}`);
          orgAgents.delete(agentId);

          const orgRooms = rooms.get(orgId);
          if (orgRooms) {
            for (const [roomName, room] of orgRooms.entries()) {
              let anyAgentStream = false;
              for (const meta of room.activeStreams.values()) {
                if (meta.agentId === agentId) { anyAgentStream = true; break; }
              }
              if (anyAgentStream) {
                //console.log(`Stopping streams in room ${roomName} because agent ${agentId} disconnected`);
                stopAgentStreaming(orgId, roomName, room, { agentId });
              }
            }
          }
        }
      }
    }

    const orgRooms = rooms.get(orgId);
    if (!orgRooms) return;
    for (const room of orgRooms.values()) {
      for (const [tid, t] of Array.from(room.transports.entries())) {
        if (t.appData?.socketId === socket.id) {
          try {
            t.close();
          } catch { }
          room.transports.delete(tid);
        }
      }
    }
  });
});

/* ========================================================
   REST API: CREATE PRODUCER (utility - useful for debugging)
   This endpoint creates plain transports but DOES NOT produce
   - used to inspect ports used for RTP
======================================================== */
app.get("/create-producer/:orgId/:roomName", async (req, res) => {
  try {
    const { orgId, roomName } = req.params;
    let room = rooms.get(orgId)?.get(roomName);
    if (!room) room = await createRoom(orgId, roomName);
    const router = room.router;

    // cleanup existing plain transports for room
    for (const [tid, t] of Array.from(room.transports.entries())) {
      if (t.appData?.type && t.appData.type.startsWith("plain-")) {
        try { t.close(); } catch (e) { }
        room.transports.delete(tid);
      }
    }

    const videoTransport = await router.createPlainTransport({
      listenIp: { ip: "0.0.0.0", announcedIp: PUBLIC_ANNOUNCED_IP },
      rtcpMux: true,
      comedia: true,
      appData: { type: "plain-video" },
    });

    const audioTransport = await router.createPlainTransport({
      listenIp: { ip: "0.0.0.0", announcedIp: PUBLIC_ANNOUNCED_IP },
      rtcpMux: true,
      comedia: true,
      appData: { type: "plain-audio" },
    });

    room.transports.set(videoTransport.id, videoTransport);
    room.transports.set(audioTransport.id, audioTransport);

    // best-effort port resolution
    const vport = await resolvePortFromTransport(videoTransport) || videoTransport.tuple?.localPort || null;
    const aport = await resolvePortFromTransport(audioTransport) || audioTransport.tuple?.localPort || null;

    res.json({ videoRtpPort: vport, audioRtpPort: aport, rtcpMux: true });
  } catch (err) {
    console.error("create-producer error:", err);
    res.status(500).json({ error: err.message });
  }
});

export default app;
