const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { Server } = require('socket.io');

const LOCAL_SEARCH_WINDOW_MS = 60_000;
const PARTNER_REMATCH_COOLDOWN_MS = 120_000;
const MESSAGE_RATE_WINDOW_MS = 3_000;
const MAX_MESSAGES_PER_WINDOW = 4;
const FIRST_MESSAGE_LINK_WINDOW_MS = 500;

function normalizeCountry(value) {
  return typeof value === 'string' ? value.trim().toUpperCase() : '';
}

function normalizeTags(value) {
  if (!Array.isArray(value)) {
    return [];
  }

  return [...new Set(value.filter((tag) => typeof tag === 'string')
    .map((tag) => tag.trim().toLowerCase())
    .filter(Boolean))];
}

function createHttpServer() {
  return http.createServer((request, response) => {
    const pathname = (request.url || '/').split('?')[0];
    if (request.method !== 'GET' || (pathname !== '/' && pathname !== '/index.html')) {
      response.writeHead(request.method === 'GET' ? 404 : 405, {
        Allow: 'GET',
        'Content-Type': 'text/plain; charset=utf-8',
      });
      response.end(request.method === 'GET' ? 'Not Found' : 'Method Not Allowed');
      return;
    }

    fs.readFile(path.join(__dirname, 'public', 'index.html'), (error, content) => {
      if (error) {
        response.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
        response.end('Unable to load the Geelom page.');
        return;
      }

      response.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      response.end(content);
    });
  });
}

function createChatServer({ server = createHttpServer(), io: existingIo } = {}) {
  const io = existingIo || new Server(server, {
    cors: {
      origin: process.env.CLIENT_ORIGIN || '*',
    },
  });
  const users = new Map();
  const waitingUsers = new Map();
  const partners = new Map();
  const recentPairs = new Map();
  const connectionStates = new Map();

  function getPairKey(firstId, secondId) {
    return JSON.stringify([firstId, secondId].sort());
  }

  function getRecentPairExpiry(firstId, secondId, now) {
    const key = getPairKey(firstId, secondId);
    const pairedAt = recentPairs.get(key);
    if (pairedAt === undefined) {
      return now;
    }

    const expiresAt = pairedAt + PARTNER_REMATCH_COOLDOWN_MS;
    if (expiresAt <= now) {
      recentPairs.delete(key);
      return now;
    }

    return expiresAt;
  }

  function removeFromQueue(socketId) {
    const user = waitingUsers.get(socketId);
    if (!user) {
      return;
    }

    clearTimeout(user.searchTimer);
    waitingUsers.delete(socketId);
  }

  function shareInterest(first, second) {
    const secondTags = new Set(second.tags);
    return first.tags.filter((tag) => secondTags.has(tag));
  }

  function isLocal(first, second) {
    return Boolean(first.country && first.country === second.country);
  }

  function findCandidate(user, now) {
    const candidates = [...waitingUsers.values()].filter((candidate) =>
      candidate.socketId !== user.socketId);
    const localCandidates = candidates.filter((candidate) =>
      isLocal(user, candidate)
      && getRecentPairExpiry(user.socketId, candidate.socketId, now) <= now);
    const localWithSharedTags = localCandidates.find((candidate) =>
      shareInterest(user, candidate).length > 0);

    if (localWithSharedTags) {
      return localWithSharedTags;
    }

    if (localCandidates.length > 0) {
      return localCandidates[0];
    }

    if (now - user.searchStartedAt < LOCAL_SEARCH_WINDOW_MS) {
      return null;
    }

    const internationalCandidates = candidates.filter((candidate) =>
      !isLocal(user, candidate)
      && now - candidate.searchStartedAt >= LOCAL_SEARCH_WINDOW_MS
      && getRecentPairExpiry(user.socketId, candidate.socketId, now) <= now);

    return internationalCandidates.find((candidate) =>
      shareInterest(user, candidate).length > 0) || internationalCandidates[0] || null;
  }

  function scheduleMatchRetry(user, now) {
    const futureEligibilityTimes = [...waitingUsers.values()]
      .filter((candidate) => candidate.socketId !== user.socketId)
      .map((candidate) => {
        const cooldownExpiry = getRecentPairExpiry(user.socketId, candidate.socketId, now);
        if (isLocal(user, candidate)) {
          return cooldownExpiry;
        }

        return Math.max(
          cooldownExpiry,
          user.searchStartedAt + LOCAL_SEARCH_WINDOW_MS,
          candidate.searchStartedAt + LOCAL_SEARCH_WINDOW_MS,
        );
      })
      .filter((eligibleAt) => eligibleAt > now);
    const nextEligibleAt = futureEligibilityTimes.length
      ? Math.min(...futureEligibilityTimes)
      : null;

    if (nextEligibleAt === null) {
      return;
    }

    user.searchTimer = setTimeout(() => tryMatch(user), nextEligibleAt - now);
  }

  function pairUsers(first, second) {
    removeFromQueue(first.socketId);
    removeFromQueue(second.socketId);
    partners.set(first.socketId, second.socketId);
    partners.set(second.socketId, first.socketId);
    const pairedAt = Date.now();
    for (const [key, timestamp] of recentPairs) {
      if (timestamp + PARTNER_REMATCH_COOLDOWN_MS <= pairedAt) {
        recentPairs.delete(key);
      }
    }
    recentPairs.set(getPairKey(first.socketId, second.socketId), pairedAt);
    const firstState = connectionStates.get(first.socketId);
    const secondState = connectionStates.get(second.socketId);
    if (firstState) {
      firstState.sentMessageInChat = false;
    }
    if (secondState) {
      secondState.sentMessageInChat = false;
    }

    const sharedTags = shareInterest(first, second);
    const firstSocket = io.sockets.sockets.get(first.socketId);
    const secondSocket = io.sockets.sockets.get(second.socketId);

    if (firstSocket) {
      firstSocket.emit('connected', {
        partnerId: second.socketId,
        partnerCountry: second.country,
        sharedTags,
      });
    }

    if (secondSocket) {
      secondSocket.emit('connected', {
        partnerId: first.socketId,
        partnerCountry: first.country,
        sharedTags,
      });
    }
  }

  function tryMatch(user) {
    if (!waitingUsers.has(user.socketId)) {
      return;
    }

    clearTimeout(user.searchTimer);
    user.searchTimer = null;
    const now = Date.now();
    const candidate = findCandidate(user, now);
    if (candidate) {
      pairUsers(user, candidate);
      return;
    }

    scheduleMatchRetry(user, now);
  }

  function enqueue(user) {
    removeFromQueue(user.socketId);
    user.searchStartedAt = Date.now();
    user.searchTimer = null;
    waitingUsers.set(user.socketId, user);
    tryMatch(user);
  }

  function detachPartner(socketId) {
    const partnerId = partners.get(socketId);
    if (!partnerId) {
      return;
    }

    partners.delete(socketId);
    partners.delete(partnerId);
    const partnerSocket = io.sockets.sockets.get(partnerId);
    if (partnerSocket) {
      partnerSocket.emit('partner-disconnected');
    }
  }

  io.on('connection', (socket) => {
    connectionStates.set(socket.id, {
      connectedAt: Date.now(),
      messageTimestamps: [],
      hasReceivedMessage: false,
      lastChatMessage: null,
      consecutiveChatRepeats: 0,
      sentMessageInChat: false,
    });

    socket.on('start-search', (payload = {}) => {
      detachPartner(socket.id);
      removeFromQueue(socket.id);

      const country = normalizeCountry(payload.country)
        || normalizeCountry(socket.handshake.auth && socket.handshake.auth.country)
        || normalizeCountry(socket.handshake.headers['cf-ipcountry'])
        || normalizeCountry(socket.handshake.headers['x-vercel-ip-country']);

      const user = {
        socketId: socket.id,
        country,
        tags: normalizeTags(payload.tags),
        searchStartedAt: Date.now(),
        searchTimer: null,
      };
      users.set(socket.id, user);
      enqueue(user);
    });

    socket.on('skip', () => {
      const user = users.get(socket.id);
      if (!user) {
        return;
      }

      detachPartner(socket.id);
      enqueue(user);
    });

    socket.on('typing', () => {
      const partnerId = partners.get(socket.id);
      const partnerSocket = partnerId && io.sockets.sockets.get(partnerId);
      if (partnerSocket) {
        partnerSocket.emit('partner-typing');
      }
    });

    socket.on('stop-typing', () => {
      const partnerId = partners.get(socket.id);
      const partnerSocket = partnerId && io.sockets.sockets.get(partnerId);
      if (partnerSocket) {
        partnerSocket.emit('partner-stop-typing');
      }
    });

    socket.on('chat-message', (payload) => {
      const message = typeof payload === 'string' ? payload : payload && payload.message;
      const state = connectionStates.get(socket.id);
      if (!state) {
        return;
      }

      const now = Date.now();
      state.messageTimestamps = state.messageTimestamps.filter((timestamp) =>
        now - timestamp < MESSAGE_RATE_WINDOW_MS);
      state.messageTimestamps.push(now);
      if (state.messageTimestamps.length > MAX_MESSAGES_PER_WINDOW) {
        socket.emit('warning', {
          message: 'You sent messages too quickly. You have been disconnected.',
        });
        socket.disconnect(true);
        return;
      }

      if (!state.hasReceivedMessage) {
        state.hasReceivedMessage = true;
        if (typeof message === 'string'
          && now - state.connectedAt < FIRST_MESSAGE_LINK_WINDOW_MS
          && /https?:\/\/|www\.|t\.me|\.com\b/i.test(message)) {
          socket.emit('warning', {
            message: 'Links are not allowed in your first message. You have been disconnected.',
          });
          socket.disconnect(true);
          return;
        }
      }

      const partnerId = partners.get(socket.id);
      if (!partnerId || typeof message !== 'string') {
        return;
      }

      if (!state.sentMessageInChat) {
        const normalizedMessage = message.trim().toLowerCase();
        if (normalizedMessage && normalizedMessage === state.lastChatMessage) {
          state.consecutiveChatRepeats += 1;
        } else {
          state.lastChatMessage = normalizedMessage;
          state.consecutiveChatRepeats = normalizedMessage ? 1 : 0;
        }
        state.sentMessageInChat = true;

        if (state.consecutiveChatRepeats >= 3) {
          socket.emit('warning', {
            message: 'The same message was sent across three chats. You have been disconnected.',
          });
          socket.disconnect(true);
          return;
        }
      }

      const partnerSocket = io.sockets.sockets.get(partnerId);
      if (partnerSocket) {
        partnerSocket.emit('chat-message', {
          from: socket.id,
          message,
        });
      }
    });

    socket.on('disconnect', () => {
      removeFromQueue(socket.id);
      detachPartner(socket.id);
      users.delete(socket.id);
      connectionStates.delete(socket.id);
    });
  });

  return { server, io, waitingUsers, users, partners, recentPairs };
}

if (require.main === module) {
  const port = Number(process.env.PORT) || 3000;
  const { server } = createChatServer();
  server.listen(port, () => {
    console.log(`Geelom chat server listening on port ${port}`);
  });
}

module.exports = { createChatServer };