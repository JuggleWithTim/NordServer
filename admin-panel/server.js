const express = require('express');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const Database = require('better-sqlite3');

function loadDotEnvFile(envPath) {
  if (!fs.existsSync(envPath)) return;

  const content = fs.readFileSync(envPath, 'utf8');
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;

    const separator = line.indexOf('=');
    if (separator <= 0) continue;

    const key = line.slice(0, separator).trim();
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) continue;

    let value = line.slice(separator + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }

    if (process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}

loadDotEnvFile(path.join(__dirname, '.env'));

const PORT = Number(process.env.PORT || 8091);
const DB_PATH = process.env.NORD_DB_PATH || path.resolve(__dirname, '..', 'nord-data.sqlite');
const BASIC_AUTH_USERNAME = process.env.BASIC_AUTH_USERNAME;
const BASIC_AUTH_PASSWORD = process.env.BASIC_AUTH_PASSWORD;

if (!BASIC_AUTH_USERNAME || !BASIC_AUTH_PASSWORD) {
  throw new Error('BASIC_AUTH_USERNAME and BASIC_AUTH_PASSWORD must be set (for example in .env)');
}

const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');
db.exec(`
  CREATE TABLE IF NOT EXISTS chat_command_permissions (
    command_name TEXT PRIMARY KEY,
    required_role TEXT NOT NULL DEFAULT 'mod',
    updated_at INTEGER NOT NULL DEFAULT 0
  );
  CREATE TABLE IF NOT EXISTS admin_popup_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sender_name TEXT NOT NULL DEFAULT 'update',
    message_text TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL DEFAULT 0,
    delivered_at INTEGER NOT NULL DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS idx_admin_popup_queue_pending
    ON admin_popup_queue(delivered_at, id);
`);

const app = express();

function timingSafeEqualString(a, b) {
  const aBuf = Buffer.from(a);
  const bBuf = Buffer.from(b);
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

function sendAuthChallenge(res) {
  res.set('WWW-Authenticate', 'Basic realm="Nord Admin Panel"');
  return res.status(401).send('Authentication required');
}

function basicAuthMiddleware(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Basic ')) {
    return sendAuthChallenge(res);
  }

  let decoded;
  try {
    decoded = Buffer.from(authHeader.slice(6), 'base64').toString('utf8');
  } catch (_err) {
    return sendAuthChallenge(res);
  }

  const separatorIndex = decoded.indexOf(':');
  if (separatorIndex < 0) {
    return sendAuthChallenge(res);
  }

  const username = decoded.slice(0, separatorIndex);
  const password = decoded.slice(separatorIndex + 1);

  if (
    !timingSafeEqualString(username, BASIC_AUTH_USERNAME) ||
    !timingSafeEqualString(password, BASIC_AUTH_PASSWORD)
  ) {
    return sendAuthChallenge(res);
  }

  return next();
}

app.use(basicAuthMiddleware);
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const ROLE_BADGES = {
  vip: { category: 6, type: 0 },
  admin: { category: 6, type: 1 },
  moderator: { category: 6, type: 2 },
};
const CHAT_COMMANDS = ['setcoins', 'setlevel'];
const CHAT_COMMAND_PERMISSION_VALUES = new Set(['everyone', 'vip', 'mod']);
const POPUP_MAX_TEXT_LENGTH = 500;

const upsertChatCommandPermissionStmt = db.prepare(`
  INSERT INTO chat_command_permissions (command_name, required_role, updated_at)
  VALUES (?, ?, ?)
  ON CONFLICT(command_name)
  DO UPDATE SET required_role = excluded.required_role, updated_at = excluded.updated_at
`);

for (const commandName of CHAT_COMMANDS) {
  db.prepare(`
    INSERT OR IGNORE INTO chat_command_permissions (command_name, required_role, updated_at)
    VALUES (?, 'mod', 0)
  `).run(commandName);
}
db.prepare(`
  UPDATE chat_command_permissions
  SET required_role = 'mod'
  WHERE lower(required_role) NOT IN ('everyone', 'vip', 'mod')
`).run();

function parseIntStrict(value, fieldName) {
  if (typeof value === 'string' && value.trim() !== '') value = Number(value);
  if (!Number.isInteger(value)) {
    const err = new Error(`${fieldName} must be an integer`);
    err.status = 400;
    throw err;
  }
  return value;
}

function normalizeCommandName(commandName) {
  let normalized = String(commandName || '').trim().toLowerCase();
  if (normalized.startsWith('/')) normalized = normalized.slice(1);
  return normalized;
}

function normalizeChatCommandPermission(requiredRole) {
  const normalized = String(requiredRole || '').trim().toLowerCase();
  if (!CHAT_COMMAND_PERMISSION_VALUES.has(normalized)) {
    const err = new Error("requiredRole must be one of: everyone, vip, mod");
    err.status = 400;
    throw err;
  }
  return normalized;
}

function listChatCommandPermissions() {
  const rows = db.prepare(`
    SELECT command_name, required_role
    FROM chat_command_permissions
    WHERE command_name IN (${CHAT_COMMANDS.map(() => '?').join(', ')})
  `).all(...CHAT_COMMANDS);
  const byCommand = new Map(rows.map((row) => [normalizeCommandName(row.command_name), row]));

  return CHAT_COMMANDS.map((commandName) => {
    const row = byCommand.get(commandName);
    const requiredRole = row ? String(row.required_role || '').trim().toLowerCase() : 'mod';
    return {
      command: commandName,
      requiredRole: CHAT_COMMAND_PERMISSION_VALUES.has(requiredRole) ? requiredRole : 'mod',
    };
  });
}

function getPlayerById(playerId) {
  const row = db.prepare(`
    SELECT
      u.user_id,
      u.username,
      u.village_id,
      u.village_name,
      u.level,
      u.heightmap_preset_id,
      COALESCE((
        SELECT vh.type
        FROM village_heightmaps vh
        WHERE vh.village_id = u.village_id
      ), 0) AS village_theme,
      u.slx_credits,
      COALESCE(SUM(CASE WHEN a.value > 0 THEN 1 ELSE 0 END), 0) AS badge_count
    FROM users u
    LEFT JOIN achievements a ON a.player_id = u.user_id
    WHERE u.user_id = ?
    GROUP BY u.user_id
  `).get(playerId);

  if (!row) return null;

  const badges = db.prepare(`
    SELECT category, type, value, updated_at
    FROM achievements
    WHERE player_id = ?
    ORDER BY category, type
  `).all(playerId);

  row.badges = badges;
  row.roles = {
    vip: badges.some((b) => b.category === ROLE_BADGES.vip.category && b.type === ROLE_BADGES.vip.type && b.value > 0),
    admin: badges.some((b) => b.category === ROLE_BADGES.admin.category && b.type === ROLE_BADGES.admin.type && b.value > 0),
    moderator: badges.some((b) => b.category === ROLE_BADGES.moderator.category && b.type === ROLE_BADGES.moderator.type && b.value > 0),
  };
  return row;
}

app.get('/api/health', (_req, res) => {
  res.json({ ok: true, dbPath: DB_PATH });
});

app.get('/api/chat-commands', (_req, res) => {
  res.json({ commands: listChatCommandPermissions() });
});

app.post('/api/chat-commands/:commandName', (req, res, next) => {
  try {
    const commandName = normalizeCommandName(req.params.commandName);
    if (!CHAT_COMMANDS.includes(commandName)) {
      const err = new Error(`Unsupported command: ${commandName}`);
      err.status = 404;
      throw err;
    }
    const requiredRole = normalizeChatCommandPermission(req.body.requiredRole);
    upsertChatCommandPermissionStmt.run(commandName, requiredRole, Date.now());
    return res.json({ ok: true, commands: listChatCommandPermissions() });
  } catch (err) {
    return next(err);
  }
});

app.post('/api/popups/global', (req, res, next) => {
  try {
    const messageText = String(req.body.messageText || '').trim();
    if (messageText === '') {
      const err = new Error('messageText must not be empty');
      err.status = 400;
      throw err;
    }
    if (messageText.length > POPUP_MAX_TEXT_LENGTH) {
      const err = new Error(`messageText must be at most ${POPUP_MAX_TEXT_LENGTH} characters`);
      err.status = 400;
      throw err;
    }

    const info = db.prepare(`
      INSERT INTO admin_popup_queue (sender_name, message_text, created_at, delivered_at)
      VALUES ('update', ?, ?, 0)
    `).run(messageText, Date.now());

    return res.json({ ok: true, queuedId: Number(info.lastInsertRowid || 0) });
  } catch (err) {
    return next(err);
  }
});

app.get('/api/players', (req, res) => {
  const rawQuery = (req.query.query || '').toString().trim();
  const limit = Math.min(parseInt(req.query.limit, 10) || 50, 200);

  let rows;
  if (rawQuery === '') {
    rows = db.prepare(`
      SELECT
        u.user_id,
        u.username,
        u.village_id,
        u.village_name,
        u.level,
        u.slx_credits,
        COALESCE(SUM(CASE WHEN a.value > 0 THEN 1 ELSE 0 END), 0) AS badge_count
      FROM users u
      LEFT JOIN achievements a ON a.player_id = u.user_id
      GROUP BY u.user_id
      ORDER BY u.user_id
      LIMIT ?
    `).all(limit);
  } else {
    const like = `%${rawQuery}%`;
    const maybeId = Number(rawQuery);
    rows = db.prepare(`
      SELECT
        u.user_id,
        u.username,
        u.village_id,
        u.village_name,
        u.level,
        u.slx_credits,
        COALESCE(SUM(CASE WHEN a.value > 0 THEN 1 ELSE 0 END), 0) AS badge_count
      FROM users u
      LEFT JOIN achievements a ON a.player_id = u.user_id
      WHERE
        u.username LIKE ?
        OR u.village_name LIKE ?
        OR u.user_id = ?
        OR u.village_id = ?
      GROUP BY u.user_id
      ORDER BY u.user_id
      LIMIT ?
    `).all(like, like, Number.isInteger(maybeId) ? maybeId : -1, Number.isInteger(maybeId) ? maybeId : -1, limit);
  }

  res.json({ players: rows });
});

app.get('/api/players/:playerId', (req, res) => {
  const playerId = parseInt(req.params.playerId, 10);
  if (!Number.isInteger(playerId)) {
    return res.status(400).json({ error: 'playerId must be an integer' });
  }

  const player = getPlayerById(playerId);
  if (!player) return res.status(404).json({ error: 'Player not found' });

  res.json({ player });
});

app.post('/api/players/:playerId/coins', (req, res, next) => {
  try {
    const playerId = parseIntStrict(req.params.playerId, 'playerId');
    const slxCredits = parseIntStrict(req.body.slxCredits, 'slxCredits');

    const update = db.prepare('UPDATE users SET slx_credits = ? WHERE user_id = ?');
    const result = update.run(slxCredits, playerId);
    if (result.changes === 0) return res.status(404).json({ error: 'Player not found' });

    return res.json({ ok: true, player: getPlayerById(playerId) });
  } catch (err) {
    return next(err);
  }
});

app.post('/api/players/:playerId/level', (req, res, next) => {
  try {
    const playerId = parseIntStrict(req.params.playerId, 'playerId');
    const level = parseIntStrict(req.body.level, 'level');

    const update = db.prepare('UPDATE users SET level = ? WHERE user_id = ?');
    const result = update.run(level, playerId);
    if (result.changes === 0) return res.status(404).json({ error: 'Player not found' });

    return res.json({ ok: true, player: getPlayerById(playerId) });
  } catch (err) {
    return next(err);
  }
});

app.post('/api/players/:playerId/village-theme', (req, res, next) => {
  try {
    const playerId = parseIntStrict(req.params.playerId, 'playerId');
    const villageTheme = parseIntStrict(req.body.villageTheme, 'villageTheme');
    if (villageTheme < 0 || villageTheme > 255) {
      const err = new Error('villageTheme must be between 0 and 255');
      err.status = 400;
      throw err;
    }

    const player = db.prepare('SELECT village_id FROM users WHERE user_id = ?').get(playerId);
    if (!player) return res.status(404).json({ error: 'Player not found' });

    db.prepare(`
      INSERT INTO village_heightmaps (village_id, height_data, type, size)
      VALUES (?, x'', ?, 0)
      ON CONFLICT(village_id)
      DO UPDATE SET type = excluded.type
    `).run(player.village_id, villageTheme);

    return res.json({ ok: true, player: getPlayerById(playerId) });
  } catch (err) {
    return next(err);
  }
});

app.post('/api/players/:playerId/badges', (req, res, next) => {
  try {
    const playerId = parseIntStrict(req.params.playerId, 'playerId');
    const category = parseIntStrict(req.body.category, 'category');
    const type = parseIntStrict(req.body.type, 'type');
    const value = parseIntStrict(req.body.value, 'value');

    const playerExists = db.prepare('SELECT 1 FROM users WHERE user_id = ?').get(playerId);
    if (!playerExists) return res.status(404).json({ error: 'Player not found' });

    const now = Date.now();
    db.prepare(`
      INSERT INTO achievements (player_id, category, type, value, updated_at)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(player_id, category, type)
      DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
    `).run(playerId, category, type, value, now);

    return res.json({ ok: true, player: getPlayerById(playerId) });
  } catch (err) {
    return next(err);
  }
});

app.delete('/api/players/:playerId/badges', (req, res, next) => {
  try {
    const playerId = parseIntStrict(req.params.playerId, 'playerId');
    const category = parseIntStrict(req.body.category, 'category');
    const type = parseIntStrict(req.body.type, 'type');

    db.prepare(
      'DELETE FROM achievements WHERE player_id = ? AND category = ? AND type = ?'
    ).run(playerId, category, type);

    return res.json({ ok: true, player: getPlayerById(playerId) });
  } catch (err) {
    return next(err);
  }
});

app.post('/api/players/:playerId/roles', (req, res, next) => {
  try {
    const playerId = parseIntStrict(req.params.playerId, 'playerId');
    const playerExists = db.prepare('SELECT 1 FROM users WHERE user_id = ?').get(playerId);
    if (!playerExists) return res.status(404).json({ error: 'Player not found' });

    const now = Date.now();
    const upsert = db.prepare(`
      INSERT INTO achievements (player_id, category, type, value, updated_at)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(player_id, category, type)
      DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
    `);
    const clear = db.prepare(
      'DELETE FROM achievements WHERE player_id = ? AND category = ? AND type = ?'
    );

    for (const [roleName, badgeDef] of Object.entries(ROLE_BADGES)) {
      if (!(roleName in req.body)) continue;
      const enabled = Boolean(req.body[roleName]);
      if (enabled) {
        upsert.run(playerId, badgeDef.category, badgeDef.type, 1, now);
      } else {
        clear.run(playerId, badgeDef.category, badgeDef.type);
      }
    }

    return res.json({ ok: true, player: getPlayerById(playerId) });
  } catch (err) {
    return next(err);
  }
});

app.post('/api/players/:playerId/village-reset', (req, res, next) => {
  try {
    const playerId = parseIntStrict(req.params.playerId, 'playerId');
    const player = db.prepare('SELECT village_id FROM users WHERE user_id = ?').get(playerId);
    if (!player) return res.status(404).json({ error: 'Player not found' });

    const tx = db.transaction((villageId, userId) => {
      db.prepare('DELETE FROM buildings WHERE village_id = ?').run(villageId);
      db.prepare('DELETE FROM trees WHERE village_id = ?').run(villageId);
      db.prepare('DELETE FROM simulated_characters WHERE village_id = ?').run(villageId);
      db.prepare('DELETE FROM village_heightmaps WHERE village_id = ?').run(villageId);
      db.prepare('UPDATE users SET heightmap_preset_id = 0 WHERE user_id = ?').run(userId);
    });
    tx(player.village_id, playerId);

    return res.json({ ok: true, player: getPlayerById(playerId) });
  } catch (err) {
    return next(err);
  }
});

app.use((err, _req, res, _next) => {
  const status = err.status || 500;
  res.status(status).json({ error: err.message || 'Internal server error' });
});

app.listen(PORT, () => {
  console.log(`[admin-panel] listening on http://localhost:${PORT}`);
  console.log(`[admin-panel] DB: ${DB_PATH}`);
});
